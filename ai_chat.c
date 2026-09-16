#include "ai_chat.h"

#include <cjson/cJSON.h>
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>

#include "kvstore.h"

/* 
 * CPU 特性：AVX2 加速（x86 上启用，其他平台关闭）
 *  */
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#define HAS_AVX2 1
#else
#define HAS_AVX2 0
#endif

vector_index_entry_t global_vector_index[MAX_VECTOR_INDEX];
size_t global_vector_index_count = 0;

static float g_match_threshold = DEFAULT_MATCH_THRESHOLD;

static CURL *g_curl = NULL;
static struct curl_slist *g_curl_headers = NULL;

/* 全局开关：由 server.c 从 config.conf 的 embedding 项设置
   1 = 真调用（走 embedding 服务）
   0 = 假调用（跳过 embedding 服务，用文本 hash 生成伪向量） */
extern int g_use_embedding;

/* 保护 global_vector_index 的写入（同步模式下暂不需要，保留兼容未来） */
static pthread_mutex_t g_index_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    char *data;
    size_t size;
} http_buffer_t;

/* CURL写回调 */
static size_t embedding_write_callback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t total = size * nmemb;
    http_buffer_t *buffer = (http_buffer_t *)userp;
    char *new_data = realloc(buffer->data, buffer->size + total + 1);
    if (new_data == NULL) return 0;
    buffer->data = new_data;
    memcpy(buffer->data + buffer->size, contents, total);
    buffer->size += total;
    buffer->data[buffer->size] = '\0';
    return total;
}

/* 
 * 初始化：根据 g_use_embedding（由 config.conf 决定）打印模式
 *  */
int keep_index_init(void) {
    global_vector_index_count = 0;

    if (g_use_embedding) {
        printf("[EMBEDDING] REAL mode (calling embedding server)\n");
    } else {
        printf("[EMBEDDING] FAKE mode enabled (skip embedding server)\n");
    }

    return 0;
}

void keep_index_destroy(void) {
    for (size_t i = 0; i < global_vector_index_count; i++) {
        kvs_free(global_vector_index[i].vector);
        global_vector_index[i].vector = NULL;
        global_vector_index[i].dim = 0;
    }
    global_vector_index_count = 0;
}

/* 
 * 添加向量到索引（加锁版）
 *  */
int keep_add_vector_index(const char *key, const float *vector, size_t dim) {
    if (key == NULL || vector == NULL || dim == 0) return -1;

    pthread_mutex_lock(&g_index_mutex);

    if (global_vector_index_count >= MAX_VECTOR_INDEX) {
        pthread_mutex_unlock(&g_index_mutex);
        return -1;
    }

    vector_index_entry_t *entry = &global_vector_index[global_vector_index_count];
    strncpy(entry->key, key, MAX_KEY_LEN - 1);
    entry->key[MAX_KEY_LEN - 1] = '\0';
    entry->vector = kvs_malloc(sizeof(float) * dim);
    if (entry->vector == NULL) {
        pthread_mutex_unlock(&g_index_mutex);
        return -1;
    }
    memcpy(entry->vector, vector, sizeof(float) * dim);
    entry->dim = dim;
    global_vector_index_count++;

    pthread_mutex_unlock(&g_index_mutex);
    return 0;
}

/* 
 * 构建索引（同步：调完 embedding 再返回）
 *  */
int keep_build_index(const char *key, const char *value, size_t value_len) {
    (void)value;
    (void)value_len;
    if (key == NULL) return -1;

    float vector[MATCH_VECTOR_DIM];
    if (ai_text_to_vector(key, vector, MATCH_VECTOR_DIM) != 0) {
        return -1;
    }
    return keep_add_vector_index(key, vector, MATCH_VECTOR_DIM);
}

/* 
 * 调用 embedding 服务
 *
 * 运行时判断 g_use_embedding（来自 config.conf）：
 *   =1：走真实 embedding 服务
 *   =0：用文本 hash 生成伪向量（不调服务），用于压测 KVStore 本身
 *  */
int ai_text_to_vector(const char *text, float *vector, size_t dim) {
    if (text == NULL || vector == NULL || dim == 0) return -1;

    /* ---------- 假调用：embedding OFF，FNV-1a hash + LCG 生成伪向量 ---------- */
    if (!g_use_embedding) {
        unsigned int h = 2166136261u;
        for (const char *p = text; *p; p++) {
            h ^= (unsigned char)*p;
            h *= 16777619u;
        }

        for (size_t i = 0; i < dim; i++) {
            h = h * 1103515245u + 12345u;
            vector[i] = ((h >> 8) & 0xFFFF) / 65536.0f - 0.5f;
        }

        /* L2 归一化 */
        double norm = 0.0;
        for (size_t i = 0; i < dim; i++) norm += (double)vector[i] * vector[i];
        norm = sqrt(norm);
        if (norm < 1e-12) return -1;
        for (size_t i = 0; i < dim; i++) vector[i] /= (float)norm;

        return 0;
    }

    /* ---------- 真调用：embedding ON，走 embedding 服务（CURL 连接复用） ---------- */
    if (g_curl == NULL) {
        g_curl = curl_easy_init();
        if (g_curl == NULL) return -1;

        g_curl_headers = curl_slist_append(g_curl_headers, "Content-Type: application/json");

        curl_easy_setopt(g_curl, CURLOPT_URL, "http://127.0.0.1:8000/embed");
        curl_easy_setopt(g_curl, CURLOPT_HTTPHEADER, g_curl_headers);
        curl_easy_setopt(g_curl, CURLOPT_POST, 1L);
        curl_easy_setopt(g_curl, CURLOPT_WRITEFUNCTION, embedding_write_callback);
        curl_easy_setopt(g_curl, CURLOPT_CONNECTTIMEOUT, 5L);
        curl_easy_setopt(g_curl, CURLOPT_TIMEOUT, 30L);
        curl_easy_setopt(g_curl, CURLOPT_TCP_KEEPALIVE, 1L);
    }

    cJSON *request = cJSON_CreateObject();
    if (request == NULL) return -1;
    cJSON_AddStringToObject(request, "text", text);
    char *request_body = cJSON_PrintUnformatted(request);
    cJSON_Delete(request);
    if (request_body == NULL) return -1;

    http_buffer_t response;
    response.data = NULL;
    response.size = 0;

    curl_easy_setopt(g_curl, CURLOPT_POSTFIELDS, request_body);
    curl_easy_setopt(g_curl, CURLOPT_WRITEDATA, &response);

    CURLcode curl_ret = curl_easy_perform(g_curl);
    long http_code = 0;
    curl_easy_getinfo(g_curl, CURLINFO_RESPONSE_CODE, &http_code);

    free(request_body);

    if (curl_ret != CURLE_OK) { free(response.data); return -1; }
    if (http_code < 200 || http_code >= 300) { free(response.data); return -1; }
    if (response.data == NULL) return -1;

    cJSON *root = cJSON_Parse(response.data);
    free(response.data);
    if (root == NULL) return -1;

    cJSON *dimension = cJSON_GetObjectItem(root, "dimension");
    if (dimension == NULL || !cJSON_IsNumber(dimension)) { cJSON_Delete(root); return -1; }
    int embedding_dim = dimension->valueint;
    if (embedding_dim != (int)dim) { cJSON_Delete(root); return -1; }

    cJSON *embedding = cJSON_GetObjectItem(root, "embedding");
    if (embedding == NULL || !cJSON_IsArray(embedding)) { cJSON_Delete(root); return -1; }
    int array_size = cJSON_GetArraySize(embedding);
    if (array_size != (int)dim) { cJSON_Delete(root); return -1; }

    for (size_t i = 0; i < dim; i++) {
        cJSON *item = cJSON_GetArrayItem(embedding, (int)i);
        if (item == NULL || !cJSON_IsNumber(item)) { cJSON_Delete(root); return -1; }
        vector[i] = (float)item->valuedouble;
    }

    cJSON_Delete(root);
    return 0;
}

/* 
 * 余弦相似度：AVX2 加速版 + 标量 fallback
 *  */
#if HAS_AVX2
static float cosine_avx2(const float *a, const float *b, size_t dim) {
    __m256 vdot = _mm256_setzero_ps();
    __m256 va2  = _mm256_setzero_ps();
    __m256 vb2  = _mm256_setzero_ps();

    size_t i = 0;
    for (; i + 8 <= dim; i += 8) {
        __m256 va = _mm256_loadu_ps(a + i);
        __m256 vb = _mm256_loadu_ps(b + i);
        vdot = _mm256_fmadd_ps(va, vb, vdot);
        va2  = _mm256_fmadd_ps(va, va, va2);
        vb2  = _mm256_fmadd_ps(vb, vb, vb2);
    }

    float dot_arr[8], a2_arr[8], b2_arr[8];
    _mm256_storeu_ps(dot_arr, vdot);
    _mm256_storeu_ps(a2_arr, va2);
    _mm256_storeu_ps(b2_arr, vb2);

    double dot = 0, na = 0, nb = 0;
    for (int j = 0; j < 8; j++) {
        dot += dot_arr[j];
        na  += a2_arr[j];
        nb  += b2_arr[j];
    }

    for (; i < dim; i++) {
        dot += (double)a[i] * (double)b[i];
        na  += (double)a[i] * (double)a[i];
        nb  += (double)b[i] * (double)b[i];
    }

    if (na < 1e-12 || nb < 1e-12) return -1.0f;
    return (float)(dot / (sqrt(na) * sqrt(nb)));
}
#endif

static int g_has_avx2 = -1;

static int cpu_supports_avx2(void) {
#if HAS_AVX2
    return __builtin_cpu_supports("avx2") && __builtin_cpu_supports("fma");
#else
    return 0;
#endif
}

float ai_cosine_similarity(const float *a, const float *b, size_t dim) {
    if (a == NULL || b == NULL || dim == 0) return -1.0f;

    if (g_has_avx2 == -1) {
        g_has_avx2 = cpu_supports_avx2();
        printf("[SIMD] AVX2 = %s\n", g_has_avx2 ? "enabled" : "disabled");
    }

#if HAS_AVX2
    if (g_has_avx2) {
        return cosine_avx2(a, b, dim);
    }
#endif

    double dot = 0.0, norm_a = 0.0, norm_b = 0.0;
    for (size_t i = 0; i < dim; i++) {
        dot += (double)a[i] * (double)b[i];
        norm_a += (double)a[i] * (double)a[i];
        norm_b += (double)b[i] * (double)b[i];
    }
    if (norm_a < 1e-12 || norm_b < 1e-12) return -1.0f;
    return (float)(dot / (sqrt(norm_a) * sqrt(norm_b)));
}

/* 
 * 阈值管理
 *  */
void match_set_threshold(float threshold) {
    if (threshold < 0.0f) threshold = 0.0f;
    if (threshold > 1.0f) threshold = 1.0f;
    g_match_threshold = threshold;
}

float match_get_threshold(void) {
    return g_match_threshold;
}

/* 
 * 查找最佳匹配（加提前终止）
 *  */
int match_find_best_key(const char *question, char *best_key, size_t best_key_size, float *best_score) {
    if (question == NULL || best_key == NULL || best_key_size == 0) return -1;

    pthread_mutex_lock(&g_index_mutex);
    size_t count = global_vector_index_count;
    pthread_mutex_unlock(&g_index_mutex);

    if (count == 0) return 1;

    float question_vector[MATCH_VECTOR_DIM];
    if (ai_text_to_vector(question, question_vector, MATCH_VECTOR_DIM) != 0) return -1;

    float max_score = -1.0f;
    const char *max_key = NULL;

    pthread_mutex_lock(&g_index_mutex);
    count = global_vector_index_count;

    for (size_t i = 0; i < count; i++) {
        vector_index_entry_t *entry = &global_vector_index[i];
        if (entry->dim != MATCH_VECTOR_DIM) continue;
        float score = ai_cosine_similarity(question_vector, entry->vector, MATCH_VECTOR_DIM);
        if (score < 0.0f) continue;
        if (max_key == NULL || score > max_score) {
            max_score = score;
            max_key = entry->key;
        }
        /* 提前终止：已经几乎完全匹配 */
        if (max_score >= 0.9999f) break;
    }

    if (max_key != NULL) {
        strncpy(best_key, max_key, best_key_size - 1);
        best_key[best_key_size - 1] = '\0';
    }
    pthread_mutex_unlock(&g_index_mutex);

    if (max_key == NULL) return 1;
    if (max_score < g_match_threshold) return 1;

    if (best_score != NULL) *best_score = max_score;
    return 0;
}

/* 
 * 匹配答案
 *  */
int match_find_answer(const char *question, char **answer, size_t *answer_len, float *best_score) {
    if (question == NULL || answer == NULL || answer_len == NULL) return -1;
    *answer = NULL;
    *answer_len = 0;

    char best_key[MAX_KEY_LEN];
    float score = 0.0f;
    int ret = match_find_best_key(question, best_key, sizeof(best_key), &score);
    if (ret < 0) return -1;
    if (ret == 1) return 1;

    extern kv_data_t *kvs_hash_get(kvs_hash_t *hash, kv_data_t *key);
    extern kvs_hash_t global_hash1;
    kv_data_t key_data = { best_key, strlen(best_key) };
    kv_data_t *result = kvs_hash_get(&global_hash1, &key_data);
    if (result == NULL || result->data == NULL || result->len == 0) return -1;

    char *output = kvs_malloc(result->len + 1);
    if (output == NULL) return -1;
    memcpy(output, result->data, result->len);
    output[result->len] = '\0';
    *answer = output;
    *answer_len = result->len;
    if (best_score != NULL) *best_score = score;
    return 0;
}