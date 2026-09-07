#include "ai_chat.h"

#include <cjson/cJSON.h>
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "kvstore.h"

static keyword_index_entry_t keyword_index[MAX_KEYWORD_INDEX];
static size_t keyword_index_count = 0;

vector_index_entry_t global_vector_index[MAX_VECTOR_INDEX];
size_t global_vector_index_count = 0;

static float g_match_threshold = DEFAULT_MATCH_THRESHOLD;

typedef struct {
    char *data;
    size_t size;
} http_buffer_t;

/* CURL写回调：将响应数据累积到动态缓冲区 */
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

/* 初始化关键词索引和向量索引 */
int keep_index_init(void) {
    keyword_index_count = 0;
    global_vector_index_count = 0;
    return 0;
}

/* 添加关键词到关键词索引 */
int keep_add_keyword_index(const char *keyword, const char *key) {
    if (keyword == NULL || key == NULL) return -1;
    if (keyword_index_count >= MAX_KEYWORD_INDEX) return -1;
    strncpy(keyword_index[keyword_index_count].keyword, keyword, MAX_KEYWORD_LEN - 1);
    keyword_index[keyword_index_count].keyword[MAX_KEYWORD_LEN - 1] = '\0';
    strncpy(keyword_index[keyword_index_count].key, key, MAX_KEY_LEN - 1);
    keyword_index[keyword_index_count].key[MAX_KEY_LEN - 1] = '\0';
    keyword_index_count++;
    return 0;
}

/* 添加向量到全局向量索引 */
int keep_add_vector_index(const char *key, const float *vector, size_t dim) {
    if (key == NULL || vector == NULL || dim == 0) return -1;
    if (global_vector_index_count >= MAX_VECTOR_INDEX) return -1;
    vector_index_entry_t *entry = &global_vector_index[global_vector_index_count];
    strncpy(entry->key, key, MAX_KEY_LEN - 1);
    entry->key[MAX_KEY_LEN - 1] = '\0';
    entry->vector = kvs_malloc(sizeof(float) * dim);
    if (entry->vector == NULL) return -1;
    memcpy(entry->vector, vector, sizeof(float) * dim);
    entry->dim = dim;
    global_vector_index_count++;
    return 0;
}

/* 解析JSON并构建关键词与向量索引 */
int keep_build_index(const char *key, const char *value, size_t value_len) {
    if (key == NULL || value == NULL) return -1;
    char *json = kvs_malloc(value_len + 1);
    if (json == NULL) return -1;
    memcpy(json, value, value_len);
    json[value_len] = '\0';
    cJSON *root = cJSON_Parse(json);
    kvs_free(json);
    if (root == NULL) return -1;

    cJSON *keywords = cJSON_GetObjectItem(root, "user_msg_keywords");
    if (keywords != NULL && cJSON_IsArray(keywords)) {
        int count = cJSON_GetArraySize(keywords);
        for (int i = 0; i < count; i++) {
            cJSON *item = cJSON_GetArrayItem(keywords, i);
            if (!cJSON_IsString(item)) continue;
            if (keep_add_keyword_index(item->valuestring, key) != 0) {
                cJSON_Delete(root);
                return -1;
            }
        }
    }

    cJSON *user_msg = cJSON_GetObjectItem(root, "user_msg");
    if (user_msg == NULL || !cJSON_IsString(user_msg) || user_msg->valuestring == NULL) {
        cJSON_Delete(root);
        return -1;
    }
    float vector[MATCH_VECTOR_DIM];
    if (ai_text_to_vector(user_msg->valuestring, vector, MATCH_VECTOR_DIM) != 0) {
        cJSON_Delete(root);
        return -1;
    }
    if (keep_add_vector_index(key, vector, MATCH_VECTOR_DIM) != 0) {
        cJSON_Delete(root);
        return -1;
    }

    cJSON_Delete(root);
    return 0;
}

/* 调用本地embedding服务将文本转换为向量 */
int ai_text_to_vector(const char *text, float *vector, size_t dim) {
    if (text == NULL || vector == NULL || dim == 0) return -1;
    const char *base_url = "http://127.0.0.1:8000";
    char url[512];
    snprintf(url, sizeof(url), "%s/embed", base_url);

    cJSON *request = cJSON_CreateObject();
    if (request == NULL) return -1;
    cJSON_AddStringToObject(request, "text", text);
    char *request_body = cJSON_PrintUnformatted(request);
    cJSON_Delete(request);
    if (request_body == NULL) return -1;

    CURL *curl = curl_easy_init();
    if (curl == NULL) {
        free(request_body);
        return -1;
    }
    http_buffer_t response;
    response.data = NULL;
    response.size = 0;
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_body);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, embedding_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

    CURLcode curl_ret = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    free(request_body);

    if (curl_ret != CURLE_OK) {
        free(response.data);
        return -1;
    }
    if (http_code < 200 || http_code >= 300) {
        free(response.data);
        return -1;
    }
    if (response.data == NULL) return -1;

    cJSON *root = cJSON_Parse(response.data);
    free(response.data);
    if (root == NULL) return -1;

    cJSON *dimension = cJSON_GetObjectItem(root, "dimension");
    if (dimension == NULL || !cJSON_IsNumber(dimension)) {
        cJSON_Delete(root);
        return -1;
    }
    int embedding_dim = dimension->valueint;
    if (embedding_dim != (int)dim) {
        cJSON_Delete(root);
        return -1;
    }

    cJSON *embedding = cJSON_GetObjectItem(root, "embedding");
    if (embedding == NULL || !cJSON_IsArray(embedding)) {
        cJSON_Delete(root);
        return -1;
    }
    int array_size = cJSON_GetArraySize(embedding);
    if (array_size != (int)dim) {
        cJSON_Delete(root);
        return -1;
    }

    for (size_t i = 0; i < dim; i++) {
        cJSON *item = cJSON_GetArrayItem(embedding, (int)i);
        if (item == NULL || !cJSON_IsNumber(item)) {
            cJSON_Delete(root);
            return -1;
        }
        vector[i] = (float)item->valuedouble;
    }

    cJSON_Delete(root);
    return 0;
}

/* 计算两个向量的余弦相似度 */
float ai_cosine_similarity(const float *a, const float *b, size_t dim) {
    if (a == NULL || b == NULL || dim == 0) return -1.0f;
    double dot = 0.0, norm_a = 0.0, norm_b = 0.0;
    for (size_t i = 0; i < dim; i++) {
        dot += (double)a[i] * (double)b[i];
        norm_a += (double)a[i] * (double)a[i];
        norm_b += (double)b[i] * (double)b[i];
    }
    if (norm_a < 1e-12 || norm_b < 1e-12) return -1.0f;
    return (float)(dot / (sqrt(norm_a) * sqrt(norm_b)));
}

/* 设置匹配阈值 */
void match_set_threshold(float threshold) {
    if (threshold < 0.0f) threshold = 0.0f;
    if (threshold > 1.0f) threshold = 1.0f;
    g_match_threshold = threshold;
}

/* 获取当前匹配阈值 */
float match_get_threshold(void) {
    return g_match_threshold;
}

/* 判断问题是否包含关键词 */
static int question_contains_keyword(const char *question, const char *keyword) {
    if (question == NULL || keyword == NULL || keyword[0] == '\0') return 0;
    return strstr(question, keyword) != NULL;
}

/* 检查候选数组中是否已存在某key */
static int candidate_exists(char **candidates, size_t count, const char *key) {
    if (candidates == NULL || key == NULL) return 0;
    for (size_t i = 0; i < count; i++) {
        if (candidates[i] != NULL && strcmp(candidates[i], key) == 0) return 1;
    }
    return 0;
}

/* 从关键词索引中查找候选key */
static size_t match_find_keyword_candidates(const char *question, char **candidates, size_t max_candidates) {
    if (question == NULL || candidates == NULL || max_candidates == 0) return 0;
    size_t count = 0;
    for (size_t i = 0; i < keyword_index_count; i++) {
        const char *keyword = keyword_index[i].keyword;
        const char *key = keyword_index[i].key;
        if (!question_contains_keyword(question, keyword)) continue;
        if (candidate_exists(candidates, count, key)) continue;
        if (count >= max_candidates) break;
        strncpy(candidates[count], key, MAX_KEY_LEN - 1);
        candidates[count][MAX_KEY_LEN - 1] = '\0';
        count++;
    }
    return count;
}

/* 根据问题查找最佳匹配的key */
int match_find_best_key(const char *question, char *best_key, size_t best_key_size, float *best_score) {
    if (question == NULL || best_key == NULL || best_key_size == 0) return -1;
    if (global_vector_index_count == 0) return 1;

    float question_vector[MATCH_VECTOR_DIM];
    if (ai_text_to_vector(question, question_vector, MATCH_VECTOR_DIM) != 0) return -1;

    char **candidates = kvs_malloc(sizeof(char *) * MAX_KEYWORD_INDEX);
    if (candidates == NULL) return -1;
    size_t allocated = 0;
    for (size_t i = 0; i < MAX_KEYWORD_INDEX; i++) {
        candidates[i] = kvs_malloc(MAX_KEY_LEN);
        if (candidates[i] == NULL) {
            for (size_t j = 0; j < allocated; j++) kvs_free(candidates[j]);
            kvs_free(candidates);
            return -1;
        }
        candidates[i][0] = '\0';
        allocated++;
    }

    size_t candidate_count = match_find_keyword_candidates(question, candidates, MAX_KEYWORD_INDEX);
    int use_all_vectors = (candidate_count == 0);

    float max_score = -1.0f;
    const char *max_key = NULL;

    for (size_t i = 0; i < global_vector_index_count; i++) {
        vector_index_entry_t *entry = &global_vector_index[i];
        if (!use_all_vectors && !candidate_exists(candidates, candidate_count, entry->key)) continue;
        if (entry->dim != MATCH_VECTOR_DIM) continue;
        float score = ai_cosine_similarity(question_vector, entry->vector, MATCH_VECTOR_DIM);
        if (score < 0.0f) continue;
        if (max_key == NULL || score > max_score) {
            max_score = score;
            max_key = entry->key;
        }
    }

    for (size_t i = 0; i < allocated; i++) kvs_free(candidates[i]);
    kvs_free(candidates);

    if (max_key == NULL) return 1;
    if (max_score < g_match_threshold) return 1;

    strncpy(best_key, max_key, best_key_size - 1);
    best_key[best_key_size - 1] = '\0';
    if (best_score != NULL) *best_score = max_score;
    return 0;
}

/* 根据问题匹配并返回答案 */
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
    extern kvs_hash_t global_hash;
    kv_data_t key_data = { best_key, strlen(best_key) };
    kv_data_t *result = kvs_hash_get(&global_hash, &key_data);
    if (result == NULL || result->data == NULL || result->len == 0) return -1;

    char *json = kvs_malloc(result->len + 1);
    if (json == NULL) return -1;
    memcpy(json, result->data, result->len);
    json[result->len] = '\0';
    cJSON *root = cJSON_Parse(json);
    kvs_free(json);
    if (root == NULL) return -1;

    cJSON *ai_msg = cJSON_GetObjectItem(root, "ai_msg");
    if (ai_msg == NULL || !cJSON_IsString(ai_msg) || ai_msg->valuestring == NULL) {
        cJSON_Delete(root);
        return -1;
    }

    size_t len = strlen(ai_msg->valuestring);
    char *output = kvs_malloc(len + 1);
    if (output == NULL) {
        cJSON_Delete(root);
        return -1;
    }
    memcpy(output, ai_msg->valuestring, len);
    output[len] = '\0';
    *answer = output;
    *answer_len = len;
    if (best_score != NULL) *best_score = score;
    cJSON_Delete(root);
    return 0;
}

/* 释放向量索引内存并重置计数 */
void keep_index_destroy(void) {
    for (size_t i = 0; i < global_vector_index_count; i++) {
        kvs_free(global_vector_index[i].vector);
        global_vector_index[i].vector = NULL;
        global_vector_index[i].dim = 0;
    }
    global_vector_index_count = 0;
    keyword_index_count = 0;
}