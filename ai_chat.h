#ifndef AI_CHAT_H
#define AI_CHAT_H

#include <stddef.h>

#define MAX_KEY_LEN         128
#define MAX_VECTOR_INDEX    10000
#define MATCH_VECTOR_DIM    512
#define DEFAULT_MATCH_THRESHOLD 0.90f

typedef struct {
    char key[MAX_KEY_LEN];
    float *vector;
    size_t dim;
} vector_index_entry_t;

extern vector_index_entry_t global_vector_index[MAX_VECTOR_INDEX];
extern size_t global_vector_index_count;

int keep_index_init(void);
void keep_index_destroy(void);

int keep_add_vector_index(const char *key, const float *vector, size_t dim);
int keep_build_index(const char *key, const char *value, size_t value_len);

int ai_text_to_vector(const char *text, float *vector, size_t dim);
float ai_cosine_similarity(const float *a, const float *b, size_t dim);

void match_set_threshold(float threshold);
float match_get_threshold(void);

int match_find_best_key(const char *question, char *best_key, size_t best_key_size, float *best_score);
int match_find_answer(const char *question, char **answer, size_t *answer_len, float *best_score);

#endif