#include "kvstore.h"
#include <string.h>
#include <stdint.h>   
#include <sys/time.h> 

#if ENABLE_RBTREE

#define RED 0
#define BLACK 1

kvs_rbtree_t global_rbtree = {0};

static inline int64_t rbtree_now_if_ttl(void) {
    if (!g_enable_ttl) return 0;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

/* ========== 节点创建与销毁 ========== */

static inline rbtree_node_binary_t* rbtree_node_create(kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    size_t total_size = sizeof(rbtree_node_binary_t) + key->len + value->len;
    rbtree_node_binary_t *node = (rbtree_node_binary_t *)kvs_malloc(total_size);
    if (!node) return NULL;
    
    node->color = RED;
    node->left = NULL;
    node->right = NULL;
    node->parent = NULL;
    node->expire_time = expire_time;
    
    node->key.data = node->data;
    node->key.len = key->len;
    memcpy(node->data, key->data, key->len);
    
    node->value.data = node->data + key->len;
    node->value.len = value->len;
    memcpy(node->value.data, value->data, value->len);
    
    return node;
}

static inline void rbtree_node_free(rbtree_node_binary_t *node) {
    if (node) kvs_free(node);
}

/* ========== 红黑树核心基础操作 ========== */

static inline rbtree_node_binary_t* rbtree_mini(rbtree_binary_t *T, rbtree_node_binary_t *x) {
    while (x->left != T->nil) {
        x = x->left;
    }
    return x;
}

static inline void rbtree_left_rotate(rbtree_binary_t *T, rbtree_node_binary_t *x) {
    rbtree_node_binary_t *y = x->right;
    x->right = y->left;
    if (y->left != T->nil) {
        y->left->parent = x;
    }
    y->parent = x->parent;
    if (x->parent == T->nil) {
        T->root = y;
    } else if (x == x->parent->left) {
        x->parent->left = y;
    } else {
        x->parent->right = y;
    }
    y->left = x;
    x->parent = y;
}

static inline void rbtree_right_rotate(rbtree_binary_t *T, rbtree_node_binary_t *y) {
    rbtree_node_binary_t *x = y->left;
    y->left = x->right;
    if (x->right != T->nil) {
        x->right->parent = y;
    }
    x->parent = y->parent;
    if (y->parent == T->nil) {
        T->root = x;
    } else if (y == y->parent->right) {
        y->parent->right = x;
    } else {
        y->parent->left = x;
    }
    x->right = y;
    y->parent = x;
}

/* ========== 插入修复 ========== */
static void rbtree_insert_fixup(rbtree_binary_t *T, rbtree_node_binary_t *z) {
    while (z->parent->color == RED) {
        if (z->parent == z->parent->parent->left) {
            rbtree_node_binary_t *y = z->parent->parent->right;
            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;
            } else {
                if (z == z->parent->right) {
                    z = z->parent;
                    rbtree_left_rotate(T, z);
                }
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                rbtree_right_rotate(T, z->parent->parent);
            }
        } else {
            rbtree_node_binary_t *y = z->parent->parent->left;
            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;
            } else {
                if (z == z->parent->left) {
                    z = z->parent;
                    rbtree_right_rotate(T, z);
                }
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                rbtree_left_rotate(T, z->parent->parent);
            }
        }
    }
    T->root->color = BLACK;
}

/* ========== 查找函数 ========== */
static inline rbtree_node_binary_t* rbtree_search(rbtree_binary_t *T, kv_data_t *key) {
    if (!T || !key) return NULL;
    rbtree_node_binary_t *node = T->root;
    while (node != NULL && node != T->nil) {
        if (!node->key.data) break;
        int cmp = kv_data_compare(key, &node->key);
        if (cmp < 0) {
            node = node->left;
        } else if (cmp > 0) {
            node = node->right;
        } else {
            return node;
        }
    }
    return T->nil;
}

static inline rbtree_node_binary_t* rbtree_find_parent(
    rbtree_binary_t *T,
    kv_data_t *key,
    rbtree_node_binary_t **parent_out,
    int *cmp_out)
{
    rbtree_node_binary_t *parent = T->nil;
    rbtree_node_binary_t *x = T->root;
    int cmp = 0;

    while (x != T->nil) {
        parent = x;
        cmp = kv_data_compare(key, &x->key);
        if (cmp < 0) {
            x = x->left;
        } else if (cmp > 0) {
            x = x->right;
        } else {
            *parent_out = x;
            *cmp_out = 0;
            return x;
        }
    }

    *parent_out = parent;
    *cmp_out = cmp;
    return T->nil;
}

/* ========== 标准节点移植（transplant） ========== */
static inline void rbtree_transplant(rbtree_binary_t *T, rbtree_node_binary_t *u, rbtree_node_binary_t *v) {
    if (u->parent == T->nil) {
        T->root = v;
    } else if (u == u->parent->left) {
        u->parent->left = v;
    } else {
        u->parent->right = v;
    }
    v->parent = u->parent;
}

/* ========== 标准删除修复 ========== */
static void rbtree_delete_fixup(rbtree_binary_t *T, rbtree_node_binary_t *x) {
    while (x != T->root && x->color == BLACK) {
        if (x == x->parent->left) {
            rbtree_node_binary_t *w = x->parent->right;
            if (w->color == RED) {
                w->color = BLACK;
                x->parent->color = RED;
                rbtree_left_rotate(T, x->parent);
                w = x->parent->right;
            }
            if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
                w->color = RED;
                x = x->parent;
            } else {
                if (w->right->color == BLACK) {
                    w->left->color = BLACK;
                    w->color = RED;
                    rbtree_right_rotate(T, w);
                    w = x->parent->right;
                }
                w->color = x->parent->color;
                x->parent->color = BLACK;
                w->right->color = BLACK;
                rbtree_left_rotate(T, x->parent);
                x = T->root;
            }
        } else {
            rbtree_node_binary_t *w = x->parent->left;
            if (w->color == RED) {
                w->color = BLACK;
                x->parent->color = RED;
                rbtree_right_rotate(T, x->parent);
                w = x->parent->left;
            }
            if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
                w->color = RED;
                x = x->parent;
            } else {
                if (w->left->color == BLACK) {
                    w->right->color = BLACK;
                    w->color = RED;
                    rbtree_left_rotate(T, w);
                    w = x->parent->left;
                }
                w->color = x->parent->color;
                x->parent->color = BLACK;
                w->left->color = BLACK;
                rbtree_right_rotate(T, x->parent);
                x = T->root;
            }
        }
    }
    x->color = BLACK;
}

/* ========== 删除函数 ========== */
static rbtree_node_binary_t* rbtree_delete(rbtree_binary_t *T, rbtree_node_binary_t *z) {
    rbtree_node_binary_t *y = z;
    int y_original_color = y->color;
    rbtree_node_binary_t *x;

    if (z->left == T->nil) {
        x = z->right;
        rbtree_transplant(T, z, z->right);
    } else if (z->right == T->nil) {
        x = z->left;
        rbtree_transplant(T, z, z->left);
    } else {
        y = rbtree_mini(T, z->right);
        y_original_color = y->color;
        x = y->right;
        if (y->parent == z) {
            x->parent = y; // 关键修复：无条件更新 x->parent，即便 x 是 T->nil
        } else {
            rbtree_transplant(T, y, y->right);
            y->right = z->right;
            y->right->parent = y;
        }
        rbtree_transplant(T, z, y);
        y->left = z->left;
        y->left->parent = y;
        y->color = z->color;
    }

    if (y_original_color == BLACK) {
        rbtree_delete_fixup(T, x);
    }

    return z;
}

/* ========== 公共接口 ========== */

int kvs_rbtree_create(kvs_rbtree_t *inst) {
    if (!inst) return -1;
    
    inst->nil = (rbtree_node_binary_t*)kvs_malloc(sizeof(rbtree_node_binary_t));
    if (!inst->nil) return -1;
    
    inst->nil->color = BLACK;
    inst->nil->left = inst->nil;    // 关键修复：哨兵 left 指向自身
    inst->nil->right = inst->nil;   // 关键修复：哨兵 right 指向自身
    inst->nil->parent = inst->nil;  // 哨兵 parent 指向自身
    inst->nil->key.data = NULL;
    inst->nil->key.len = 0;
    inst->nil->value.data = NULL;
    inst->nil->value.len = 0;
    inst->nil->expire_time = 0;
    inst->root = inst->nil;
    return 0;
}

static void rbtree_free_node(rbtree_binary_t *T, rbtree_node_binary_t *node) {
    if (node == T->nil) return;
    rbtree_free_node(T, node->left);
    rbtree_free_node(T, node->right);
    rbtree_node_free(node);
}

void kvs_rbtree_destroy(kvs_rbtree_t *inst) {
    if (!inst) return;
    rbtree_binary_t *T = (rbtree_binary_t*)inst;
    if (T->root != T->nil) {
        rbtree_free_node(T, T->root);
    }
    if (T->nil) {
        kvs_free(T->nil);
        T->nil = NULL;
    }
    T->root = NULL;
}

/* ========== SET ========== */
int kvs_rbtree_set(kvs_rbtree_t *inst, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!inst || !key || !value) return -1;
    
    rbtree_binary_t *T = (rbtree_binary_t*)inst;
    rbtree_node_binary_t *parent;
    int cmp;

    rbtree_node_binary_t *existing = rbtree_find_parent(T, key, &parent, &cmp);

    if (existing != T->nil) {
        if (value->len <= existing->value.len) {
            memcpy(existing->value.data, value->data, value->len);
            existing->value.len = value->len;
            existing->expire_time = expire_time;
            return 0;
        } else {
            rbtree_node_binary_t *to_delete = rbtree_delete(T, existing);
            if (to_delete && to_delete != T->nil) {
                rbtree_node_free(to_delete);
            }
            rbtree_node_binary_t *new_node = rbtree_node_create(key, value, expire_time);
            if (!new_node) return -1;
            existing = rbtree_find_parent(T, key, &parent, &cmp);
            if (existing != T->nil) {
                rbtree_node_free(new_node);
                return -1;
            }
            new_node->parent = parent;
            new_node->left = T->nil;
            new_node->right = T->nil;
            new_node->color = RED;
            if (parent == T->nil) {
                T->root = new_node;
            } else if (cmp < 0) {
                parent->left = new_node;
            } else {
                parent->right = new_node;
            }
            rbtree_insert_fixup(T, new_node);
            return 0;
        }
    }

    rbtree_node_binary_t *node = rbtree_node_create(key, value, expire_time);
    if (!node) return -1;

    node->parent = parent;
    node->left = T->nil;
    node->right = T->nil;
    node->color = RED;

    if (parent == T->nil) {
        T->root = node;
    } else if (cmp < 0) {
        parent->left = node;
    } else {
        parent->right = node;
    }

    rbtree_insert_fixup(T, node);
    return 0;
}

/* ========== GET ========== */
kv_data_t* kvs_rbtree_get(kvs_rbtree_t *inst, kv_data_t *key) {
    if (!inst || !key) return NULL;
    rbtree_binary_t *T = (rbtree_binary_t*)inst;
    rbtree_node_binary_t *node = rbtree_search(T, key);
    if (node == NULL || node == T->nil) return NULL;
    
    if (g_enable_ttl && node->expire_time > 0) {
        int64_t now = rbtree_now_if_ttl();
        if (now > node->expire_time) {
            kvs_rbtree_del(inst, key);
            return NULL;
        }
    }
    return &node->value;
}

/* ========== DEL ========== */
int kvs_rbtree_del(kvs_rbtree_t *inst, kv_data_t *key) {
    if (!inst || !key) return -1;
    rbtree_binary_t *T = (rbtree_binary_t*)inst;
    rbtree_node_binary_t *node = rbtree_search(T, key);
    if (node == inst->nil) return 1;
    
    rbtree_node_binary_t *deleted = rbtree_delete(T, node);
    if (deleted && deleted != T->nil) {
        rbtree_node_free(deleted);
    }
    return 0;
}

int kvs_rbtree_del_if_expired(kvs_rbtree_t *inst, kv_data_t *key, int64_t expected_expire) {
    if (!inst || !key) return 0;
    rbtree_binary_t *T = (rbtree_binary_t*)inst;
    rbtree_node_binary_t *node = rbtree_search(T, key);
    if (node == NULL || node == T->nil) return 0;
    if (node->expire_time != expected_expire) return 0;
    kvs_rbtree_del(inst, key);
    return 1;
}

/* ========== MOD ========== */
int kvs_rbtree_mod(kvs_rbtree_t *inst, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    return kvs_rbtree_set(inst, key, value, expire_time);
}

/* ========== EXISTS ========== */
int kvs_rbtree_exist(kvs_rbtree_t *inst, kv_data_t *key) {
    if (!inst || !key) return -1;
    kv_data_t *res = kvs_rbtree_get(inst, key);
    return (res == NULL) ? 1 : 0;
}

/* ========== FOREACH ========== */
static void rbtree_foreach_node(rbtree_binary_t *T, rbtree_node_binary_t *node, 
                                int64_t now, int check_expire,
                                void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg) {
    if (node == T->nil) return;
    rbtree_foreach_node(T, node->left, now, check_expire, callback, arg);
    if (check_expire && node->expire_time > 0 && now > node->expire_time) {
        /* skip expired */
    } else {
        callback(&node->key, &node->value, arg);
    }
    rbtree_foreach_node(T, node->right, now, check_expire, callback, arg);
}

void kvs_rbtree_foreach(kvs_rbtree_t *inst, void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg) {
    if (!inst || !callback) return;
    rbtree_binary_t *T = (rbtree_binary_t*)inst;
    int64_t now = 0;
    int check_expire = g_enable_ttl;
    if (check_expire) now = rbtree_now_if_ttl();
    rbtree_foreach_node(T, T->root, now, check_expire, callback, arg);
}

/* ========== UTILITY ========== */
int kvs_rbtree_get_value_len(char *key_ptr, int key_len) {
    if (!key_ptr || key_len <= 0) return 0;
    if (global_rbtree.nil == NULL || global_rbtree.root == NULL) return 0;
    kv_data_t tmp_key = { .data = key_ptr, .len = (size_t)key_len };
    kv_data_t *res_val = kvs_rbtree_get(&global_rbtree, &tmp_key);
    if (res_val == NULL || res_val->data == NULL) return 0;
    if (res_val->len > (size_t)0x7fffffff) return 0;
    return (int)res_val->len;
}

kv_data_t* kvs_rbtree_get_global(kv_data_t *key) {
    return kvs_rbtree_get(&global_rbtree, key);
}

#endif