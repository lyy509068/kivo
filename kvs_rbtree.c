

#include "kvstore.h"
#include <string.h>
#include <stdint.h>   
#include <sys/time.h> 

#if ENABLE_RBTREE
#define RED 0
#define BLACK 1

// 全局红黑树实例
kvs_rbtree_t global_rbtree = {0};

static inline int64_t rbtree_now_if_ttl(void) {
    if (!g_enable_ttl) return 0;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static rbtree_node_binary_t* rbtree_mini(rbtree_binary_t *T, rbtree_node_binary_t *x) {
    while (x->left != T->nil) {
        x = x->left;
    }
    return x;
}

static rbtree_node_binary_t* rbtree_successor(rbtree_binary_t *T, rbtree_node_binary_t *x) {
    rbtree_node_binary_t *y = x->parent;
    
    if (x->right != T->nil) {
        return rbtree_mini(T, x->right);
    }
    
    while ((y != T->nil) && (x == y->right)) {
        x = y;
        y = y->parent;
    }
    return y;
}

static void rbtree_left_rotate(rbtree_binary_t *T, rbtree_node_binary_t *x) {
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

static void rbtree_right_rotate(rbtree_binary_t *T, rbtree_node_binary_t *y) {
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

static void rbtree_insert(rbtree_binary_t *T, rbtree_node_binary_t *z) {
    rbtree_node_binary_t *y = T->nil;
    rbtree_node_binary_t *x = T->root;
    
    while (x != T->nil) {
        y = x;
        int cmp = kv_data_compare(&z->key, &x->key);
        if (cmp < 0) {
            x = x->left;
        } else if (cmp > 0) {
            x = x->right;
        } else {
            return;  // key 已存在
        }
    }
    
    z->parent = y;
    if (y == T->nil) {
        T->root = z;
    } else if (kv_data_compare(&z->key, &y->key) < 0) {
        y->left = z;
    } else {
        y->right = z;
    }
    
    z->left = T->nil;
    z->right = T->nil;
    z->color = RED;
    
    rbtree_insert_fixup(T, z);
}

static void rbtree_delete_fixup(rbtree_binary_t *T, rbtree_node_binary_t *x) {
    while ((x != T->root) && (x->color == BLACK)) {
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

static rbtree_node_binary_t* rbtree_delete(rbtree_binary_t *T, rbtree_node_binary_t *z) {
    rbtree_node_binary_t *y = T->nil;
    rbtree_node_binary_t *x = T->nil;
    
    if ((z->left == T->nil) || (z->right == T->nil)) {
        y = z;
    } else {
        y = rbtree_successor(T, z);
    }
    
    if (y->left != T->nil) {
        x = y->left;
    } else if (y->right != T->nil) {
        x = y->right;
    }
    
    x->parent = y->parent; 
    
    if (y->parent == T->nil) {
        T->root = x;
    } else if (y == y->parent->left) {
        y->parent->left = x;
    } else {
        y->parent->right = x;
    }
    
    if (y != z) {
        // ✅ 修复：先保存 z 的旧数据
        kv_data_t old_key = z->key;
        kv_data_t old_value = z->value;
        
        // 把 y 的数据转移到 z
        z->key = y->key;
        z->value = y->value;
        z->expire_time = y->expire_time;
        
        // 把旧数据放到 y，让外层统一释放
        y->key = old_key;
        y->value = old_value;
    }
    
    if (y->color == BLACK) {
        rbtree_delete_fixup(T, x);
    }
    
    return y;  // 外层会统一释放 y 的 key/value 和 y 本身
}


static rbtree_node_binary_t* rbtree_search(rbtree_binary_t *T, kv_data_t *key) {
    if (!T || !key || !T->root) return NULL; //安全拦截
    
    rbtree_node_binary_t *node = T->root;
    
    // 双重防御：既不能等于 nil，也不能等于底层真正的 NULL
    while (node != NULL && node != T->nil) {
        
        if (!node->key.data) {// 确保node内部的key内存是合法的
            break;
        }

        int cmp = kv_data_compare(key, &node->key);
        if (cmp < 0) {
            node = node->left;
        } else if (cmp > 0) {
            node = node->right;
        } else {
            return node; // 找到了
        }
    }
    return T->nil;
}

// 原样输出
int kvs_rbtree_create(kvs_rbtree_t *inst) {
    if (!inst) return -1;
    
    inst->nil = (rbtree_node_binary_t*)kvs_malloc_type(OBJ_RBTREE, sizeof(rbtree_node_binary_t));
    if (!inst->nil) return -1;
    
    inst->nil->color = BLACK;
    inst->nil->left = NULL;
    inst->nil->right = NULL;
    inst->nil->parent = NULL;
    inst->nil->key.data = NULL;
    inst->nil->key.len = 0;
    inst->nil->value.data = NULL;
    inst->nil->value.len = 0;
    inst->nil->expire_time = 0; 
    
    inst->root = inst->nil;
    
    return 0;
}

// 原样输出
static void rbtree_free_node(rbtree_binary_t *T, rbtree_node_binary_t *node) {
    if (node == T->nil) return;
    rbtree_free_node(T, node->left);
    rbtree_free_node(T, node->right);
    
    kv_data_destroy(&node->key);
    kv_data_destroy(&node->value);
    kvs_free_type(OBJ_RBTREE, node);
}

// 原样输出
void kvs_rbtree_destroy(kvs_rbtree_t *inst) {
    if (!inst) return;
    rbtree_binary_t *T = (rbtree_binary_t*)inst;
    if (T->root != T->nil) {
        rbtree_free_node(T, T->root); // 递归释放整棵树的所有节点
    }
    if (T->nil) {
        kvs_free_type(OBJ_RBTREE, T->nil);
        T->nil = NULL;
    }
    T->root = NULL;
}

int kvs_rbtree_set(kvs_rbtree_t *inst, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!inst || !key || !value) return -1;
    
    rbtree_binary_t *T = (rbtree_binary_t*)inst;
    rbtree_node_binary_t *existing = rbtree_search(T, key);
    
    if (existing != inst->nil) {
        // 直接覆盖 value，不检查是否过期
        kv_data_destroy(&existing->value);
        if (kv_data_dup(&existing->value, value) != 0) return -2;
        existing->expire_time = expire_time;
        return 0;
    }
    
    rbtree_node_binary_t *node = (rbtree_node_binary_t*)kvs_malloc_type(OBJ_RBTREE, sizeof(rbtree_node_binary_t));
    if (!node) return -2;
    if (kv_data_dup(&node->key, key) != 0) {
        kvs_free_type(OBJ_RBTREE, node);
        return -2;
    }
    if (kv_data_dup(&node->value, value) != 0) {
        kv_data_destroy(&node->key);
        kvs_free_type(OBJ_RBTREE, node);
        return -2;
    }
    node->color = RED;
    node->left = inst->nil;
    node->right = inst->nil;
    node->parent = inst->nil;
    node->expire_time = expire_time; 
    rbtree_insert((rbtree_binary_t*)inst, node);
    return 0;
}

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

// 原样输出
int kvs_rbtree_del(kvs_rbtree_t *inst, kv_data_t *key) {
    if (!inst || !key) return -1;
    
    rbtree_node_binary_t *node = rbtree_search((rbtree_binary_t*)inst, key);
    if (node == inst->nil) return 1;  // 不存在
    
    rbtree_node_binary_t *cur = rbtree_delete((rbtree_binary_t*)inst, node);
    if (cur) {
        // 如果 cur 内部还有残留数据，则正常销毁
        kv_data_destroy(&cur->key);
        kv_data_destroy(&cur->value);
        kvs_free_type(OBJ_RBTREE, cur);
    }
    
    return 0;
}

/**
 * 条件删除：只有当 key 存在且节点的 expire_time 等于 expected_expire 时才删除
 * 返回 1 表示已删除，0 表示未删除（key 不存在或时间不匹配）
 */
int kvs_rbtree_del_if_expired(kvs_rbtree_t *inst, kv_data_t *key, int64_t expected_expire) {
    if (!inst || !key) return 0;
    
    rbtree_binary_t *T = (rbtree_binary_t*)inst;
    rbtree_node_binary_t *node = rbtree_search(T, key);
    
    // 找不到或返回哨兵
    if (node == NULL || node == T->nil) return 0;
    
    // 检查过期时间是否完全匹配
    if (node->expire_time != expected_expire) return 0;
    
    // 匹配，执行物理删除
    kvs_rbtree_del(inst, key);
    return 1;
}

int kvs_rbtree_mod(kvs_rbtree_t *inst, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    // MOD 合并为 SET，无需单独实现
    return kvs_rbtree_set(inst, key, value, expire_time);
}


int kvs_rbtree_exist(kvs_rbtree_t *inst, kv_data_t *key) {
    if (!inst || !key) return -1;
    
    // 内部通过调修改后的 get 可以间接实现惰性删除拦截
    kv_data_t *res = kvs_rbtree_get(inst, key);
    return (res == NULL) ? 1 : 0;
}


static void rbtree_foreach_node(rbtree_binary_t *T, rbtree_node_binary_t *node, 
                                int64_t now, int check_expire,
                                void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg) {
    if (node == T->nil) return;
    rbtree_foreach_node(T, node->left, now, check_expire, callback, arg);
    if (check_expire && node->expire_time > 0 && now > node->expire_time) {
        // 跳过过期节点
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
    if (check_expire) {
        now = rbtree_now_if_ttl();
    }
    rbtree_foreach_node(T, T->root, now, check_expire, callback, arg);
}

int kvs_rbtree_get_value_len(char *key_ptr, int key_len) {
    if (!key_ptr || key_len <= 0) {
        return 0;
    }

    // 1. 检查全局红黑树是否已初始化
    if (global_rbtree.nil == NULL || global_rbtree.root == NULL) {
        return 0;
    }

    // 2. 在栈上构造临时 key（不分配内存，仅引用网络缓冲区）
    kv_data_t tmp_key;
    tmp_key.data = key_ptr;
    tmp_key.len  = (size_t)key_len;

    // 3. 直接调用公共接口，而不是手动操作底层 rbtree_search
    kv_data_t *res_val = kvs_rbtree_get(&global_rbtree, &tmp_key);

    // 4. 如果未找到或 value 无效，则返回 0
    if (res_val == NULL || res_val->data == NULL) {
        return 0;
    }

    // 5. 防止 size_t -> int 转换溢出
    if (res_val->len > (size_t)0x7fffffff) {
        return 0;
    }

    // 6. 返回实际 value 长度
    return (int)res_val->len;
}

// 原样输出
kv_data_t* kvs_rbtree_get_global(kv_data_t *key) {
    return kvs_rbtree_get(&global_rbtree, key);
}
#endif