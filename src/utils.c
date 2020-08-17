#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "utils.h"
#include "cs165_api.h"

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_RESET   "\x1b[0m"

#define LOG 1
#define LOG_ERR 1
#define LOG_INFO 1

/* removes newline characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of newline characters.
 */ 
char* trim_newline(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!(str[i] == '\r' || str[i] == '\n')) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}
/* removes space characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of space characters.
 */ 
char* trim_whitespace(char *str)
{
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!isspace(str[i])) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

/* removes parenthesis characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of parenthesis characters.
 */ 
char* trim_parenthesis(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!(str[i] == '(' || str[i] == ')')) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

char* trim_quotes(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (str[i] != '\"') {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

/* The following three functions will show output on the terminal
 * based off whether the corresponding level is defined.
 * To see log output, define LOG.
 * To see error output, define LOG_ERR.
 * To see info output, define LOG_INFO
 */
void cs165_log(FILE* out, const char *format, ...) {
#ifdef LOG
    va_list v;
    va_start(v, format);
    vfprintf(out, format, v);
    va_end(v);
#else
    (void) out;
    (void) format;
#endif
}

void log_err(const char *format, ...) {
#ifdef LOG_ERR
    va_list v;
    va_start(v, format);
    fprintf(stderr, ANSI_COLOR_RED);
    vfprintf(stderr, format, v);
    fprintf(stderr, ANSI_COLOR_RESET);
    va_end(v);
#else
    (void) format;
#endif
}

void log_info(const char *format, ...) {
#ifdef LOG_INFO
    va_list v;
    va_start(v, format);
    fprintf(stdout, ANSI_COLOR_GREEN);
    vfprintf(stdout, format, v);
    fprintf(stdout, ANSI_COLOR_RESET);
    fflush(stdout);
    va_end(v);
#else
    (void) format;
#endif
}

//binary search key
int search_key(int key, int* key_vec, int n){
    int low = 0;
    int high = n-1;
    int mid;
    while(low <= high){
        mid = (low+high)/2;
        if(key_vec[mid] == key){
            while(mid>0 && key_vec[mid-1] == key){
                mid--;
            }
            return mid;
        }else if(key_vec[mid] < key){
            low = mid+1;
        }else{
            high = mid-1;
        }
    }
    if(key_vec[mid] < key){
        mid = mid+1;
    }
    return (size_t) mid;
}

BTreeNode* btree_create(int key, int pos){
    BTreeNode* root = malloc(1 * sizeof(BTreeNode));
    root->core_node.lnode.key_vec[0] = key;
    root->core_node.lnode.pos_vec[0] = pos;
    root->core_node.lnode.pre = NULL;
    root->core_node.lnode.next = NULL;
    root->key_count = 1;
    root->is_leaf = 1;
    return root;
}

BTreeNode* btree_new_leaf(){
    BTreeNode* leaf = malloc(1 * sizeof(BTreeNode));
    leaf->core_node.lnode.pre = NULL;
    leaf->core_node.lnode.next = NULL;
    leaf->key_count = 0;
    leaf->is_leaf = 1;
    return leaf;
}

BTreeNode* btree_new_internal(){
    BTreeNode* internal = malloc(1 * sizeof(BTreeNode));
    internal->key_count = 0;
    internal->is_leaf = 0;
    return internal;
}

BTreeNode* btree_search(BTreeNode* root, int key, BTreeNode*** access_vec, size_t* access_vec_size, int need_access_vec){
    if(root == NULL){
        return NULL;
    }
    BTreeNode* cur = root;
    size_t next_pos;
    while(!cur->is_leaf){
        next_pos = search_key(key, cur->core_node.inode.keys, cur->key_count);
        if(need_access_vec){
            *access_vec[*access_vec_size] = cur->core_node.inode.childs[next_pos];
            *access_vec_size +=1;
        }
        cur = cur->core_node.inode.childs[next_pos];
    }
    return cur;
}

size_t find_insert_pos(BTreeNode* node, int key){
    int pos;
    if(node->is_leaf){
        pos = search_key(key, node->core_node.lnode.key_vec, node->key_count);
        while(pos < node->key_count && node->core_node.lnode.key_vec[pos] == key){
            pos++;
        }
    }else{
        pos = search_key(key, node->core_node.inode.keys, node->key_count);
        while(pos < node->key_count && node->core_node.inode.keys[pos] == key){
            pos++;
        }
    }
    return pos;
}

void update_index_pos_vec(BTreeNode* leaf, int pos, int is_insert){
    BTreeNode* cur;
    if(is_insert){
        //adding a new key pos pair
        //update current leaf first
        for(int i=0;i<leaf->key_count;i++){
            if(leaf->core_node.lnode.pos_vec[i] >= pos){
                leaf->core_node.lnode.pos_vec[i]++;
            }
        }
        //update all leaf nodes following current leaf node
        cur = leaf->core_node.lnode.next;
        while(cur){
            for(int i=0;i<cur->key_count;i++){
                if(cur->core_node.lnode.pos_vec[i] >= pos){
                    leaf->core_node.lnode.pos_vec[i]++;
                }
            }
            cur=cur->core_node.lnode.next;
        }
        //NECESSARY: update all leaf nodes preceding the current leaf node
        //TODO: is the following code needed for clustered btree?
        cur = leaf->core_node.lnode.pre;
        while(cur){
            for(int i=0;i<cur->key_count;i++){
                if(cur->core_node.lnode.pos_vec[i] >= pos){
                    leaf->core_node.lnode.pos_vec[i]++;
                }
            }
            cur=cur->core_node.lnode.pre;
        }
    }else{
        //removing an existing key pos pair
        //update current leaf first
        for(int i=0;i<leaf->key_count;i++){
            if(leaf->core_node.lnode.pos_vec[i] >= pos){
                leaf->core_node.lnode.pos_vec[i]--;
            }
        }
        //update all leaf nodes following current leaf node
        cur = leaf->core_node.lnode.next;
        while(cur){
            for(int i=0;i<cur->key_count;i++){
                if(cur->core_node.lnode.pos_vec[i] >= pos){
                    leaf->core_node.lnode.pos_vec[i]--;
                }
            }
            cur=cur->core_node.lnode.next;
        }
        //NECESSARY: update all leaf nodes preceding the current leaf node
        //TODO: is the following code needed for clustered btree?
        cur = leaf->core_node.lnode.pre;
        while(cur){
            for(int i=0;i<cur->key_count;i++){
                if(cur->core_node.lnode.pos_vec[i] >= pos){
                    leaf->core_node.lnode.pos_vec[i]--;
                }
            }
            cur=cur->core_node.lnode.pre;
        }
    }
}

BTreeNode* btree_insert_leaf_simple(BTreeNode* leaf, int key, int pos, int insert_at_ordered_column_middle_pos, BTreeNode* root){
    size_t insert_pos = find_insert_pos(leaf, key);
    if(insert_at_ordered_column_middle_pos){
        update_index_pos_vec(leaf, pos, 1);
    }
    for(size_t i=leaf->key_count;i>insert_pos;i--){
        leaf->core_node.lnode.key_vec[i] = leaf->core_node.lnode.key_vec[i-1];
        leaf->core_node.lnode.pos_vec[i] = leaf->core_node.lnode.pos_vec[i-1];
    }
    leaf->core_node.lnode.key_vec[insert_pos] = key;
    leaf->core_node.lnode.pos_vec[insert_pos] = pos;
    leaf->key_count++;
    return root;
}

BTreeNode* btree_create_new_root_and_insert(BTreeNode* left_child, BTreeNode* right_child, int extra_key){
    BTreeNode* new_root = btree_new_internal();
    new_root->core_node.inode.childs[0] = left_child;
    new_root->core_node.inode.childs[1] = right_child;
    new_root->core_node.inode.keys[0] = extra_key;
    return new_root;
}

BTreeNode* btree_insert_internal_simple(BTreeNode* internal, BTreeNode* right_child, int extra_key, BTreeNode* root){
    
    size_t insert_pos = find_insert_pos(internal, extra_key);
    for(size_t i=internal->key_count;i>insert_pos;i++){
        internal->core_node.inode.keys[i] = internal->core_node.inode.keys[i-1];
        internal->core_node.inode.childs[i+1] = internal->core_node.inode.childs[i];
    }
    internal->core_node.inode.keys[insert_pos] = extra_key;
    internal->core_node.inode.childs[insert_pos+1] = right_child;
    internal->key_count++;
    return root;
}

BTreeNode* btree_split_and_insert_internal(BTreeNode* internal, BTreeNode* right_child, int extra_key, BTreeNode* root, BTreeNode*** access_vec, size_t* access_vec_size){
    int temp_keys[FANOUT];
    void* temp_childs[FANOUT+1];
    size_t insert_pos = find_insert_pos(internal, extra_key);
    temp_keys[insert_pos] = extra_key;
    temp_childs[insert_pos+1] = right_child;
    size_t new_index=0;
    size_t old_index=0;
    while(old_index < FANOUT-1){
        if(new_index == insert_pos){
            new_index++;
        }
        temp_keys[new_index] = internal->core_node.inode.keys[old_index];
        new_index++;
        old_index++;
    }
    new_index=0;
    old_index=0;
    while(old_index < FANOUT){
        if(new_index == insert_pos+1){
            new_index++;
        }
        temp_childs[old_index] = internal->core_node.inode.childs[old_index];
        new_index++;
        old_index++;
    }
    BTreeNode* right_internal = btree_new_internal();
    BTreeNode* left_internal = internal;
    size_t middle_point = FANOUT/2;
    int new_extra_key = temp_keys[middle_point];
    for(size_t i=0;i<middle_point;i++){
        left_internal->core_node.inode.keys[i] = temp_keys[i];
        left_internal->core_node.inode.childs[i] = temp_childs[i];
    }
    left_internal->key_count = middle_point;
    left_internal->core_node.inode.childs[middle_point] = temp_childs[middle_point];
    new_index=middle_point+1;
    old_index=0;
    while(new_index < FANOUT){
        right_internal->core_node.inode.keys[old_index] = temp_keys[new_index];
        right_internal->core_node.inode.childs[old_index] = temp_childs[new_index];
        new_index++;
        old_index++;
    }
    right_internal->key_count = FANOUT-1-middle_point;
    right_internal->core_node.inode.childs[old_index] = temp_childs[new_index];
    BTreeNode* new_internal = NULL;
    if(*access_vec_size>=1){
        new_internal = *access_vec[*access_vec_size-1];
        *access_vec_size -= 1;
    }
    return btree_insert_internal(new_internal, left_internal, right_internal, new_extra_key, root, access_vec, access_vec_size);
}

BTreeNode* btree_insert_internal(BTreeNode* internal, BTreeNode* left_child, BTreeNode* right_child, int extra_key, BTreeNode* root, BTreeNode*** access_vec, size_t* access_vec_size){
    if(internal == NULL){
        return btree_create_new_root_and_insert(left_child, right_child, extra_key);
    }
    if(internal->key_count+1 < FANOUT){
        return btree_insert_internal_simple(internal, right_child, extra_key, root);
    }else{
        return btree_split_and_insert_internal(internal, right_child, extra_key, root, access_vec, access_vec_size);
    }
}

BTreeNode* btree_split_and_insert_leaf(BTreeNode* leaf, int key, int pos, int insert_at_ordered_column_middle_pos, BTreeNode* root, BTreeNode*** access_vec, size_t* access_vec_size){
    size_t insert_pos = find_insert_pos(leaf, key);
    //TODO: might be buggy here. Is situation exactly the same here?
    if(insert_at_ordered_column_middle_pos){
        update_index_pos_vec(leaf, pos, 1);
    }
    int temp_key_vec[LEAF_SIZE+1];
    int temp_pos_vec[LEAF_SIZE+1];
    temp_key_vec[insert_pos] = key;
    temp_pos_vec[insert_pos] = pos;
    //put all key pos pairs in one large container first before splitting
    size_t new_index=0;
    size_t old_index=0;
    while(old_index<LEAF_SIZE){
        if(new_index == insert_pos){
            new_index++;
        }
        temp_key_vec[new_index] = leaf->core_node.lnode.key_vec[old_index];
        temp_pos_vec[new_index] = leaf->core_node.lnode.pos_vec[old_index];
        new_index++;
        old_index++;
    }
    //redistribute the key pos pairs
    BTreeNode* left_leaf = leaf;
    BTreeNode* right_leaf = btree_new_leaf();
    size_t middle_point = (LEAF_SIZE+1)/2;
    for(size_t i=0;i<middle_point;i++){
        left_leaf->core_node.lnode.key_vec[i] = temp_key_vec[i];
        left_leaf->core_node.lnode.pos_vec[i] = temp_pos_vec[i];
    }
    left_leaf->key_count = middle_point;
    new_index=middle_point;
    old_index=0;
    while(new_index<(LEAF_SIZE+1)){
        right_leaf->core_node.lnode.key_vec[old_index] = temp_key_vec[new_index];
        right_leaf->core_node.lnode.pos_vec[old_index] = temp_key_vec[new_index];
        new_index++;
        old_index++;
    }
    right_leaf->core_node.lnode.pre = left_leaf;
    right_leaf->core_node.lnode.next = left_leaf->core_node.lnode.next;
    right_leaf->key_count = LEAF_SIZE+1-middle_point;
    left_leaf->core_node.lnode.next = right_leaf;
    
    BTreeNode* internal = NULL;
    if(*access_vec_size>=1){
        internal = *access_vec[*access_vec_size-1];
        *access_vec_size -= 1;
    }
    return btree_insert_internal(internal, left_leaf, right_leaf, right_leaf->core_node.lnode.key_vec[0], root, access_vec, access_vec_size);
}

BTreeNode* btree_insert(BTreeNode* root, int key, int pos, int insert_at_ordered_column_middle_pos){
    if(root == NULL){
        return btree_create(key, pos);
    }
    
    //TODO: use access queue here?
    BTreeNode** access_vec = malloc(MAX_TREE_HEIGHT * sizeof(BTreeNode*));
    size_t access_vec_size=0;
    access_vec[0]=root;
    access_vec_size++;
    
    BTreeNode* leaf = btree_search(root, key, &access_vec, &access_vec_size, 1);
    BTreeNode* new_root = NULL;
    if(leaf->key_count < LEAF_SIZE){
        new_root = btree_insert_leaf_simple(leaf, key, pos, insert_at_ordered_column_middle_pos, root);
        free(access_vec);
        return new_root;
    }else{
        new_root = btree_split_and_insert_leaf(leaf, key, pos, insert_at_ordered_column_middle_pos, root, &access_vec, &access_vec_size);
        free(access_vec);
        return new_root;
    }
}
void btree_find_real_start_leaf_and_index(BTreeNode* leaf, int key, BTreeNode** real_start_leaf_pointer, int* real_start_pos_pointer){
    int pos = search_key(key, leaf->core_node.lnode.key_vec, leaf->key_count);
    BTreeNode* pre;
    int pre_pos;
    if(pos == 0){
        //handle duplicate keys splitted into two or more leafs
        pre = leaf->core_node.lnode.pre;
        while(pre){
            pre_pos = search_key(key, pre->core_node.lnode.key_vec, pre->key_count);
            if(pre_pos<pre->key_count){
                leaf = pre;
                //our binary search does not guarantee that this is the first matching key in this leaf
                while(pre_pos>0 && pre->core_node.lnode.key_vec[pre_pos-1] == key){
                    pre_pos--;
                }
                pos = pre_pos;
                if(pre_pos != 0){
                    //this ensures that this is the first matching key
                    break;
                }else{
                    pre = pre->core_node.lnode.pre;
                }
            }else{
                //larger than all keys in this leaf, we are done
                break;
            }
        }
    }
    *real_start_leaf_pointer = leaf;
    *real_start_pos_pointer = pos;
}

int btree_find_pos_clustered(BTreeNode* root, int key, int include_key){
    BTreeNode* leaf = btree_search(root, key, NULL, NULL, 0);
    if(!leaf){
        return 0;
    }
    BTreeNode* real_start_leaf = NULL;
    int real_start_index = 0;
    //handle duplicate keys
    btree_find_real_start_leaf_and_index(leaf, key, &real_start_leaf, &real_start_index);
    if(include_key){
        //include lowerbound
        if(real_start_index<real_start_leaf->key_count){
            return real_start_leaf->core_node.lnode.pos_vec[real_start_index];
        }else{
            if(real_start_leaf->core_node.lnode.next){
                return real_start_leaf->core_node.lnode.next->core_node.lnode.pos_vec[0];
            }else{
                //Not sure the return statement is 100% correct due to the implementation of our binary search. Check this section for source of bug later.
                cs165_log(stdout, "WARNING: possible buggy line of code in btree_find_pos_clustered function in utils.c executed\n");
                //if our lowerbound is larger than the maximum of our column, should we just return some special values instead?
                return -1;
            }
        }
    }else{
        //exclude upperbound
        if(real_start_index==0){
            if(real_start_leaf->core_node.lnode.pre){
                BTreeNode* real_pre_leaf=real_start_leaf->core_node.lnode.pre;
                size_t pre_idx = real_pre_leaf->key_count - 1;
                return real_pre_leaf->core_node.lnode.pos_vec[pre_idx] + 1;
            }else{
                //Not sure the return statement is 100% correct due to the implementation of our binary search. Check this section for source of bug later.
                cs165_log(stdout, "WARNING: possible buggy line of code in btree_find_pos_clustered function in utils.c executed\n");
                //if our upperbound is smaller than the maximum of our column, should we just return some special values instead?
                return -1;
            }
        }else{
            return real_start_leaf->core_node.lnode.pos_vec[real_start_index-1] + 1;
        }
    }
}

void btree_find_pos_unclustered(BTreeNode* root, Comparator* comp, int** qualifying_index_add, size_t* index_count_add){
    int* qualifying_index = *qualifying_index_add;
    size_t index_count=0;
    int lowerbound;
    int upperbound;
    if(comp->ct1 != NO_COMPARISON && comp->ct2 != NO_COMPARISON){
        lowerbound = comp->lowerbound;
        upperbound = comp->upperbound;
        
        BTreeNode* lowerbound_leaf = btree_search(root, lowerbound, NULL, NULL, 0);
        BTreeNode* lowerbound_start_leaf = NULL;
        int lowerbound_start_index = 0;
        btree_find_real_start_leaf_and_index(lowerbound_leaf, lowerbound, &lowerbound_start_leaf, &lowerbound_start_index);
        
        BTreeNode* upperbound_leaf = btree_search(root, upperbound, NULL, NULL, 0);
        BTreeNode* upperbound_start_leaf = NULL;
        int upperbound_start_index = 0;
        btree_find_real_start_leaf_and_index(upperbound_leaf, upperbound, &upperbound_start_leaf, &upperbound_start_index);
        
        int start = lowerbound_start_index;
        int end;
        if(lowerbound_start_leaf == upperbound_start_leaf){
            end = upperbound_start_index;
            for(int i=start;i<end;i++){
                qualifying_index[index_count] = lowerbound_start_leaf->core_node.lnode.pos_vec[i];
            }
        }else{
            end = lowerbound_start_leaf->key_count;
            for(int i=start;i<end;i++){
                qualifying_index[index_count] = lowerbound_start_leaf->core_node.lnode.pos_vec[i];
                index_count++;
            }
            //traverse from lowerbound_start_leaf to upperbound_start_leaf
            start = 0;
            BTreeNode* cur = lowerbound_start_leaf->core_node.lnode.next;
            while(cur && cur->core_node.lnode.pre != upperbound_start_leaf){
                if(cur == upperbound_start_leaf){
                    end = upperbound_start_index;
                }else{
                    end = cur->key_count;
                }
                for(int i=start;i<end;i++){
                    qualifying_index[index_count] = cur->core_node.lnode.pos_vec[i];
                    index_count++;
                }
                start = 0;
                cur = cur->core_node.lnode.next;
            }
        }
    }else if(comp->ct1 != NO_COMPARISON){
        lowerbound = comp->lowerbound;
        
        BTreeNode* lowerbound_leaf = btree_search(root, lowerbound, NULL, NULL, 0);
        BTreeNode* lowerbound_start_leaf = NULL;
        int lowerbound_start_index = 0;
        btree_find_real_start_leaf_and_index(lowerbound_leaf, lowerbound, &lowerbound_start_leaf, &lowerbound_start_index);
        
        int start = lowerbound_start_index;
        for(int i=start;i<lowerbound_start_leaf->key_count;i++){
            qualifying_index[index_count] = lowerbound_start_leaf->core_node.lnode.pos_vec[i];
            index_count++;
        }
        
        //forward: traverse from lowerbound_start_leaf to end of the list of leaf node
        BTreeNode* cur = lowerbound_start_leaf->core_node.lnode.next;
        while(cur){
            for(int i=0;i<cur->key_count;i++){
                qualifying_index[index_count] = cur->core_node.lnode.pos_vec[i];
                index_count++;
            }
            cur = cur->core_node.lnode.next;
        }
    }else if(comp->ct2 != NO_COMPARISON){
        upperbound = comp->upperbound;
        
        BTreeNode* upperbound_leaf = btree_search(root, upperbound, NULL, NULL, 0);
        BTreeNode* upperbound_start_leaf = NULL;
        int upperbound_start_index = 0;
        btree_find_real_start_leaf_and_index(upperbound_leaf, upperbound, &upperbound_start_leaf, &upperbound_start_index);
        
        int end = upperbound_start_index;
        for(int i=end-1;i>=0;i--){
            qualifying_index[index_count] = upperbound_start_leaf->core_node.lnode.pos_vec[i];
            index_count++;
        }
        //backward: traverse from upperbound_start_leaf to start of the list of leaf node
        BTreeNode* cur = upperbound_start_leaf->core_node.lnode.pre;
        while(cur){
            for(int i=cur->key_count-1;i>=0;i--){
                qualifying_index[index_count] = cur->core_node.lnode.pos_vec[i];
                index_count++;
            }
            cur = cur->core_node.lnode.pre;
        }
    }
    //by design, comp->ct1 != NO_COMPARISON || comp->ct2 != NO_COMPARISON
    
    //no need to realloc memory for qualifying_index. The realloc will be done in scan function later.
    *index_count_add = index_count;
}

void btree_remove(BTreeNode* root, int key, int pos){
    BTreeNode* leaf = btree_search(root, key, NULL, NULL, 0);
    BTreeNode* real_start_leaf = NULL;
    int real_start_index = 0;
    btree_find_real_start_leaf_and_index(leaf, key, &real_start_leaf, &real_start_index);
    BTreeNode* cur = real_start_leaf;
    int cur_index = real_start_index;
    while(cur != NULL){
        if(cur->core_node.lnode.pos_vec[cur_index] == pos){
            for(int i=cur_index;i+1<cur->key_count;i++){
                cur->core_node.lnode.key_vec[i]=cur->core_node.lnode.key_vec[i+1];
                cur->core_node.lnode.pos_vec[i]=cur->core_node.lnode.pos_vec[i+1];
            }
            cur->key_count--;
            break;
        }else{
            cur_index++;
            if(cur_index >= cur->key_count){
                //go to the next leaf
                cur = cur->core_node.lnode.next;
                cur_index = 0;
            }
        }
    }
    //do not merge btree leaf
    update_index_pos_vec(leaf, pos, 0);
}

void sorted_insert_val_vec(int* vec, int vec_size, int idx, int val){
    //only needs to shift
    for(int i=vec_size;i>idx;i--){
        vec[i]=vec[i-1];
    }
    vec[idx]=val;
}

void sorted_insert_pos_vec(int* vec, int vec_size, int idx, int pos_val, int insert_at_ordered_column_middle_pos){
    if(insert_at_ordered_column_middle_pos){
        //update pos val first
        for(int i=0;i<vec_size;i++){
            if(vec[i]>=pos_val){
                vec[i]++;
            }
        }
    }
    //shift
    for(int i=vec_size;i>idx;i--){
        vec[i]=vec[i-1];
    }
    vec[idx]=pos_val;
}

ColumnIndex* sorted_insert(ColumnIndex* ci, int size, int key, int pos, int insert_at_ordered_column_middle_pos){
    size_t insert_pos = search_key(key, ci->key_vec, size);
    sorted_insert_val_vec(ci->key_vec, size, key, insert_pos);
    sorted_insert_pos_vec(ci->key_vec, size, pos, insert_pos, insert_at_ordered_column_middle_pos);
    return ci;
}

void sorted_delete_and_update(ColumnIndex* ci, int size, int pos){
    int* key_vec = ci->key_vec;
    int* pos_vec = ci->pos_vec;
    for(int i=pos;i+1<size;i++){
        key_vec[i] = key_vec[i+1];
        pos_vec[i] = pos_vec[i+1];
        if(pos_vec[i]>pos){
            pos_vec[i]--;
        }
    }
    for(int i=0;i<pos;i++){
        if(pos_vec[i]>pos){
            pos_vec[i]--;
        }
    }
}

void update_column_index(Column* column, int key, size_t pos, int no_need_to_shift){
    //TODO:double check if we get the flag conditions right
    int insert_at_ordered_column_middle_pos_flag = !no_need_to_shift && pos != column->size && column->clustered;
    if(column->it==BTREE_CLUSTERED){
        column->index_file = (void*) btree_insert((BTreeNode*) column->index_file, key, pos, insert_at_ordered_column_middle_pos_flag);
    }else if(column->it==BTREE_UNCLUSTERED){
        column->index_file = (void*) btree_insert((BTreeNode*) column->index_file, key, pos, insert_at_ordered_column_middle_pos_flag);
    }else if(column->it==SORTED_UNCLUSTERED){
        column->index_file = (void*) sorted_insert((ColumnIndex*) column->index_file, column->size, key, pos, insert_at_ordered_column_middle_pos_flag);
    }
}

ExtHashTable* hashtable_create(){
    ExtHashTable* ht = malloc(sizeof(ExtHashTable));
    ht->global_bit_len = INITIAL_BIT_LEN;
    ht->bucket_num = 1<<INITIAL_BIT_LEN;
    ht->buckets = malloc(ht->bucket_num * sizeof(Bucket*));
    //needed to free later
    ht->btracker_size = ht->bucket_num;
    ht->btracker_capacity = 2 * ht->bucket_num;
    ht->btracker = malloc(ht->btracker_capacity * sizeof(Bucket*));
    for(size_t i=0;i<ht->bucket_num;i++){
        ht->buckets[i] = calloc(1, sizeof(Bucket));
        ht->btracker[i] = ht->buckets[i];
        ht->buckets[i]->key_count = 0;
        ht->buckets[i]->local_bit_len = INITIAL_BIT_LEN;
    }
    return ht;
}

/**
 * Use the same sdbm algorithm as used by SteveKekacs's project
 * which claims to have nice distribution properties for our bit manipulating hash table
 **/
unsigned long hash_func(int key){
    unsigned char* str = (unsigned char*) &key;
    unsigned long res = 0;
    int cur;
    while((cur=*str++)){
        res = cur + (res<<6) + (res<<16) - res;
    }
    return res;
}

int hashtable_get_bucket_pos(ExtHashTable* ht, int key){
    unsigned long hashed_key = hash_func(key);
    int bit_len = ht->global_bit_len;
    int mask = (1<<bit_len)-1;
    return (int) hashed_key & mask;
}

void hashtable_expand(ExtHashTable* ht){
    size_t pre_bucket_num = ht->bucket_num;
    ht->bucket_num = 2 * ht->bucket_num;
    ht->global_bit_len++;
    ht->buckets = realloc(ht->buckets, ht->bucket_num * sizeof(Bucket*));
    int pre_bucket_pos;
    for(size_t i= pre_bucket_num; i<ht->bucket_num;i++){
        pre_bucket_pos = i-pre_bucket_num;
        ht->buckets[i] = ht->buckets[pre_bucket_pos];
    }
}

void hashtable_split_bucket(ExtHashTable* ht, int bucket_pos){
    Bucket* target_bucket = ht->buckets[bucket_pos];
    int temp_key_vec[BUCKET_SIZE];
    PositionList* temp_val_vec[BUCKET_SIZE];
    for(size_t i=0;i<BUCKET_SIZE;i++){
        temp_key_vec[i] = target_bucket->key_vec[i];
        temp_val_vec[i] = target_bucket->val_vec[i];
        //cs165_log(stdout, "copied key_vec: %d\n", target_bucket->key_vec[i]);
    }
//    cs165_log(stdout, "hash table bucket num before expand: %d\n", ht->bucket_num);
//    cs165_log(stdout, "target bucket pos before expand: %d\n", bucket_pos);
//    cs165_log(stdout, "hash table global bit len before expand: %d\n", ht->global_bit_len);
//    cs165_log(stdout, "target bucket local bit len before expand: %d\n", target_bucket->local_bit_len);
    if(target_bucket->local_bit_len == ht->global_bit_len){
        hashtable_expand(ht);
    }
    
    int has_encountered_count = 0;
    for(size_t i=0;i<ht->bucket_num;i++){
        if(ht->buckets[i] == target_bucket){
            has_encountered_count++;
            if(has_encountered_count > 1){
                ht->buckets[i] = calloc(1, sizeof(Bucket));
                //update btracker
                if(ht->btracker_size == ht->btracker_capacity){
                    ht->btracker_capacity *= 2;
                    ht->btracker = realloc(ht->btracker, ht->btracker_capacity * sizeof(Bucket*));
                }
                ht->btracker[ht->btracker_size] = ht->buckets[i];
                ht->btracker_size++;
                
                ht->buckets[i]->key_count = 0;
                ht->buckets[i]->local_bit_len = ht->global_bit_len;
            }else{
                ht->buckets[i]->key_count = 0;
                ht->buckets[i]->local_bit_len = ht->global_bit_len;
            }
        }
    }
    
    int new_insert_pos;
    Bucket* cur;
    for(size_t i=0;i<BUCKET_SIZE;i++){
        new_insert_pos = hashtable_get_bucket_pos(ht, temp_key_vec[i]);
        //cs165_log(stdout, "new insert position: %d\n", new_insert_pos);
        cur = ht->buckets[new_insert_pos];
        cur->key_vec[cur->key_count] = temp_key_vec[i];
        cur->val_vec[cur->key_count] = temp_val_vec[i];
        cur->key_count++;
    }
    //cs165_log(stdout, "target bucket key count: %d\n", target_bucket->key_count);
}

void hashtable_insert(ExtHashTable* ht, int key, int val){
    int bucket_pos = hashtable_get_bucket_pos(ht, key);
    Bucket* target_bucket = ht->buckets[bucket_pos];
    //TODO: check if the key is in there already
    PositionList* pl;
    for(size_t i=0;i<BUCKET_SIZE;i++){
        if(target_bucket->key_vec[i] == key){
            //found the key
            //check capacity first
            pl = target_bucket->val_vec[i];
            if(pl->size == pl->capacity){
                pl->capacity = 2 * pl->capacity;
                pl->pos_vec = realloc(pl->pos_vec, pl->capacity * sizeof(int));
            }
            pl->pos_vec[pl->size] = val;
            pl->size++;
            return;
        }
    }
    //Otherwise, have to add new key
    if(target_bucket->key_count < BUCKET_SIZE){
        target_bucket->key_vec[target_bucket->key_count] = key;
        target_bucket->val_vec[target_bucket->key_count] = malloc(1 * sizeof(PositionList));
        target_bucket->val_vec[target_bucket->key_count]->pos_vec = malloc(INITIAL_POSITIONLIST_LEN * sizeof(int));
        target_bucket->val_vec[target_bucket->key_count]->pos_vec[0] = val;
        target_bucket->val_vec[target_bucket->key_count]->capacity = INITIAL_POSITIONLIST_LEN;
        target_bucket->val_vec[target_bucket->key_count]->size = 1;
        target_bucket->key_count++;
    }else{
        //split bucket
        hashtable_split_bucket(ht, bucket_pos);
        bucket_pos = hashtable_get_bucket_pos(ht, key);
        target_bucket = ht->buckets[bucket_pos];
        while(target_bucket->key_count == BUCKET_SIZE){
            //cs165_log(stdout, "Warning: have to expand for another time\n");
            hashtable_split_bucket(ht, bucket_pos);
            bucket_pos = hashtable_get_bucket_pos(ht, key);
            target_bucket = ht->buckets[bucket_pos];
        }
        target_bucket->key_vec[target_bucket->key_count] = key;
        target_bucket->val_vec[target_bucket->key_count] = malloc(1 * sizeof(PositionList));
        target_bucket->val_vec[target_bucket->key_count]->pos_vec = malloc(INITIAL_POSITIONLIST_LEN * sizeof(int));
        target_bucket->val_vec[target_bucket->key_count]->pos_vec[0] = val;
        target_bucket->val_vec[target_bucket->key_count]->capacity = INITIAL_POSITIONLIST_LEN;
        target_bucket->val_vec[target_bucket->key_count]->size = 1;
        target_bucket->key_count++;
    }
}

void hashtable_probe(ExtHashTable* ht, int key, int* res_pos_vec, size_t* res_tuples_num_p){
    size_t res_tuples_num = 0;
    int bucket_pos = hashtable_get_bucket_pos(ht, key);
    Bucket* target_bucket = ht->buckets[bucket_pos];
    PositionList* pl;
    for(size_t i=0;i<target_bucket->key_count;i++){
        if(target_bucket->key_vec[i] == key){
            pl = target_bucket->val_vec[i];
            for(size_t j=0;j<pl->size;j++){
                res_pos_vec[res_tuples_num] = pl->pos_vec[j];
                res_tuples_num++;
            }
        }
    }
    *res_tuples_num_p = res_tuples_num;
}

void hashtable_free(ExtHashTable* ht){
    Bucket* bucket;
    PositionList* pl;
    for(size_t i=0;i<ht->btracker_size;i++){
        bucket = ht->btracker[i];
        for(size_t j=0;j<bucket->key_count;j++){
            pl = bucket->val_vec[j];
            if(pl->size){
                free(pl->pos_vec);
            }
            free(pl);
        }
        free(bucket);
    }
    free(ht->btracker);
    free(ht->buckets);
    free(ht);
}
