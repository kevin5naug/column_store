// utils.h
// CS165 Fall 2015
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdarg.h>
#include <stdio.h>
#include "cs165_api.h"

/**
 * trims newline characters from a string (in place)
 **/

char* trim_newline(char *str);

/**
 * trims parenthesis characters from a string (in place)
 **/

char* trim_parenthesis(char *str);

/**
 * trims whitespace characters from a string (in place)
 **/

char* trim_whitespace(char *str);

/**
 * trims quotations characters from a string (in place)
 **/

char* trim_quotes(char *str);

// cs165_log(out, format, ...)
// Writes the string from @format to the @out pointer, extendable for
// additional parameters.
//
// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE* out, const char *format, ...);

// log_err(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stderr.
//
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char *format, ...);

// log_info(format, ...)
// Writes the string from @format to stdout, extendable for
// additional parameters. Like cs165_log, but specifically to stdout.
// Only use this when appropriate (e.g., denoting a specific checkpoint),
// else defer to using printf.
//
// Usage: log_info("Command received: %s", command_string);
void log_info(const char *format, ...);

int search_key(int key, int* key_vec, int n);

BTreeNode* btree_create(int key, int pos);

BTreeNode* btree_new_leaf();

BTreeNode* btree_new_internal();

BTreeNode* btree_search(BTreeNode* root, int key, BTreeNode*** access_vec, size_t* access_vec_size, int need_access_vec);

size_t find_insert_pos(BTreeNode* node, int key);

void update_index_pos_vec(BTreeNode* leaf, int pos, int is_insert);

BTreeNode* btree_insert_leaf_simple(BTreeNode* leaf, int key, int pos, int insert_at_ordered_column_middle_pos, BTreeNode* root);

BTreeNode* btree_create_new_root_and_insert(BTreeNode* left_child, BTreeNode* right_child, int extra_key);

BTreeNode* btree_insert_internal_simple(BTreeNode* internal, BTreeNode* right_child, int extra_key, BTreeNode* root);

BTreeNode* btree_split_and_insert_internal(BTreeNode* internal, BTreeNode* right_child, int extra_key, BTreeNode* root, BTreeNode*** access_vec, size_t* access_vec_size);

BTreeNode* btree_insert_internal(BTreeNode* internal, BTreeNode* left_child, BTreeNode* right_child, int extra_key, BTreeNode* root, BTreeNode*** access_vec, size_t* access_vec_size);

BTreeNode* btree_split_and_insert_leaf(BTreeNode* leaf, int key, int pos, int insert_at_ordered_column_middle_pos, BTreeNode* root, BTreeNode*** access_vec, size_t* access_vec_size);

BTreeNode* btree_insert(BTreeNode* root, int key, int pos, int insert_at_ordered_column_middle_pos);

void btree_find_real_start_leaf_and_index(BTreeNode* leaf, int key, BTreeNode** real_start_leaf_pointer, int* real_start_pos_pointer);

int btree_find_pos_clustered(BTreeNode* root, int key, int include_key);

void btree_find_pos_unclustered(BTreeNode* root, Comparator* comp, int** qualifying_index_add, size_t* index_count_add);

void btree_remove(BTreeNode* root, int key, int pos);

void sorted_insert_val_vec(int* vec, int vec_size, int idx, int val);

void sorted_insert_pos_vec(int* vec, int vec_size, int idx, int pos_val, int insert_at_ordered_column_middle_pos);

ColumnIndex* sorted_insert(ColumnIndex* ci, int size, int key, int pos, int insert_at_ordered_column_middle_pos);

void sorted_delete_and_update(ColumnIndex* ci, int size, int pos);

void update_column_index(Column* column, int key, size_t pos, int no_need_to_shift);

ExtHashTable* hashtable_create();

unsigned long hash_func(int key);

int hashtable_get_bucket_pos(ExtHashTable* ht, int key);

void hashtable_expand(ExtHashTable* ht);

void hashtable_split_bucket(ExtHashTable* ht, int bucket_pos);

void hashtable_insert(ExtHashTable* ht, int key, int val);

void hashtable_probe(ExtHashTable* ht, int key, int* res_pos_vec, size_t* res_tuples_num_p);

void hashtable_free(ExtHashTable* ht);

#endif /* __UTILS_H__ */
