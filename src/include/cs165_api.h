

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <message.h>

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64
#define PAGE_SIZE 4096
#define SAFE_MARGIN 64
//#define FANOUT ((PAGE_SIZE-2*sizeof(int)-SAFE_MARGIN)/(8+sizeof(int)))//tunes later
//#define LEAF_SIZE ((PAGE_SIZE-2*sizeof(int)-16-SAFE_MARGIN)/(2*sizeof(int))) //tunes later
#define FANOUT 335
#define LEAF_SIZE 501
#define MAX_TREE_HEIGHT 512
#define BUCKET_SIZE 32 //tunes later
#define INITIAL_BIT_LEN 4
#define INITIAL_POSITIONLIST_LEN 256
/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef enum DataType {
     INT,
     FLOAT,
     LONG,
} DataType;

typedef struct PositionList{
    int* pos_vec;
    size_t capacity;
    size_t size;
} PositionList;

typedef struct Bucket{
    int key_vec[BUCKET_SIZE];
    PositionList* val_vec[BUCKET_SIZE];
    size_t key_count;
    size_t local_bit_len;
} Bucket;

//typedef struct Bucket{
//    int key_vec[BUCKET_SIZE];
//    int val_vec[BUCKET_SIZE];
//    size_t key_count;
//    size_t local_bit_len;
//} Bucket;

typedef struct ExtHashTable{
    Bucket** buckets;
    size_t bucket_num;
    size_t global_bit_len;
    Bucket** btracker;
    size_t btracker_size;
    size_t btracker_capacity;
} ExtHashTable;

typedef enum IndexType {
    BTREE_CLUSTERED,
    BTREE_UNCLUSTERED,
    SORTED_CLUSTERED,
    SORTED_UNCLUSTERED,
    NONE,
} IndexType;

typedef struct BTreeNode BTreeNode;

typedef struct BTreeInternalNode{
    BTreeNode* childs[FANOUT];
    int keys[FANOUT-1];
} BTreeInternalNode;

typedef struct BTreeLeafNode{
    int key_vec[LEAF_SIZE];
    int pos_vec[LEAF_SIZE];
    struct BTreeNode* pre;
    struct BTreeNode* next;
} BTreeLeafNode;

typedef union GBTreeNode{ //generalized BTreeNode
    BTreeInternalNode inode;
    BTreeLeafNode lnode;
} GBTreeNode;

struct BTreeNode{
    GBTreeNode core_node;
    int key_count;
    int is_leaf;
};

typedef struct ColumnIndex {
    int* key_vec;
    int* pos_vec;
} ColumnIndex;


/* Alternative design: does not make much sense though if updates are often
typedef struct ColumnIndex {
    IndexPair* indexes;
} ColumnIndex;
 */

typedef struct IndexPair {
    int key;
    int pos;
} IndexPair;

typedef struct Column {
    char name[MAX_SIZE_NAME]; 
    int* data;
    size_t size;
    // You will implement column indexes later.
    void* index_file;
    IndexType it;
    int clustered;
} Column;


/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table {
    char name [MAX_SIZE_NAME];
    Column *columns;
    size_t col_count;
    size_t col_capacity;
    size_t table_length;
    size_t table_length_capacity;
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME]; 
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GCType {
    RESULT,
    COLUMN,
} GCType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GCPointer {
    Result* result;
    Column* column;
} GCPointer;


/*
 * used to refer to a column in our client context
 */

typedef struct GCHandle {
    GCPointer p;
    char name[HANDLE_MAX_SIZE];
    GCType type;
} GCHandle;

typedef enum ContextType {
    TABLE,
    GCOLUMN,
} ContextType;

typedef struct ContextNode {
    void* p;
    char name[MAX_SIZE_NAME];
    ContextType type;
    struct ContextNode* next;
} ContextNode;

typedef struct ContextTable {
    ContextNode** buckets;
    size_t size;
}ContextTable;

/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    ComparatorType ct1;
    ComparatorType ct2;
    long int lowerbound; // used in equality and ranges.
    long int upperbound; // used in range compares.
} Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    SELECT,
    AGGREGATE,
    FETCH,
    PRINT,
    LOAD,
    JOIN,
    DELETE,
    UPDATE,
    SHUTDOWN,
    BATCH_MODE_BEGIN,
    BATCH_MODE_EXECUTE,
} OperatorType;


typedef enum CreateType {
    _DB,
    _TABLE,
    _COLUMN,
    _IDX,
} CreateType;

/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating. 
 * For example, if create_type == _DB, the operator should create a db named <<name>> 
 * if create_type = _TABLE, the operator should create a table named <<name>> with <<col_count>> columns within db <<db>>
 * if create_type = = _COLUMN, the operator should create a column named <<name>> within table <<table>>
 */
typedef struct CreateOperator {
    CreateType create_type; 
    char name[MAX_SIZE_NAME]; 
    Db* db;
    Table* table;
    Column* column;
    int col_count;
    IndexType it;
} CreateOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;
/*
* necessary fields for select
*/
typedef struct SelectOperator {
    GCHandle* gch1;
    GCHandle* gch2;
    Comparator comparator;
} SelectOperator;
/*
* necessary fields for aggregate
*/
typedef enum AggregateType {
    MIN,
    MAX,
    SUM,
    AVG,
    ADD,
    SUB,
} AggregateType;

typedef struct AggregateOperator {
    GCHandle* gch1;
    GCHandle* gch2;
    AggregateType type;
} AggregateOperator;
/*
* necessary fields for fetch
*/
typedef struct FetchOperator {
    GCHandle* gch1;
    GCHandle* gch2;
} FetchOperator;
/*
* necessary fields for print
*/
typedef struct PrintOperator {
    GCHandle** gch_list;
    size_t gch_count;
} PrintOperator;
/*
 * necessary fields for load
 */
typedef struct LoadOperator {
    Table* table;
} LoadOperator;
/*
* necessary fields for join
*/
typedef enum JoinType {
    NESTED_LOOP,
    HASH,
} JoinType;

typedef struct JoinOperator {
    Result* val_vec1;
    Result* pos_vec1;
    Result* val_vec2;
    Result* pos_vec2;
    JoinType jt;
} JoinOperator;
/*
* necessary fields for delete
*/
typedef struct DeleteOperator {
    Table* table;
    Result* pos_vec;
} DeleteOperator;
/*
* necessary fields for update
*/
typedef struct UpdateOperator {
    Table* table;
    Column* col;
    Result* pos_vec;
    int new_value;
} UpdateOperator;
/*
 * necessary fields for shutdown
 */
typedef struct ShutDownOperator {
    int place_holder; //might want to check if we are shuting the right db later
} ShutDownOperator;
/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    CreateOperator create_operator;
    InsertOperator insert_operator;
    LoadOperator load_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    AggregateOperator aggregate_operator;
    JoinOperator join_operator;
    DeleteOperator delete_operator;
    UpdateOperator update_operator;
    ShutDownOperator shutdown_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ContextTable* context_table;
    char client_variables[2][HANDLE_MAX_SIZE];
    size_t client_variables_num;
} DbOperator;

typedef struct ThreadedScanArgs{
    size_t thread_id;
    void* data;
    size_t start;
    size_t end;
    DataType dt;
} ThreadedScanArgs;

/* 
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error. 
 */
Status db_startup();

void create_db(char* db_name, message* msg);

Table* create_table(char* name, size_t col_capacity, Db* db, message* msg);

Column* create_column(char *name, Db* db, Table* table, message* msg);

void create_idx(IndexType it, Table* table, Column* col, message* msg);

void shutdown_server(message* msg);

char** execute_db_operator(DbOperator* query, message* msg);
void db_operator_free(DbOperator* query);


extern Db* current_db;
extern ContextTable* db_catalog;
#endif /* CS165_H */

