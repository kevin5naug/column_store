/** server.c
 * CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024
#define MULTI_THREADING 1
#define THREAD_NUM 4

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 * 
 * Getting started hints: 
 *      What are the structural attributes of a `query`?
 *      How will you interpret different queries?
 *      How will you ensure different queries invoke different execution paths in your code?
 **/

int batch_mode=0;
size_t batch_size=0;
DbOperator** batched_queries=NULL;
Result** batched_results=NULL;

//to store results for multithreaded shared scan
//takes the shape (THREAD_NUM, batch_size, tuples_num)
//only support column literals at the moment
//TODO: support other data types
int*** threaded_batched_results=NULL;
int** threaded_batched_results_count=NULL;

void free_query(DbOperator* query){
    if(query->type == INSERT){
        free(query->operator_fields.insert_operator.values);
    }else if(query->type == PRINT){
        free(query->operator_fields.print_operator.gch_list);
    }
    free(query);
}

void free_batched_queries(){
    DbOperator* query;
    for(size_t i=0;i<batch_size;i++){
        query=batched_queries[i];
        free_query(query);
    }
    free(batched_queries);
    batched_queries=NULL;
}

void execute_create_operator(DbOperator* query, message* msg){
    CreateOperator operator = query->operator_fields.create_operator;
    if(operator.create_type == _DB){
        create_db(operator.name, msg);
    }else if(operator.create_type == _TABLE){
        create_table(operator.name, operator.col_count, operator.db, msg);
    }else if(operator.create_type == _COLUMN){
        create_column(operator.name, operator.db, operator.table, msg);
    }else if(operator.create_type == _IDX){
        create_idx(operator.it, operator.table, operator.column, msg);
    }
    else{
        msg->status = INCORRECT_FORMAT;
    }
}

//separated from execute_insert_operator so we can use it for update
void execute_insert(Table* table, int* values, message* msg){
    Column* columns = table->columns;
    if(table->table_length > table->table_length_capacity/2){
        //reserving space in advance might be beneficial, might change later
        table->table_length_capacity *= 2;
        for(size_t i=0;i<table->col_capacity;i++){
            columns[i].data = realloc(columns[i].data, sizeof(int) * table->table_length_capacity);
            if(columns[i].it == SORTED_UNCLUSTERED){
                //other index type does not need to allocate additional memory
                ColumnIndex* ci = (ColumnIndex*) columns[i].index_file;
                ci->key_vec = realloc(ci->key_vec, sizeof(int) * table->table_length_capacity);
                ci->pos_vec = realloc(ci->pos_vec, sizeof(int) * table->table_length_capacity);
            }
        }
    }
    int principal_column = -1;
    int insert_pos = table->table_length;
    for(size_t i=0;i<table->col_count;i++){
        if(columns[i].it == BTREE_CLUSTERED || columns[i].it == SORTED_CLUSTERED){
            principal_column = i;
            insert_pos = search_key(values[i], columns[i].data, columns[i].size);
            break;
        }
    }
    
    for(size_t i=0;i<table->col_count;i++){
        //first insert value
        if(principal_column == -1){
            //no order
            columns[i].data[insert_pos] = values[i];
        }else{
            sorted_insert_val_vec(columns[i].data, columns[i].size, insert_pos, values[i]);
        }
        //next update index file
        if(columns[i].it != NONE){
            update_column_index(&columns[i], values[i], insert_pos, 0);
        }
        columns[i].size++;
    }
    table->table_length++;
    msg->status = OK_DONE;
}

void execute_insert_operator(DbOperator* query, message* msg){
    Table* table = query->operator_fields.insert_operator.table;
    int* values = query->operator_fields.insert_operator.values;
    execute_insert(table, values, msg);
}

int* execute_scan(void* val_payload, void* pos_payload, Comparator* comp, DataType dt, Result* res, size_t tuples_num, IndexType it, void* index_file){
    //cs165_log(stdout, "Entering scan\n");
    int* qualifying_index = (int*) malloc(tuples_num * sizeof(int));
    size_t index_count = 0;
    int* pos_vec = (int*) pos_payload;
    //TODO: add a query optimizer
    if(it != NONE && index_file != NULL && pos_vec == NULL && (comp->ct1 != NO_COMPARISON || comp->ct2 != NO_COMPARISON)){
        //we can use the index
        if(it == BTREE_CLUSTERED){
            int start = 0;
            int end = tuples_num;
            if(comp->ct1 != NO_COMPARISON){
                start = btree_find_pos_clustered((BTreeNode*) index_file, comp->lowerbound, 1);
            }
            if(comp->ct2 != NO_COMPARISON){
                end = btree_find_pos_clustered((BTreeNode*) index_file, comp->upperbound, 0);
            }
            if(start != -1 && end != -1){
                for(int i=start;i<end;i++){
                    qualifying_index[index_count] = i;
                    index_count++;
                }
            }else{
                cs165_log(stdout, "WARNING: btree clustered index start = -1 or end = -1 in execute_scan. \n");
                cs165_log(stdout, "WARNING: If there are actually no qualifying indexes, then this is the expected behavior. \n");
                cs165_log(stdout, "WARNING: Otherwise, expect error! \n");
            }
        }else if(it == BTREE_UNCLUSTERED){
            //we will realloc memory for qualifying index which might cause pointer change. Hence, we have to pass address of it.
            btree_find_pos_unclustered((BTreeNode*) index_file, comp, &qualifying_index, &index_count);
        }else if(it == SORTED_CLUSTERED){
            int* val_vec = (int*) val_payload;
            int start = 0;
            int end = tuples_num;
            if(comp->ct1 != NO_COMPARISON){
                start = search_key(comp->lowerbound, val_vec, tuples_num);
            }
            if(comp->ct2 != NO_COMPARISON){
                end = search_key(comp->upperbound, val_vec, tuples_num);
            }
            for(int i=start;i<end;i++){
                qualifying_index[index_count] = i;
                index_count++;
            }
        }else if(it == SORTED_UNCLUSTERED){
            ColumnIndex* ci = (ColumnIndex*) index_file;
            int* index_key_vec = ci->key_vec;
            int* index_pos_vec = ci->pos_vec;
            int start = 0;
            int end = tuples_num;
            if(comp->ct1 != NO_COMPARISON){
                start = search_key(comp->lowerbound, index_key_vec, tuples_num);
            }
            if(comp->ct2 != NO_COMPARISON){
                end = search_key(comp->upperbound, index_key_vec, tuples_num);
            }
            for(int i=start;i<end;i++){
                qualifying_index[index_count] = index_pos_vec[i];
                index_count++;
            }
        }
    }else{
        //do the scan
        if(dt == INT){
            int* val_vec = (int*) val_payload;
            if(pos_vec == NULL){
            //we are selecting directly from a column/result, no pos_vec
                for(size_t i=0;i<tuples_num;i++){
                    if((comp->ct1 == NO_COMPARISON || comp->lowerbound <= val_vec[i]) && (comp->ct2 == NO_COMPARISON || comp->upperbound > val_vec[i])){
                        qualifying_index[index_count] = i;
                        index_count++;
                    }
                }
            }else{
                for(size_t i=0;i<tuples_num;i++){
                    if((comp->ct1 == NO_COMPARISON || comp->lowerbound <= val_vec[i]) && (comp->ct2 == NO_COMPARISON || comp->upperbound > val_vec[i])){
                        qualifying_index[index_count] = pos_vec[i];
                        index_count++;
                    }
                }
            }
        }else if(dt == FLOAT){
            double* val_vec = (double*) val_payload;
            if(pos_vec == NULL){
            //we are selecting directly from a column/result, no pos_vec
                for(size_t i=0;i<tuples_num;i++){
                    if((comp->ct1 == NO_COMPARISON || comp->lowerbound <= val_vec[i]) && (comp->ct2 == NO_COMPARISON || comp->upperbound > val_vec[i])){
                        qualifying_index[index_count] = i;
                        index_count++;
                    }
                }
            }else{
                for(size_t i=0;i<tuples_num;i++){
                    if((comp->ct1 == NO_COMPARISON || comp->lowerbound <= val_vec[i]) && (comp->ct2 == NO_COMPARISON || comp->upperbound > val_vec[i])){
                        qualifying_index[index_count] = pos_vec[i];
                        index_count++;
                    }
                }
            }
        }else{
            long* val_vec = (long*) val_payload;
            if(pos_vec == NULL){
            //we are selecting directly from a column/result, no pos_vec
                for(size_t i=0;i<tuples_num;i++){
                    if((comp->ct1 == NO_COMPARISON || comp->lowerbound <= val_vec[i]) && (comp->ct2 == NO_COMPARISON || comp->upperbound > val_vec[i])){
                        qualifying_index[index_count] = i;
                        index_count++;
                    }
                }
            }else{
                for(size_t i=0;i<tuples_num;i++){
                    if((comp->ct1 == NO_COMPARISON || comp->lowerbound <= val_vec[i]) && (comp->ct2 == NO_COMPARISON || comp->upperbound > val_vec[i])){
                        qualifying_index[index_count] = pos_vec[i];
                        index_count++;
                    }
                }
            }
        }
    }
    qualifying_index = realloc(qualifying_index, sizeof(int)*index_count);
    res->num_tuples=index_count;
    cs165_log(stdout, "qualifying index count value %zd \n", index_count);
    //cs165_log(stdout, "Exiting scan\n");
    return qualifying_index;
}

//Usage1: <vec_pos>=select(<col_name>,<low>,<high>)
//Usage2: <vec_pos>=select(<posn_vec>,<val_vec>,<low>,<high>)
void execute_select_operator(DbOperator* query, message* msg){
    if(query->client_variables_num != 1){
        //we must have at least one handle
        msg->status = INCORRECT_FORMAT;
        cs165_log(stdout, "no client variable to store the result\n");
        return;
    }
    GCHandle* gch1 = query->operator_fields.select_operator.gch1;
    GCHandle* gch2 = query->operator_fields.select_operator.gch2;
    Comparator comp = query->operator_fields.select_operator.comparator;
    size_t tuples_num = 0;
    Result* res = malloc(sizeof(Result));
    int* qualifying_index = NULL;
    IndexType it = NONE;
    void* index_file = NULL;
    //cs165_log(stdout, "before case by case handle \n");
    if(gch2 != NULL){
        //we have pos_vec from gch1 and val_vec from gch2
        if(gch1->type != RESULT || gch1->p.result->data_type != INT){
            msg->status = QUERY_UNSUPPORTED;
            cs165_log(stdout, "query unsupported: column data cannot be used for pos_vec or result payload type is not int\n");
            return;
        }
        Result* pos_vec = gch1->p.result;
        tuples_num = pos_vec->num_tuples;
        qualifying_index = (int*) pos_vec->payload;
        if(gch2->type == RESULT){
            Result* val_vec = gch2->p.result;
            res->data_type = val_vec->data_type;
            res->payload = (void*) execute_scan((void *) val_vec->payload, (void*) qualifying_index, &comp, val_vec->data_type, res, tuples_num, it, index_file);
        }else{
            Column* val_vec = gch2->p.column;
            it = val_vec->it;
            index_file = val_vec->index_file;
            res->data_type = INT;
            res->payload = (void*) execute_scan((void *) val_vec->data, (void*) qualifying_index, &comp, INT, res, tuples_num, it, index_file);
        }
    }else{
        //we have only val_vec from gch1
        if(gch1->type == RESULT){
            Result* val_vec = gch1->p.result;
            res->data_type = val_vec->data_type;
            tuples_num = val_vec->num_tuples;
            res->payload = (void*) execute_scan((void *) val_vec->payload, (void*) qualifying_index, &comp, val_vec->data_type, res, tuples_num, it, index_file);
        }else{
            //cs165_log(stdout, "preprocess & type casting for scan\n");
            Column* val_vec = gch1->p.column;
            it = val_vec->it;
            index_file = val_vec->index_file;
            res->data_type = INT;
            tuples_num = val_vec->size;
            //cs165_log(stdout, "preprocess & type casting for scan completed\n");
            res->payload = (void*) execute_scan((void *) val_vec->data, (void*) qualifying_index, &comp, INT, res, tuples_num, it, index_file);
        }
    }
    
    GCHandle* gch_res = malloc(sizeof(GCHandle));
    strcpy(gch_res->name, query->client_variables[0]);
    gch_res->type = RESULT;
    gch_res->p.result = res;
    insert_context(query->context_table, gch_res->name, (void*) gch_res, GCOLUMN);
    cs165_log(stdout, "adding new context with variable name: %s\n", gch_res->name);
    msg->status = OK_DONE;
}
//Usage: <vec_val>=fetch(<col_var>,<vec_pos>)
void execute_fetch_operator(DbOperator* query, message* msg){
    if(query->client_variables_num != 1){
        cs165_log(stdout, "client variable num not matching \n");
        msg->status = INCORRECT_FORMAT;
        return;
    }
    GCHandle* gch1 = query->operator_fields.fetch_operator.gch1;
    GCHandle* gch2 = query->operator_fields.fetch_operator.gch2;
    if(gch2->type != RESULT || gch2->p.result->data_type != INT){
        cs165_log(stdout, "gch2 type and data type not matching \n");
        msg->status = QUERY_UNSUPPORTED;
        return;
    }
    Result* pos_vec = gch2->p.result;
    Result* res = malloc(sizeof(Result));
    if(gch1->type == RESULT){
        Result* val_vec = gch1->p.result;
        res->data_type = val_vec->data_type;
        if(pos_vec->num_tuples == 0){
            res->num_tuples = 0;
            res->payload = NULL;
        }else{
            int* qualifying_index = (int*) pos_vec->payload;
            if(val_vec->data_type == INT){
                int* val_payload = (int*) val_vec->payload;
                int* res_payload = malloc(pos_vec->num_tuples * sizeof(int));
                for(size_t i=0;i<pos_vec->num_tuples;i++){
                    res_payload[i] = val_payload[qualifying_index[i]];
                }
                res->payload = (void*) res_payload;
            }else if(val_vec->data_type == FLOAT){
                double* val_payload = (double*) val_vec->payload;
                double* res_payload = malloc(pos_vec->num_tuples * sizeof(double));
                for(size_t i=0;i<pos_vec->num_tuples;i++){
                    res_payload[i] = val_payload[qualifying_index[i]];
                }
                res->payload = (void*) res_payload;
            }else{
                long* val_payload = (long*) val_vec->payload;
                long* res_payload = malloc(pos_vec->num_tuples * sizeof(long));
                for(size_t i=0;i<pos_vec->num_tuples;i++){
                    res_payload[i] = val_payload[qualifying_index[i]];
                }
                res->payload = (void*) res_payload;
            }
            res->num_tuples = pos_vec->num_tuples;
        }
    }else{
        Column* val_vec = gch1->p.column;
        res->data_type = INT;
        if(pos_vec->num_tuples == 0){
            res->num_tuples = 0;
            res->payload = NULL;
        }else{
            int* qualifying_index = (int*) pos_vec->payload;
            int* val_payload = val_vec->data;
            int* res_payload = malloc(pos_vec->num_tuples * sizeof(int));
            for(size_t i=0;i<pos_vec->num_tuples;i++){
                res_payload[i] = val_payload[qualifying_index[i]];
            }
            res->num_tuples = pos_vec->num_tuples;
            res->payload = (void*) res_payload;
        }
    }
    
    GCHandle* gch_res = malloc(sizeof(GCHandle));
    strcpy(gch_res->name, query->client_variables[0]);
    gch_res->type = RESULT;
    gch_res->p.result = res;
    insert_context(query->context_table, gch_res->name, (void*) gch_res, GCOLUMN);
    cs165_log(stdout, "adding new context with variable name: %s\n", gch_res->name);
    msg->status = OK_DONE;
}

// Usage 1: <agg_val>=agg(<vec_val>)
// Usage 2: <agg_pos>, <agg_val>=agg(<vec_pos>,<vec_val>)
void execute_min_max_operator(DbOperator* query, message* msg){
    GCHandle* gch1 = query->operator_fields.aggregate_operator.gch1;
    GCHandle* gch2 = query->operator_fields.aggregate_operator.gch2;
    AggregateType t = query->operator_fields.aggregate_operator.type;
    if(query->client_variables_num == 0){
        msg->status = INCORRECT_FORMAT;
        return;
    }else if(query->client_variables_num == 1){
        //<agg_val>=agg(<vec_val>
        if(gch2 != NULL){
            msg->status = QUERY_UNSUPPORTED;
            return;
        }
        size_t val_tuples_num;
        Result* res = malloc(sizeof(Result));
        if(gch1->type == COLUMN){
            int* val_vec = gch1->p.column->data;
            val_tuples_num = gch1->p.column->size;
            if(val_tuples_num == 0){//no tuples to aggregate over
                res->num_tuples = 0;
                res->data_type = INT; //can be any type actually?
                res->payload = NULL;
            }else{
                res->num_tuples = 1;
                res->data_type = INT;
                int* payload = malloc(sizeof(int));
                *payload = val_vec[0];
                for(size_t i=1;i<val_tuples_num;i++){
                    //TODO: check if we have a better way to do this, too many ifs
                    //we might want to break this function into two after all.
                    if(t == MIN){
                        *payload = val_vec[i] < *payload ? val_vec[i] : *payload;
                    }else{
                        *payload = val_vec[i] > *payload ? val_vec[i] : *payload;
                    }
                }
                res->payload = (void*) payload;
            }
        }else{
            //we are dealing with result with data_type
            val_tuples_num = gch1->p.result->num_tuples;
            if(gch1->p.result->data_type == INT){
                int* val_vec = (int*) gch1->p.result->payload;
                if(val_tuples_num == 0){
                    res->num_tuples = 0;
                    res->data_type = gch1->p.result->data_type;
                    res->payload = NULL;
                }else{
                    res->num_tuples = 1;
                    res->data_type = gch1->p.result->data_type;
                    int* payload = malloc(sizeof(int));
                    *payload = val_vec[0];
                    for(size_t i=1;i<val_tuples_num;i++){
                        if(t == MIN){
                            *payload = val_vec[i] < *payload ? val_vec[i] : *payload;
                        }else{
                            *payload = val_vec[i] > *payload ? val_vec[i] : *payload;
                        }
                    }
                    res->payload = (void*) payload;
                }
            }else if(gch1->p.result->data_type == FLOAT){
                double* val_vec = (double*) gch1->p.result->payload;
                if(val_tuples_num == 0){
                    res->num_tuples = 0;
                    res->data_type = gch1->p.result->data_type;
                    res->payload = NULL;
                }else{
                    res->num_tuples = 1;
                    res->data_type = gch1->p.result->data_type;
                    double* payload = malloc(sizeof(double));
                    *payload = val_vec[0];
                    for(size_t i=1;i<val_tuples_num;i++){
                        if(t == MIN){
                            *payload = val_vec[i] < *payload ? val_vec[i] : *payload;
                        }else{
                            *payload = val_vec[i] > *payload ? val_vec[i] : *payload;
                        }
                    }
                    res->payload = (void*) payload;
                }
            }else{
                //dealing with long type
                long* val_vec = (long*) gch1->p.result->payload;
                if(val_tuples_num == 0){
                    res->num_tuples = 0;
                    res->data_type = gch1->p.result->data_type;
                    res->payload = NULL;
                }else{
                    res->num_tuples = 1;
                    res->data_type = gch1->p.result->data_type;
                    long* payload = malloc(sizeof(long));
                    *payload = val_vec[0];
                    for(size_t i=1;i<val_tuples_num;i++){
                        if(t == MIN){
                            *payload = val_vec[i] < *payload ? val_vec[i] : *payload;
                        }else{
                            *payload = val_vec[i] > *payload ? val_vec[i] : *payload;
                        }
                    }
                    res->payload = (void*) payload;
                }
            }
        }
        GCHandle* gch_res = malloc(sizeof(GCHandle));
        strcpy(gch_res->name, query->client_variables[0]);
        gch_res->type = RESULT;
        gch_res->p.result = res;
        insert_context(query->context_table, gch_res->name, (void*) gch_res, GCOLUMN);
        cs165_log(stdout, "adding new context with variable name: %s\n", gch_res->name);
        msg->status = OK_DONE;
    }else if(query->client_variables_num == 2){
        //<agg_pos>, <agg_val>=agg(<vec_pos>,<vec_val>
        if(gch1->type != RESULT || gch1->p.result->data_type != INT || gch2 == NULL){
            msg->status = QUERY_UNSUPPORTED;
            return;
        }
        int* pos_vec = (int*) gch1->p.result->payload;
        size_t pos_tuples_num = gch1->p.result->num_tuples;
        Result* res_pos = malloc(sizeof(Result));
        Result* res_val = malloc(sizeof(Result));
        if(gch2->type == COLUMN){
            int* val_vec = gch2->p.column->data;
            if(pos_tuples_num == 0){//no tuples to aggregate over
                res_pos->num_tuples = 0;
                res_pos->data_type = INT; //can be any type actually?
                res_pos->payload = NULL;
                res_val->num_tuples = 0;
                res_val->data_type = INT;
                res_val->payload = NULL;
            }else{
                res_pos->num_tuples = 1;
                res_pos->data_type = INT;
                int* payload_pos = malloc(sizeof(int));
                *payload_pos = pos_vec[0];
                res_val->num_tuples = 1;
                res_val->data_type = INT;
                int* payload_val = malloc(sizeof(int));
                *payload_val = val_vec[pos_vec[0]];
                for(size_t i=1;i<pos_tuples_num;i++){
                    //TODO: check if we have a better way to do this, too many ifs
                    //we might want to break this function into two after all.
                    if(t == MIN){
                        if(val_vec[pos_vec[i]] < *payload_val){
                            *payload_val = val_vec[pos_vec[i]];
                            *payload_pos = pos_vec[i];
                        }
                    }else{
                        if(val_vec[i] > *payload_val){
                            *payload_val = val_vec[pos_vec[i]];
                            *payload_pos = pos_vec[i];
                        }
                    }
                }
                res_pos->payload = (void*) payload_pos;
                res_val->payload = (void*) payload_val;
            }
        }else{
            //we are dealing with result with data_type
            if(gch2->p.result->data_type == INT){
                int* val_vec = (int*) gch2->p.result->payload;
                if(pos_tuples_num == 0){//no tuples to aggregate over
                    res_pos->num_tuples = 0;
                    res_pos->data_type = INT; //can be any type actually?
                    res_pos->payload = NULL;
                    res_val->num_tuples = 0;
                    res_val->data_type = INT;
                    res_val->payload = NULL;
                }else{
                    res_pos->num_tuples = 1;
                    res_pos->data_type = INT;
                    int* payload_pos = malloc(sizeof(int));
                    *payload_pos = pos_vec[0];
                    res_val->num_tuples = 1;
                    res_val->data_type = INT;
                    int* payload_val = malloc(sizeof(int));
                    *payload_val = val_vec[pos_vec[0]];
                    for(size_t i=1;i<pos_tuples_num;i++){
                        //TODO: check if we have a better way to do this, too many ifs
                        //we might want to break this function into two after all.
                        if(t == MIN){
                            if(val_vec[pos_vec[i]] < *payload_val){
                                *payload_val = val_vec[pos_vec[i]];
                                *payload_pos = pos_vec[i];
                            }
                        }else{
                            if(val_vec[i] > *payload_val){
                                *payload_val = val_vec[pos_vec[i]];
                                *payload_pos = pos_vec[i];
                            }
                        }
                    }
                    res_pos->payload = (void*) payload_pos;
                    res_val->payload = (void*) payload_val;
                }
            }else if(gch2->p.result->data_type == FLOAT){
                double* val_vec = (double*) gch2->p.result->payload;
                if(pos_tuples_num == 0){//no tuples to aggregate over
                    res_pos->num_tuples = 0;
                    res_pos->data_type = INT; //can be any type actually?
                    res_pos->payload = NULL;
                    res_val->num_tuples = 0;
                    res_val->data_type = FLOAT;
                    res_val->payload = NULL;
                }else{
                    res_pos->num_tuples = 1;
                    res_pos->data_type = INT;
                    int* payload_pos = malloc(sizeof(int));
                    *payload_pos = pos_vec[0];
                    res_val->num_tuples = 1;
                    res_val->data_type = FLOAT;
                    double* payload_val = malloc(sizeof(double));
                    *payload_val = val_vec[pos_vec[0]];
                    for(size_t i=1;i<pos_tuples_num;i++){
                        //TODO: check if we have a better way to do this, too many ifs
                        //we might want to break this function into two after all.
                        if(t == MIN){
                            if(val_vec[pos_vec[i]] < *payload_val){
                                *payload_val = val_vec[pos_vec[i]];
                                *payload_pos = pos_vec[i];
                            }
                        }else{
                            if(val_vec[i] > *payload_val){
                                *payload_val = val_vec[pos_vec[i]];
                                *payload_pos = pos_vec[i];
                            }
                        }
                    }
                    res_pos->payload = (void*) payload_pos;
                    res_val->payload = (void*) payload_val;
                }
            }else{
                //dealing with LONG type
                long* val_vec = (long*) gch2->p.result->payload;
                if(pos_tuples_num == 0){//no tuples to aggregate over
                    res_pos->num_tuples = 0;
                    res_pos->data_type = INT; //can be any type actually?
                    res_pos->payload = NULL;
                    res_val->num_tuples = 0;
                    res_val->data_type = LONG;
                    res_val->payload = NULL;
                }else{
                    res_pos->num_tuples = 1;
                    res_pos->data_type = INT;
                    int* payload_pos = malloc(sizeof(int));
                    *payload_pos = pos_vec[0];
                    res_val->num_tuples = 1;
                    res_val->data_type = FLOAT;
                    long* payload_val = malloc(sizeof(long));
                    *payload_val = val_vec[pos_vec[0]];
                    for(size_t i=1;i<pos_tuples_num;i++){
                        //TODO: check if we have a better way to do this, too many ifs
                        //we might want to break this function into two after all.
                        if(t == MIN){
                            if(val_vec[pos_vec[i]] < *payload_val){
                                *payload_val = val_vec[pos_vec[i]];
                                *payload_pos = pos_vec[i];
                            }
                        }else{
                            if(val_vec[i] > *payload_val){
                                *payload_val = val_vec[pos_vec[i]];
                                *payload_pos = pos_vec[i];
                            }
                        }
                    }
                    res_pos->payload = (void*) payload_pos;
                    res_val->payload = (void*) payload_val;
                }
            }
        }
        GCHandle* gch_res_pos = malloc(sizeof(GCHandle));
        strcpy(gch_res_pos->name, query->client_variables[0]);
        gch_res_pos->type = RESULT;
        gch_res_pos->p.result = res_pos;
        insert_context(query->context_table, gch_res_pos->name, (void*) gch_res_pos, GCOLUMN);
        GCHandle* gch_res_val = malloc(sizeof(GCHandle));
        strcpy(gch_res_val->name, query->client_variables[0]);
        gch_res_val->type = RESULT;
        gch_res_val->p.result = res_val;
        insert_context(query->context_table, gch_res_val->name, (void*) gch_res_val, GCOLUMN);
        msg->status = OK_DONE;
    }
}

// Usage: <agg_val>=agg(<vec_val>)
void execute_sum_avg_operator(DbOperator* query, message* msg){
    GCHandle* gch1 = query->operator_fields.aggregate_operator.gch1;
    AggregateType t = query->operator_fields.aggregate_operator.type;
    if(query->client_variables_num != 1){
        msg->status = INCORRECT_FORMAT;
        return;
    }
    size_t tuples_num;
    Result* res = malloc(sizeof(Result));
    if(gch1->type == COLUMN){
        tuples_num = gch1->p.column->size;
        int* val_vec = gch1->p.column->data;
        if(tuples_num == 0){
            res->num_tuples = 1;
            if(t == AVG){
                res->data_type = FLOAT;
                double* res_payload = malloc(sizeof(int));
                *res_payload = 0;
                res->payload = res_payload;
            }else{
                res->data_type = LONG; //can be set to other data type as well?
                long* res_payload = malloc(sizeof(long));
                *res_payload = 0;
                res->payload = res_payload;
            }
        }else{
            res->num_tuples = 1;
            long s = 0;
            for(size_t i=0;i<tuples_num;i++){
                s += val_vec[i];
            }
            if(t == AVG){
                res->data_type = FLOAT;
                double* res_payload = malloc(sizeof(double));
                *res_payload = (double) s / (double) tuples_num;
                res->payload = (void*) res_payload;
            }else{
                res->data_type = LONG;
                long* res_payload = malloc(sizeof(long));
                *res_payload = s;
                res->payload = (void*) res_payload;
            }
        }
    }else{
        //we are dealing with results with different data type
        tuples_num = gch1->p.result->num_tuples;
        if(gch1->p.result->data_type == INT){
            int* val_vec = (int*) gch1->p.result->payload;
            if(tuples_num == 0){
                res->num_tuples = 1;
                if(t == AVG){
                    res->data_type = FLOAT;
                    double* res_payload = malloc(sizeof(int));
                    *res_payload = 0;
                    res->payload = res_payload;
                }else{
                    res->data_type = LONG; //can be set to other data type as well?
                    long* res_payload = malloc(sizeof(long));
                    *res_payload = 0;
                    res->payload = res_payload;
                }
            }else{
                res->num_tuples = 1;
                long s = 0;
                for(size_t i=0;i<tuples_num;i++){
                    s += val_vec[i];
                }
                cs165_log(stdout, "finish aggregate sum loop\n");
                if(t == AVG){
                    res->data_type = FLOAT;
                    double* res_payload = malloc(sizeof(double));
                    *res_payload = (double) s / (double) tuples_num;
                    res->payload = (void*) res_payload;
                }else{
                    res->data_type = LONG;
                    long* res_payload = malloc(sizeof(long));
                    *res_payload = s;
                    res->payload = (void*) res_payload;
                }
            }
        }else if(gch1->p.result->data_type == FLOAT){
            double* val_vec = (double*) gch1->p.result->payload;
            if(tuples_num == 0){
                res->num_tuples = 1;
                if(t == AVG){
                    res->data_type = FLOAT;
                    double* res_payload = malloc(sizeof(int));
                    *res_payload = 0;
                    res->payload = res_payload;
                }else{
                    res->data_type = FLOAT; //can be set to other data type as well?
                    double* res_payload = malloc(sizeof(double));
                    *res_payload = 0;
                    res->payload = res_payload;
                }
            }else{
                res->num_tuples = 1;
                double s = 0;
                for(size_t i=0;i<tuples_num;i++){
                    s += val_vec[i];
                }
                if(t == AVG){
                    res->data_type = FLOAT;
                    double* res_payload = malloc(sizeof(double));
                    *res_payload = (double) s / (double) tuples_num;
                    res->payload = (void*) res_payload;
                }else{
                    res->data_type = FLOAT;
                    double* res_payload = malloc(sizeof(double));
                    *res_payload = s;
                    res->payload = (void*) res_payload;
                }
            }
        }else{
            //dealing with long type
            long* val_vec = (long*) gch1->p.result->payload;
            if(tuples_num == 0){
                res->num_tuples = 1;
                if(t == AVG){
                    res->data_type = FLOAT;
                    double* res_payload = malloc(sizeof(int));
                    *res_payload = 0;
                    res->payload = res_payload;
                }else{
                    res->data_type = LONG; //can be set to other data type as well?
                    long* res_payload = malloc(sizeof(long));
                    *res_payload = 0;
                    res->payload = res_payload;
                }
            }else{
                res->num_tuples = 1;
                long s = 0;
                for(size_t i=0;i<tuples_num;i++){
                    s += val_vec[i];
                }
                if(t == AVG){
                    res->data_type = FLOAT;
                    double* res_payload = malloc(sizeof(double));
                    *res_payload = (double) s / (double) tuples_num;
                    res->payload = (void*) res_payload;
                }else{
                    res->data_type = LONG;
                    long* res_payload = malloc(sizeof(long));
                    *res_payload = s;
                    res->payload = (void*) res_payload;
                }
            }
        }
    }
    GCHandle* gch_res = malloc(sizeof(GCHandle));
    strcpy(gch_res->name, query->client_variables[0]);
    gch_res->type = RESULT;
    gch_res->p.result = res;
    insert_context(query->context_table, gch_res->name, (void*) gch_res, GCOLUMN);
    cs165_log(stdout, "adding new context with variable name: %s\n", gch_res->name);
    msg->status = OK_DONE;
}

//Usage: <vec_val>=agg(<vec_val1>,<vec_val2>)
void execute_add_sub_operator(DbOperator* query, message* msg){
    GCHandle* gch1 = query->operator_fields.aggregate_operator.gch1;
    GCHandle* gch2 = query->operator_fields.aggregate_operator.gch2;
    AggregateType t = query->operator_fields.aggregate_operator.type;
    if(query->client_variables_num != 1){
        msg->status = INCORRECT_FORMAT;
        return;
    }
    size_t tuples_num1=0;
    size_t tuples_num2=0;
    size_t res_tuples_num=0;
    DataType dt1=INT;
    DataType dt2=INT;
    DataType res_dt=INT;
    if(gch1->type == COLUMN){
        tuples_num1 = gch1->p.column->size;
        dt1 = INT;
    }else{
        tuples_num1 = gch1->p.result->num_tuples;
        dt1 = gch1->p.result->data_type;
    }
    cs165_log(stdout, "tuple num from first column: %zd\n", tuples_num1);
    if(gch2->type == COLUMN){
        tuples_num2 = gch2->p.column->size;
        dt2 = INT;
    }else{
        tuples_num2 = gch2->p.result->num_tuples;
        dt2 = gch2->p.result->data_type;
    }
    cs165_log(stdout, "tuple num from second column: %zd\n", tuples_num2);
    if(tuples_num1 != tuples_num2){
        msg->status = INCORRECT_FORMAT;
        cs165_log(stdout, "tuples num not matched! \n");
        return;
    }
    res_tuples_num = tuples_num1;
    if(dt1 == LONG || dt2 == LONG){
        res_dt = LONG;
    }
    if(dt1 == FLOAT || dt2 == FLOAT){
        res_dt = FLOAT;
    }
    Result* res = malloc(sizeof(Result));
    res->data_type = res_dt;
    res->num_tuples = res_tuples_num;
    if(gch1->type == COLUMN){
        int* val_vec1 = gch1->p.column->data;
        if(gch2->type == COLUMN){
            int* val_vec2 = gch2->p.column->data;
            int* payload = malloc(res_tuples_num * sizeof(int));
            for(size_t i=0;i<res_tuples_num;i++){
                if(t == ADD){
                    payload[i] = val_vec1[i] + val_vec2[i];
                }else{
                    payload[i] = val_vec1[i] - val_vec2[i];
                }
            }
            res->payload = (void*) payload;
            res->data_type = INT;
        }else{
            //we are dealing with results with different data types for gch2
            if(gch2->p.result->data_type == INT){
                int* val_vec2 = (int*) gch2->p.result->payload;
                int* payload = malloc(res_tuples_num * sizeof(int));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = val_vec1[i] + val_vec2[i];
                    }else{
                        payload[i] = val_vec1[i] - val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = INT;
            }else if(gch2->p.result->data_type == FLOAT){
                double* val_vec2 = (double*) gch2->p.result->payload;
                double* payload = malloc(res_tuples_num * sizeof(double));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = (double) val_vec1[i] + val_vec2[i];
                    }else{
                        payload[i] = (double) val_vec1[i] - val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = FLOAT;
            }else{
                //dealing with long
                long* val_vec2 = (long*) gch2->p.result->payload;
                long* payload = malloc(res_tuples_num * sizeof(long));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = (long) val_vec1[i] + val_vec2[i];
                    }else{
                        payload[i] = (long) val_vec1[i] - val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = LONG;
            }
        }
    }else{
        //we are dealing with results with different data types for gch1
        if(gch1->p.result->data_type == INT){
            int* val_vec1 = (int*) gch1->p.result->payload;
            if(gch2->p.result->data_type == INT){
                int* val_vec2 = (int*) gch2->p.result->payload;
                int* payload = malloc(res_tuples_num * sizeof(int));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = val_vec1[i] + val_vec2[i];
                    }else{
                        payload[i] = val_vec1[i] - val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = INT;
            }else if(gch2->p.result->data_type == FLOAT){
                double* val_vec2 = (double*) gch2->p.result->payload;
                double* payload = malloc(res_tuples_num * sizeof(double));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = (double) val_vec1[i] + val_vec2[i];
                    }else{
                        payload[i] = (double) val_vec1[i] - val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = FLOAT;
            }else{
                //dealing with long
                long* val_vec2 = (long*) gch2->p.result->payload;
                long* payload = malloc(res_tuples_num * sizeof(long));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = (long) val_vec1[i] + val_vec2[i];
                    }else{
                        payload[i] = (long) val_vec1[i] - val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = LONG;
            }
        }else if(gch1->p.result->data_type == FLOAT){
            double* val_vec1 = (double *) gch1->p.result->payload;
            if(gch2->p.result->data_type == INT){
                int* val_vec2 = (int*) gch2->p.result->payload;
                double* payload = malloc(res_tuples_num * sizeof(double));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = val_vec1[i] + (double) val_vec2[i];
                    }else{
                        payload[i] = val_vec1[i] - (double) val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = FLOAT;
            }else if(gch2->p.result->data_type == FLOAT){
                double* val_vec2 = (double*) gch2->p.result->payload;
                double* payload = malloc(res_tuples_num * sizeof(double));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = val_vec1[i] + val_vec2[i];
                    }else{
                        payload[i] = val_vec1[i] - val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = FLOAT;
            }else{
                //dealing with long
                long* val_vec2 = (long*) gch2->p.result->payload;
                double* payload = malloc(res_tuples_num * sizeof(double));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = val_vec1[i] + (double) val_vec2[i];
                    }else{
                        payload[i] = val_vec1[i] - (double) val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = FLOAT;
            }
        }else{
            //we are dealting with long payload from gch1
            long* val_vec1 = (long *) gch1->p.result->payload;
            if(gch2->p.result->data_type == INT){
                int* val_vec2 = (int*) gch2->p.result->payload;
                long* payload = malloc(res_tuples_num * sizeof(long));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = val_vec1[i] + (long) val_vec2[i];
                    }else{
                        payload[i] = val_vec1[i] - (long) val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = LONG;
            }else if(gch2->p.result->data_type == FLOAT){
                double* val_vec2 = (double*) gch2->p.result->payload;
                double* payload = malloc(res_tuples_num * sizeof(double));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = (double) val_vec1[i] + val_vec2[i];
                    }else{
                        payload[i] = (double) val_vec1[i] - val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = FLOAT;
            }else{
                //dealing with long
                long* val_vec2 = (long*) gch2->p.result->payload;
                long* payload = malloc(res_tuples_num * sizeof(long));
                for(size_t i=0;i<res_tuples_num;i++){
                    if(t == ADD){
                        payload[i] = val_vec1[i] + val_vec2[i];
                    }else{
                        payload[i] = val_vec1[i] - val_vec2[i];
                    }
                }
                res->payload = (void*) payload;
                res->data_type = LONG;
            }
        }
    }
    GCHandle* gch_res = malloc(sizeof(GCHandle));
    strcpy(gch_res->name, query->client_variables[0]);
    gch_res->type = RESULT;
    gch_res->p.result = res;
    insert_context(query->context_table, gch_res->name, (void*) gch_res, GCOLUMN);
    cs165_log(stdout, "adding new context with variable name: %s\n", gch_res->name);
    msg->status = OK_DONE;
}

void execute_aggregate_operator(DbOperator* query, message* msg){
    AggregateType t = query->operator_fields.aggregate_operator.type;
    if(t==MIN || t==MAX){
        execute_min_max_operator(query, msg);
    }else if(t==SUM || t==AVG){
        execute_sum_avg_operator(query, msg);
    }else if(t==ADD || t==SUB){
        execute_add_sub_operator(query, msg);
    }else{
        msg->status = QUERY_UNSUPPORTED;
    }
}

void execute_print_operator(DbOperator* query, message* msg){
    GCHandle** gch_list = query->operator_fields.print_operator.gch_list;
    size_t gch_count = query->operator_fields.print_operator.gch_count;
    //TODO: let parser check that all gch contain same number of tuples
    size_t tuples_num = 0;
    if(gch_list[0]->type == RESULT){
        tuples_num = gch_list[0]->p.result->num_tuples;
    }else{
        tuples_num = gch_list[0]->p.column->size;
    }
    cs165_log(stdout, "total number of tuples: %zd\n", tuples_num);
    //send meta data: total tuples number to client first
    send(query->client_fd, &tuples_num, sizeof(size_t), 0);
    cs165_log(stdout, "total tuples number sent to client\n");
    GCHandle* gch;
    GCType t;
    DataType dt;
    for(size_t j=0;j<gch_count;j++){
        gch = gch_list[j];
        t = gch->type;
        if(t == COLUMN){
            dt = INT;
            //send meta data: data type for this column
            send(query->client_fd, &dt, sizeof(DataType), 0);
            int* data = gch->p.column->data;
            for(size_t i=0;i<tuples_num;i++){
                send(query->client_fd, &(data[i]), sizeof(int), 0);
            }
        }else{
            //we are dealing with results with different data type
            dt = gch->p.result->data_type;
            send(query->client_fd, &dt, sizeof(DataType), 0);
            cs165_log(stdout, "data type for vector %zd sent to client\n", j);
            if(dt == INT){
                int* payload = (int*) gch->p.result->payload;
                for(size_t i=0;i<tuples_num;i++){
                    send(query->client_fd, &(payload[i]), sizeof(int), 0);
                    //cs165_log(stdout, "value %d sent to client\n", payload[i]);
                }
            }else if(dt == FLOAT){
                double* payload = (double*) gch->p.result->payload;
                for(size_t i=0;i<tuples_num;i++){
                    send(query->client_fd, &(payload[i]), sizeof(double), 0);
                }
            }else{
                long* payload = (long*) gch->p.result->payload;
                for(size_t i=0;i<tuples_num;i++){
                    send(query->client_fd, &(payload[i]), sizeof(long), 0);
                }
            }
        }
    }
    msg->status = OK_DONE;
}

int IndexPairCompare(const void *ip1p, const void *ip2p){
    IndexPair* ip1 = (IndexPair*) ip1p;
    IndexPair* ip2 = (IndexPair*) ip2p;
    return (ip1->key - ip2->key);
}

/*TODO: if we have indexes, the following load might break
 if there are data in current_db before we load. Check this if we have time later*/
void execute_load_operator(DbOperator* query, message* msg){
    Table* table = query->operator_fields.load_operator.table;
    size_t col_count = table->col_count;
    Column* columns = table->columns;
    size_t tuples_num = 0;
    
    //check if we have sorted column frist
    int principal_column = -1;
    for(size_t i=0;i<col_count;i++){
        if(columns[i].it == BTREE_CLUSTERED || columns[i].it == SORTED_CLUSTERED){
            principal_column = i;
            break;
        }
    }
    //receive tuples meta data from the client
    recv(query->client_fd, &tuples_num, sizeof(size_t), 0);
    //check if we have reserved enough space in this table for all tuples to be inserted
    size_t table_length_capacity = table->table_length_capacity;
    while((table->table_length + tuples_num) > table_length_capacity){
        table_length_capacity = table_length_capacity * 2;
    }
    if(table_length_capacity != table->table_length_capacity){
        //realloc memory for this table
        for(size_t i=0;i<col_count;i++){
            columns[i].data = realloc(columns[i].data, table_length_capacity * sizeof(int));
            if(columns[i].it == SORTED_UNCLUSTERED){
                ColumnIndex* index_file = (ColumnIndex*) columns[i].index_file;
                index_file->key_vec = realloc(index_file->key_vec, table_length_capacity * sizeof(int));
                index_file->pos_vec = realloc(index_file->pos_vec, table_length_capacity * sizeof(int));
            }
        }
    }
    int** tuples=malloc(col_count * sizeof(int*));
    int* tuple=malloc(col_count * sizeof(int));
    for(size_t j=0;j<col_count;j++){
        tuples[j]=malloc(tuples_num * sizeof(int));
    }
    
    for(size_t i=0;i<tuples_num;i++){
        recv(query->client_fd, tuple, col_count * sizeof(int), 0);
        for(size_t j=0;j<col_count;j++){
            tuples[j][i] = tuple[j];
        }
    }
    
    IndexPair* ip_vector=malloc(tuples_num * sizeof(IndexPair));
    if(principal_column != -1){
        //sort according to the principal copy
        for(size_t i=0;i<tuples_num;i++){
            ip_vector[i].key = tuples[principal_column][i];
            ip_vector[i].pos = i;
        }
        qsort(ip_vector, tuples_num, sizeof(IndexPair), IndexPairCompare);
        for(size_t i=0;i<tuples_num;i++){ //rearange the data
            tuples[principal_column][ip_vector[i].pos] = ip_vector[i].key;
        }
        for(size_t i=0;i<tuples_num;i++){
            columns[principal_column].data[i] = tuples[principal_column][i];
        }
        //propogate the order of principal copy
        for(int j=0;j< (int)col_count;j++){
            for(size_t i=0;i<tuples_num;i++){
                if(j!=principal_column){
                    columns[j].data[i] = tuples[j][ip_vector[i].pos];
                }
            }
        }
    }
    
    //do the insert
    for(size_t j=0;j<col_count;j++){
        if(principal_column == -1){
            for(size_t i=0;i<tuples_num;i++){
                columns[j].data[i] = tuples[j][i];
            }
        }
        columns[j].size += tuples_num;
        if(columns[j].it == SORTED_UNCLUSTERED){
            //generate additional copy of data for sorted unclustered index
            for(size_t i=0;i<tuples_num;i++){
                ip_vector[i].key = columns[i].data[i];
                ip_vector[i].pos = i;
            }
            qsort(ip_vector, tuples_num, sizeof(IndexPair), IndexPairCompare);
            ColumnIndex* ci;
            for(size_t i=0;i<tuples_num;i++){
                ci = (ColumnIndex*) columns[j].index_file;
                ci->key_vec[i] = ip_vector[i].key;
                ci->pos_vec[i] = ip_vector[i].pos;
            }
        }else if(columns[j].it == BTREE_CLUSTERED || columns[j].it == BTREE_UNCLUSTERED){
            for(size_t i=0;i<tuples_num;i++){
                update_column_index(&(columns[j]), tuples[j][i], i, 1); //no need to shift pos_vec since data have been sorted already
            }
        }
        free(tuples[j]);
    }
    free(tuples);
    free(tuple);
    free(ip_vector);
    table->table_length += tuples_num;
    
    msg->status = OK_DONE;
}

//work with int type only for now
//work with column literal only for now
void execute_shared_scan(void* val_payload, DataType dt, size_t tuples_num){
    (void) dt;
    //TODO:Support Other data type
    int* data = (int*) val_payload;
    int** payloads = malloc(batch_size * sizeof(int*));
    size_t* results_tuples_num = malloc(batch_size * sizeof(size_t));
    //assume worst case scenario for payloads memory usage
    for(size_t j=0;j<batch_size;j++){
        payloads[j] = malloc(tuples_num * sizeof(int));
        results_tuples_num[j] = 0;
    }
    int target;
    for(size_t i=0;i<tuples_num;i++){
        target = data[i];
        for(size_t j=0;j<batch_size;j++){
            payloads[j][results_tuples_num[j]] = i;
            results_tuples_num[j] += (batched_queries[j]->operator_fields.select_operator.comparator.ct1 == NO_COMPARISON || batched_queries[j]->operator_fields.select_operator.comparator.lowerbound <= target) && (batched_queries[j]->operator_fields.select_operator.comparator.ct2 == NO_COMPARISON || batched_queries[j]->operator_fields.select_operator.comparator.upperbound > target);
        }
    }
    //realloc space for payloads
    for(size_t j=0;j<batch_size;j++){
        payloads[j]=realloc(payloads[j], results_tuples_num[j] * sizeof(int));
        batched_results[j]->num_tuples = results_tuples_num[j];
        batched_results[j]->payload = payloads[j];
    }
    
    free(results_tuples_num);
    free(payloads);
    return;
}

void * threaded_execute_shared_scan(void * thread_args){
    //TODO:Support Other data type using args->dt
    ThreadedScanArgs* args = (ThreadedScanArgs*) thread_args;
    size_t thread_id = args->thread_id;
    int* data = (int*) args->data;
    size_t start = args->start;
    size_t end = args->end;
    DataType dt = args->dt;
    (void) dt;
    int** unit_batched_results = malloc(batch_size * sizeof(int*));
    int* unit_batched_results_count = malloc(batch_size * sizeof(int));
    size_t tuples_num = end+1-start;
    //assume worst case scenario for payloads memory usage
    for(size_t j=0;j<batch_size;j++){
        unit_batched_results[j] = malloc(tuples_num * sizeof(int));
        unit_batched_results_count[j] = 0;
    }
    int target;
    for(size_t i=start;i<=end;i++){
        target = data[i];
        for(size_t j=0;j<batch_size;j++){
            unit_batched_results[i][unit_batched_results_count[j]] = i;
            unit_batched_results_count[j] += (batched_queries[j]->operator_fields.select_operator.comparator.ct1 == NO_COMPARISON || batched_queries[j]->operator_fields.select_operator.comparator.lowerbound <= target) && (batched_queries[j]->operator_fields.select_operator.comparator.ct2 == NO_COMPARISON || batched_queries[j]->operator_fields.select_operator.comparator.upperbound > target);
        }
    }
    
    //realloc space for payloads
    for(size_t j=0;j<batch_size;j++){
        unit_batched_results[j]=realloc(unit_batched_results[j], unit_batched_results_count[j] * sizeof(int));
    }
    threaded_batched_results[thread_id]=unit_batched_results;
    threaded_batched_results_count[thread_id]=unit_batched_results_count;
    return NULL;
}

//Assume that all batched queries access the same GCHandle at the moment
void execute_batched_select_operators(message* msg){
    GCHandle* gch;
    batched_results = malloc(batch_size * sizeof(Result*));
    gch=batched_queries[0]->operator_fields.select_operator.gch1;
    DataType dt;
    size_t tuples_num;
    //TODO:support results handle
    if(gch->type==COLUMN){
        dt=INT;
        int* data = gch->p.column->data;
        tuples_num = gch->p.column->size;
        execute_shared_scan((void*) data, dt, tuples_num);
        for(size_t i=0;i<batch_size;i++){
            gch = malloc(1*sizeof(GCHandle));
            strcpy(gch->name, batched_queries[i]->client_variables[0]);
            gch->type = RESULT;
            gch->p.result = batched_results[i];
            insert_context(batched_queries[i]->context_table, gch->name, (void*) gch, GCOLUMN);
        }
        msg->status = OK_DONE;
    }
    batch_mode=0;
    batch_size=0;
    free_batched_queries();
    free(batched_results);
    return;
}

void execute_batched_select_operators_threaded(message* msg){
    GCHandle* gch;
    
    threaded_batched_results = malloc(THREAD_NUM * sizeof(int**));
    threaded_batched_results_count = malloc(THREAD_NUM * sizeof(int*));
    
    gch=batched_queries[0]->operator_fields.select_operator.gch1;
    DataType dt;
    size_t tuples_num, threaded_tuples_num, start, end;
    int* payload_temp;
    //TODO:support results handle
    if(gch->type==COLUMN){
        dt=INT;
        int* data = gch->p.column->data;
        tuples_num = gch->p.column->size;
        threaded_tuples_num = tuples_num/THREAD_NUM;
        pthread_t* threads = malloc(THREAD_NUM * sizeof(pthread_t));
        ThreadedScanArgs** threads_args = malloc(THREAD_NUM * sizeof(ThreadedScanArgs *));
        start=0;
        for(size_t thread_id=0;thread_id<THREAD_NUM;thread_id++){
            end = start + threaded_tuples_num - 1 + ((tuples_num % THREAD_NUM) - thread_id > 0);
            threads_args[thread_id] = malloc(1 * sizeof(ThreadedScanArgs));
            threads_args[thread_id]->thread_id = thread_id;
            threads_args[thread_id]->data = (void *) data;
            threads_args[thread_id]->start = start;
            threads_args[thread_id]->end = end;
            threads_args[thread_id]->dt = dt;
            pthread_create(&threads[thread_id], NULL, threaded_execute_shared_scan, threads_args[thread_id]);
            start = end + 1;
        }
        for(size_t thread_id=0;thread_id<THREAD_NUM;thread_id++){
            pthread_join(threads[thread_id], NULL);
            free(threads_args[thread_id]);
        }
        free(threads);
        free(threads_args);
        
        //reduce
        batched_results = malloc(batch_size * sizeof(Result*));
        for(size_t j=0;j<batch_size;j++){
            batched_results[j] = malloc(1 * sizeof(Result));
            batched_results[j]->num_tuples = 0;
            for(size_t thread_id=0;thread_id<THREAD_NUM;thread_id++){
                batched_results[j]->num_tuples += threaded_batched_results_count[thread_id][j];
            }
            batched_results[j]->payload = malloc(batched_results[j]->num_tuples * sizeof(int));
            start = 0;
            for(size_t thread_id=0;thread_id<THREAD_NUM;thread_id++){
                for(int i=0;i<threaded_batched_results_count[thread_id][j];i++){
                    payload_temp = (int*) batched_results[j]->payload;
                    payload_temp[start+i] = threaded_batched_results[thread_id][j][i];
                }
                start+=threaded_batched_results_count[thread_id][j];
                free(threaded_batched_results[thread_id][j]);
            }
            batched_results[j]->data_type = INT;
        }
        for(size_t thread_id=0;thread_id<THREAD_NUM;thread_id++){
            free(threaded_batched_results[thread_id]);
            free(threaded_batched_results_count[thread_id]);
        }
        free(threaded_batched_results);
        free(threaded_batched_results_count);
        
        for(size_t j=0;j<batch_size;j++){
            gch = malloc(1*sizeof(GCHandle));
            strcpy(gch->name, batched_queries[j]->client_variables[0]);
            gch->type = RESULT;
            gch->p.result = batched_results[j];
            insert_context(batched_queries[j]->context_table, gch->name, (void*) gch, GCOLUMN);
        }
        msg->status = OK_DONE;
    }
    batch_mode=0;
    batch_size=0;
    free_batched_queries();
    free(batched_results);
    return;
}

void execute_batched_queries(message* msg){
    if(batch_size==0){
        return;
    }
    clock_t begin, end;
    double time_elapsed = 0;
    if(MULTI_THREADING){
        begin = clock();
        execute_batched_select_operators_threaded(msg);
        end = clock();
        cs165_log(stdout, "Multithreaded shared scan takes time: %f \n", time_elapsed);
    }else{
        begin = clock();
        execute_batched_select_operators(msg);
        end = clock();
        cs165_log(stdout, "Single thread shared scan takes time: %f \n", time_elapsed);
        //TODO: for comparison purpose, add a mode to execute batched select queries one by one
    }
}

void execute_nested_loop_join(void* outer_val_vec_p, int* outer_pos_vec, size_t outer_tuples_num,
                              void* inner_val_vec_p, int* inner_pos_vec, size_t inner_tuples_num,
                              int** res_outer_pos_vec_p, int** res_inner_pos_vec_p,  size_t* res_tuples_num_p,
                              DataType dt){
    int* res_outer_pos_vec = *res_outer_pos_vec_p;
    int* res_inner_pos_vec = *res_inner_pos_vec_p;
    size_t res_tuples_num = 0;
    size_t res_capacity = PAGE_SIZE;
    if(dt == INT){
        int* outer_val_vec = (int*) outer_val_vec_p;
        int* inner_val_vec = (int*) inner_val_vec_p;
        size_t outer_p = PAGE_SIZE/sizeof(int);
        size_t inner_p = PAGE_SIZE/sizeof(int);
        for(size_t block_i=0;block_i<outer_tuples_num;block_i=block_i+outer_p){
            for(size_t block_j=0;block_j<inner_tuples_num;block_j=block_j+inner_p){
                for(size_t i=block_i;i<block_i+outer_p && i<outer_tuples_num;i++){
                    for(size_t j=block_j;j<block_j+inner_p && j<inner_tuples_num;j++){
                        if(outer_val_vec[i]==inner_val_vec[j]){
                            if(res_tuples_num == res_capacity){
                                res_capacity *= 2;
                                res_outer_pos_vec = realloc(res_outer_pos_vec, res_capacity * sizeof(int));
                                res_inner_pos_vec = realloc(res_inner_pos_vec, res_capacity * sizeof(int));
                            }
                            res_outer_pos_vec[res_tuples_num] = outer_pos_vec[i];
                            res_inner_pos_vec[res_tuples_num] = inner_pos_vec[j];
                            res_tuples_num++;
                        }
                    }
                }
            }
        }
        res_outer_pos_vec = realloc(res_outer_pos_vec, res_tuples_num * sizeof(int));
        res_inner_pos_vec = realloc(res_inner_pos_vec, res_tuples_num * sizeof(int));
        *res_inner_pos_vec_p = res_inner_pos_vec;
        *res_outer_pos_vec_p = res_outer_pos_vec;
        *res_tuples_num_p = res_tuples_num;
    }else if(dt == FLOAT){
        //TODO: support double
    }else{
        //TODO: support long
    }
    
}

void execute_hash_join(void* smaller_val_vec_p, int* smaller_pos_vec, size_t smaller_tuples_num,
                       void* larger_val_vec_p, int* larger_pos_vec, size_t larger_tuples_num,
                       int** res_smaller_pos_vec_p, int** res_larger_pos_vec_p,  size_t* res_tuples_num_p,
                       DataType dt){
    int* res_smaller_pos_vec = *res_smaller_pos_vec_p;
    int* res_larger_pos_vec = *res_larger_pos_vec_p;
    size_t res_tuples_num = 0;
    size_t res_capacity = PAGE_SIZE;
    if(dt == INT){
        int* smaller_val_vec = (int*) smaller_val_vec_p;
        int* larger_val_vec = (int*) larger_val_vec_p;
        ExtHashTable* ht = hashtable_create();
        for(size_t i=0;i<smaller_tuples_num;i++){
            hashtable_insert(ht, smaller_val_vec[i], smaller_pos_vec[i]);
        }
        int* temp_prob_res = malloc(smaller_tuples_num * sizeof(int));
        size_t temp_prob_res_num = 0;
        for(size_t i=0;i<larger_tuples_num;i++){
            hashtable_probe(ht, larger_val_vec[i], temp_prob_res, &temp_prob_res_num);
            for(size_t j=0;j<temp_prob_res_num;j++){
                if(res_tuples_num == res_capacity){
                    res_capacity *= 2;
                    res_smaller_pos_vec = realloc(res_smaller_pos_vec, res_capacity * sizeof(int));
                    res_larger_pos_vec = realloc(res_larger_pos_vec, res_capacity * sizeof(int));
                }
                res_smaller_pos_vec[res_tuples_num] = temp_prob_res[j];
                res_larger_pos_vec[res_tuples_num] = larger_pos_vec[i];
                res_tuples_num++;
            }
        }
        free(temp_prob_res);
        hashtable_free(ht);
        res_smaller_pos_vec = realloc(res_smaller_pos_vec, res_tuples_num * sizeof(int));
        res_larger_pos_vec = realloc(res_larger_pos_vec, res_tuples_num * sizeof(int));
        *res_smaller_pos_vec_p = res_smaller_pos_vec;
        *res_larger_pos_vec_p = res_larger_pos_vec;
        *res_tuples_num_p = res_tuples_num;
    }else if(dt == FLOAT){
        //TODO: support double
    }else{
        //TODO: support long
    }
}

void execute_join_operator(DbOperator* query, message* msg){
    //assume all data cannot fit into the cache
    //in such case the smaller one should be used for the outer loop
    //the larger one should be used for the inner loop
    JoinType jt = query->operator_fields.join_operator.jt;
    Result* gch_val_vec1 = query->operator_fields.join_operator.val_vec1;
    Result* gch_pos_vec1 = query->operator_fields.join_operator.pos_vec1;
    Result* gch_val_vec2 = query->operator_fields.join_operator.val_vec2;
    Result* gch_pos_vec2 = query->operator_fields.join_operator.pos_vec2;
    int* pos_vec1 = (int*) gch_pos_vec1->payload;
    size_t tuples_num1 = gch_pos_vec1->num_tuples;
    int* pos_vec2 = (int*) gch_pos_vec2->payload;
    size_t tuples_num2 = gch_pos_vec2->num_tuples;
    
    int* outer_pos_vec = NULL;
    int* inner_pos_vec = NULL;
    size_t outer_tuples_num = 0;
    size_t inner_tuples_num = 0;
    
    Result* res_outer = malloc(1 * sizeof(Result));
    Result* res_inner = malloc(1 * sizeof(Result));
    res_outer->data_type = INT;
    res_inner->data_type = INT;
    int* res_outer_pos_vec = NULL;
    int* res_inner_pos_vec = NULL;
    size_t res_tuples_num = 0;
    
    if(gch_val_vec1->data_type == INT && gch_val_vec2->data_type == INT){
        int* val_vec1 = (int*) gch_val_vec1->payload;
        int* val_vec2 = (int*) gch_val_vec2->payload;
        int* outer_val_vec = NULL;
        int* inner_val_vec = NULL;
        if(tuples_num1 > tuples_num2){
            //val_vec 1 -> inner
            //val_vec 2 -> outer
            res_outer_pos_vec = malloc(PAGE_SIZE * sizeof(int));
            res_inner_pos_vec = malloc(PAGE_SIZE * sizeof(int));
            outer_val_vec = val_vec2;
            outer_pos_vec = pos_vec2;
            outer_tuples_num = tuples_num2;
            inner_val_vec = val_vec1;
            inner_pos_vec = pos_vec1;
            inner_tuples_num = tuples_num1;
            if(jt == NESTED_LOOP){
                execute_nested_loop_join((void*)outer_val_vec, outer_pos_vec, outer_tuples_num,
                                         (void*)inner_val_vec, inner_pos_vec, inner_tuples_num,
                                         &res_outer_pos_vec, &res_inner_pos_vec, &res_tuples_num, INT);
            }else{
                execute_hash_join((void*)outer_val_vec, outer_pos_vec, outer_tuples_num,
                                  (void*)inner_val_vec, inner_pos_vec, inner_tuples_num,
                                  &res_outer_pos_vec, &res_inner_pos_vec, &res_tuples_num, INT);
            }
            res_outer->payload = (void*) res_outer_pos_vec;
            res_outer->num_tuples = res_tuples_num;
            res_inner->payload = (void*) res_inner_pos_vec;
            res_inner->num_tuples = res_tuples_num;
            
            GCHandle* gch_res_1 = malloc(sizeof(GCHandle));
            strcpy(gch_res_1->name, query->client_variables[0]);
            gch_res_1->type = RESULT;
            gch_res_1->p.result = res_inner;
            insert_context(query->context_table, gch_res_1->name, (void*) gch_res_1, GCOLUMN);
            cs165_log(stdout, "adding new context with variable name: %s\n", gch_res_1->name);
            
            GCHandle* gch_res_2 = malloc(sizeof(GCHandle));
            strcpy(gch_res_2->name, query->client_variables[1]);
            gch_res_2->type = RESULT;
            gch_res_2->p.result = res_outer;
            insert_context(query->context_table, gch_res_2->name, (void*) gch_res_2, GCOLUMN);
            cs165_log(stdout, "adding new context with variable name: %s\n", gch_res_2->name);
            
            msg->status = OK_DONE;
        }else{
            //tuples_num1 <= tuples_num2
            //val_vec 1 -> outer
            //val_vec 2 -> inner
            res_outer_pos_vec = malloc(PAGE_SIZE * sizeof(int));
            res_inner_pos_vec = malloc(PAGE_SIZE * sizeof(int));
            outer_val_vec = val_vec1;
            outer_pos_vec = pos_vec1;
            outer_tuples_num = tuples_num1;
            inner_val_vec = val_vec2;
            inner_pos_vec = pos_vec2;
            inner_tuples_num = tuples_num2;
            if(jt == NESTED_LOOP){
                execute_nested_loop_join((void*)outer_val_vec, outer_pos_vec, outer_tuples_num,
                                         (void*)inner_val_vec, inner_pos_vec, inner_tuples_num,
                                         &res_outer_pos_vec, &res_inner_pos_vec, &res_tuples_num, INT);
            }else{
                execute_hash_join((void*)outer_val_vec, outer_pos_vec, outer_tuples_num,
                                  (void*)inner_val_vec, inner_pos_vec, inner_tuples_num,
                                  &res_outer_pos_vec, &res_inner_pos_vec, &res_tuples_num, INT);
            }
            res_outer->payload = (void*) res_outer_pos_vec;
            res_outer->num_tuples = res_tuples_num;
            res_inner->payload = (void*) res_inner_pos_vec;
            res_inner->num_tuples = res_tuples_num;
            
            GCHandle* gch_res_1 = malloc(sizeof(GCHandle));
            strcpy(gch_res_1->name, query->client_variables[0]);
            gch_res_1->type = RESULT;
            gch_res_1->p.result = res_outer;
            insert_context(query->context_table, gch_res_1->name, (void*) gch_res_1, GCOLUMN);
            cs165_log(stdout, "adding new context with variable name: %s\n", gch_res_1->name);
            
            GCHandle* gch_res_2 = malloc(sizeof(GCHandle));
            strcpy(gch_res_2->name, query->client_variables[1]);
            gch_res_2->type = RESULT;
            gch_res_2->p.result = res_inner;
            insert_context(query->context_table, gch_res_2->name, (void*) gch_res_2, GCOLUMN);
            cs165_log(stdout, "adding new context with variable name: %s\n", gch_res_2->name);
            
            msg->status = OK_DONE;
        }
    }else if(gch_val_vec1->data_type == FLOAT && gch_val_vec2->data_type == FLOAT){
        //TODO: support double type join
    }else{
        //TODO: support long type join
    }
}
//separated from execute_delete_operator so we can use it for update
void execute_delete_simple(Table* table, int* pos_vec, size_t tuples_num){
    Column* col = NULL;
    ColumnIndex* ci = NULL;
    BTreeNode* root = NULL;
    int pos;
    int key;
    for(size_t j=0;j<table->col_count;j++){
        ci = NULL;
        root = NULL;
        col = &(table->columns[j]);
        if(col->it == BTREE_CLUSTERED || col->it == BTREE_UNCLUSTERED){
            root = (BTreeNode*) col->index_file;
        }else if(col->it == SORTED_UNCLUSTERED){
            ci = (ColumnIndex*) col->index_file;
        }
        //TODO:can we do better? can we move data and update index in one pass?
        //entirely possible for all cases, but have to assume pos_vec is sorted, a trade-off
        //try it if we have time left
        //it also seems to be a sound idea to move data in one pass and update index one by one
        for(int i=tuples_num-1;i>=0;i--){
            pos=pos_vec[i];
            key=col->data[pos];
            for(size_t k=pos;k+1<col->size;k++){
                col->data[k] = col->data[k+1];
            }
            if(root != NULL){
                btree_remove(root, key, pos);
            }else if(ci != NULL){
                sorted_delete_and_update(ci, col->size, pos);
            }
            col->size--;
        }
        
    }
    table->table_length -= tuples_num;
}

void execute_delete_one_pass(Table* table, int* pos_vec, size_t tuples_num){
    if(tuples_num <= 0){
        return;
    }
    Column* col = NULL;
    ColumnIndex* ci = NULL;
    BTreeNode* root = NULL;
    int pos;
    int key;
    int real_pos=0;
    int delete_pos=0;
    int shift=0;
    size_t original_column_size=0;
    for(size_t j=0;j<table->col_count;j++){
        ci = NULL;
        root = NULL;
        col = &(table->columns[j]);
        original_column_size=col->size;
        if(col->it == BTREE_CLUSTERED || col->it == BTREE_UNCLUSTERED){
            root = (BTreeNode*) col->index_file;
        }else if(col->it == SORTED_UNCLUSTERED){
            ci = (ColumnIndex*) col->index_file;
        }
        //update index one by one
        for(int i=tuples_num-1;i>=0;i--){
            pos=pos_vec[i];
            key=col->data[pos];
            if(root != NULL){
                btree_remove(root, key, pos);
            }else if(ci != NULL){
                //can do better: sort index first according to pos, update pos in one pass, then sort index according to key again to get back
                sorted_delete_and_update(ci, col->size, pos);
            }
            col->size--;
        }
        
        //remove data in one pass
        //we assume pos_vec is sorted
        real_pos=0;
        delete_pos=0;
        shift=0;
        while(real_pos+tuples_num<original_column_size){
            while(delete_pos < (int)tuples_num && pos_vec[delete_pos]==(real_pos + shift)){
                shift++;
                delete_pos++;
            }
            col->data[real_pos]=col->data[real_pos+shift];
            real_pos++;
        }
        
    }
    table->table_length -= tuples_num;
}

void execute_delete_operator(DbOperator* query, message* msg){
    Table* table = query->operator_fields.delete_operator.table;
    Result* res_pos_vec = query->operator_fields.delete_operator.pos_vec;
    size_t tuples_num = res_pos_vec->num_tuples;
    int* pos_vec = (int*) res_pos_vec->payload;
    execute_delete_one_pass(table, pos_vec, tuples_num);
    msg->status = OK_DONE;
}

void execute_update_operator(DbOperator* query, message* msg){
    Table* table = query->operator_fields.update_operator.table;
    Result* res_pos_vec = query->operator_fields.update_operator.pos_vec;
    size_t tuples_num = res_pos_vec->num_tuples;
    int* pos_vec = (int*) res_pos_vec->payload;
    Column* col = query->operator_fields.update_operator.col;
    int new_value = query->operator_fields.update_operator.new_value;
    //update as first delete then insert
    //get all rows
    int** rows = malloc(tuples_num * sizeof(int*));
    int target_pos;
    Column* cur_col;
    for(size_t i=0;i<tuples_num;i++){
        rows[i] = malloc(table->col_count * sizeof(int));
        target_pos = pos_vec[i];
        for(size_t j=0;j<table->col_count;j++){
            cur_col = &(table->columns[j]);
            if(cur_col == col){
                rows[i][j] = new_value;
            }else{
                rows[i][j] = cur_col->data[target_pos];
            }
        }
    }
    //delete rows
    execute_delete_one_pass(table, pos_vec, tuples_num);
    //insert rows
    for(size_t i=0;i<tuples_num;i++){
        execute_insert(table, rows[i], msg);
    }
    msg->status = OK_DONE;
}

void execute_DbOperator(DbOperator* query, message* send_message) {
    cs165_log(stdout, "Query parsed. Executing the query...\n");
    if(batch_mode){
        if(query->type == BATCH_MODE_BEGIN){
            batch_size++;
            batched_queries=realloc(batched_queries, batch_size*sizeof(DbOperator*));
            batched_queries[batch_size-1]=query;
        }else if(query->type == BATCH_MODE_EXECUTE){
            execute_batched_queries(send_message);
            free_query(query);
        }
    }else{
        if(query->type == CREATE){
            execute_create_operator(query, send_message);
        }else if(query->type == INSERT){
            execute_insert_operator(query, send_message);
        }else if(query->type == SELECT){
            execute_select_operator(query, send_message);
        }else if(query->type == FETCH){
            execute_fetch_operator(query, send_message);
        }else if(query->type == AGGREGATE){
            execute_aggregate_operator(query, send_message);
        }else if(query->type == JOIN){
            execute_join_operator(query, send_message);
        }else if (query->type == DELETE){
            execute_delete_operator(query, send_message);
        }else if (query->type == UPDATE){
            execute_update_operator(query, send_message);
        }else if(query->type == PRINT){
            execute_print_operator(query, send_message);
        }else if(query->type == LOAD){
            execute_load_operator(query, send_message);
        }else if(query->type == SHUTDOWN){
            shutdown_server(send_message);
        }
        free_query(query);
        return;
    }
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
int handle_client(int client_socket) {
    int done = 0;
    int length = 0;
    int shutdown = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ContextTable* client_context_table = initialize_context();

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';
            
            if(strncmp(recv_message.payload, "shutdown", 8) == 0){
                shutdown = 1;
            }
            
            //we have two special commands that requies additional communication between client and server:
            //1) print 2) load
            if(strncmp(recv_message.payload, "print", 5) == 0){
                DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context_table);
                execute_DbOperator(query, &send_message);
                continue;
            }else if(strncmp(recv_message.payload, "load", 4) == 0){
                DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context_table);
                execute_DbOperator(query, &send_message);
                continue;
            }

            // 1. Parse command
            //    Query string is converted into a request for an database operator
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context_table);
        //
            // 2. Handle request
            //    Corresponding database operator is executed over the query
            char* result = "";
            if(query != NULL){
                execute_DbOperator(query, &send_message);
            }

            send_message.length = strlen(result);
            char send_buffer[send_message.length + 1];
            strcpy(send_buffer, result);
            send_message.payload = send_buffer;
            
            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response to the request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }
        }
    } while (!done);
    
    clean_context_table(client_context_table);
    cs165_log(stdout, "finish cleaning client context table\n");
    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
    return shutdown;
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

BTreeNode* load_btree_recursive(FILE* fd, BTreeNode*** leaf_vec, size_t* leaf_vec_size){
    BTreeNode* root = malloc(1 * sizeof(BTreeNode));
    fread(root, sizeof(BTreeNode), 1, fd);
    if(root->is_leaf){
        fread(root->core_node.lnode.key_vec, sizeof(int), root->key_count, fd);
        fread(root->core_node.lnode.pos_vec, sizeof(int), root->key_count, fd);
        root->core_node.lnode.pre = NULL;
        root->core_node.lnode.next = NULL;
        if(leaf_vec == NULL){
            *leaf_vec = malloc(1 * sizeof(BTreeNode*));
            *leaf_vec[0] = root;
            *leaf_vec_size = 1;
        }else{
            *leaf_vec = realloc(*leaf_vec, (*leaf_vec_size+1) * sizeof(BTreeNode*));
            *leaf_vec[*leaf_vec_size] = root;
            *leaf_vec_size += 1;
        }
        return root;
    }else{
        fread(root->core_node.inode.keys, sizeof(int), root->key_count, fd);
        for(int i=0;i<=root->key_count;i++){
            root->core_node.inode.childs[i]=load_btree_recursive(fd, leaf_vec, leaf_vec_size);
        }
    }
    return root;
}

void* load_btree(FILE* fd){
    BTreeNode** leaf_vec = NULL;
    size_t leaf_vec_size = 0;
    BTreeNode* root = load_btree_recursive(fd, &leaf_vec, &leaf_vec_size);
    //link all leaf nodes
    BTreeNode* pre_node;
    BTreeNode* cur_node;
    for(size_t i=1;i<leaf_vec_size;i++){
        pre_node = leaf_vec[i-1];
        cur_node = leaf_vec[i];
        pre_node->core_node.lnode.next = cur_node;
        cur_node->core_node.lnode.pre = pre_node;
    }
    free(leaf_vec);
    return (void*) root;
}

void load_db(){
    FILE* fd = fopen("db_meta.txt", "rb");
    if(!fd){
        //we don't have a db yet. do nothing.
        cs165_log(stdout, "cannot find existing file containing db data at the moment\n");
        return;
    }
    //found existing db meta file
    current_db = malloc(sizeof(Db));
    db_catalog = initialize_context();
    fread(current_db, sizeof(Db), 1, fd);
    
    //load db meta data
    current_db->tables = malloc(current_db->tables_capacity * sizeof(Table));
    size_t tables_size = current_db->tables_size;
    Table* table;
    size_t columns_count;
    Column* column;
    for(size_t i=0;i<tables_size;i++){
        table = &(current_db->tables[i]);
        //load table meta data
        fread(table, sizeof(Table), 1, fd);
        insert_context(db_catalog, table->name, (void*) table, TABLE);
        table->columns = malloc(table->col_capacity * sizeof(Column));
        columns_count = table->col_count;
        for(size_t j=0;j<columns_count;j++){
            column = &(table->columns[j]);
            //load column meta data
            fread(column, sizeof(Column), 1, fd);
            column->data = malloc(table->table_length_capacity * sizeof(int));
            fread(column->data, sizeof(int), column->size, fd);
            //load column index
            if(column->it == BTREE_CLUSTERED || column->it == BTREE_UNCLUSTERED){
                column->index_file = load_btree(fd);
            }else if(column->it == SORTED_UNCLUSTERED){
                ColumnIndex* ci = malloc(sizeof(ColumnIndex));
                ci->key_vec = malloc(table->table_length_capacity * sizeof(int));
                ci->pos_vec = malloc(table->table_length_capacity * sizeof(int));
                fread(ci->key_vec, sizeof(int), column->size, fd);
                fread(ci->pos_vec, sizeof(int), column->size, fd);
                column->index_file = (void*) ci;
            }
            GCHandle* gch = malloc(sizeof(GCHandle));
            gch->p.column = column;
            strcpy(gch->name, column->name);
            gch->type = COLUMN;
            insert_context(db_catalog, column->name, (void*) gch, GCOLUMN);
        }
    }
    fclose(fd);
    cs165_log(stdout, "finish loading db\n");
    return;
}

void dump_and_free_btree_recursive(FILE* fd, BTreeNode* cur){
    fwrite(cur, sizeof(BTreeNode), 1, fd);
    if(cur->is_leaf){
        fwrite(&cur->core_node.lnode.key_vec, sizeof(int), cur->key_count, fd);
        fwrite(&cur->core_node.lnode.pos_vec, sizeof(int), cur->key_count, fd);
    }else{
        fwrite(&cur->core_node.inode.keys, sizeof(int), cur->key_count, fd);
        for(int i=0;i<=cur->key_count;i++){
            dump_and_free_btree_recursive(fd, (BTreeNode*) cur->core_node.inode.childs[i]);
        }
    }
    free(cur);
}

void dump_db(Db* db, message* msg){
    FILE* fd = fopen("db_meta.txt", "wb");
    if(!fd){
        log_err("cannot dump db!\n");
    }
    //metadata for this db
    fwrite(db, sizeof(Db), 1, fd);
    size_t tables_size = db->tables_size;
    Table* table;
    size_t columns_num;
    Column* column;
    BTreeNode* root;
    ColumnIndex* ci;
    for(size_t i=0;i<tables_size;i++){
        table = &(db->tables[i]);
        //metadata for this table
        fwrite(table, sizeof(Table), 1, fd);
        columns_num = table->col_count;
        for(size_t j=0;j<columns_num;j++){
            column = &(table->columns[j]);
            //metadata for this column
            fwrite(column, sizeof(Column), 1, fd);
            if(column->it == BTREE_CLUSTERED || column->it == BTREE_UNCLUSTERED){
                root = (BTreeNode*) column->index_file;
                dump_and_free_btree_recursive(fd, root);
            }else if(column->it == SORTED_UNCLUSTERED){
                ci = (ColumnIndex*) column->index_file;
                fwrite(ci->key_vec, sizeof(int), column->size, fd);
                fwrite(ci->pos_vec, sizeof(int), column->size, fd);
                free(ci->key_vec);
                free(ci->pos_vec);
                free(ci);
            }
            //stored data in this column
            fwrite(column->data, sizeof(int), column->size, fd);
            free(column->data);
        }
        free(table->columns);
    }
    free(db->tables);
    free(db);
    fclose(fd);
    msg->status = OK_DONE;
}

void shutdown_server(message* msg){
    if(current_db!=NULL){
        dump_db(current_db, msg);
        cs165_log(stdout, "finish dumping db data at the moment\n");
    }
    cs165_log(stdout, "before cleaning db context table\n");
    clean_context_table(db_catalog);
    cs165_log(stdout, "finish cleaning db context table\n");
    msg->status=OK_DONE;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
// 
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients? 
//      Is there a maximum number of concurrent client connections you will allow?
//      What aspects of siloes or isolation are maintained in your design? (Think `what` is shared between `whom`?)
int main(void)
{
    load_db();
    int done = 0;
    while(!done){
        int server_socket = setup_server();
        if (server_socket < 0) {
            exit(1);
        }

        log_info("Waiting for a connection %d ...\n", server_socket);

        struct sockaddr_un remote;
        socklen_t t = sizeof(remote);
        int client_socket = 0;

        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }

        handle_client(client_socket);
    }
    return 0;
}

//int main(void)
//{
//    int server_socket = setup_server();
//    if (server_socket < 0) {
//        exit(1);
//    }
//
//    log_info("Waiting for a connection %d ...\n", server_socket);
//
//    struct sockaddr_un remote;
//    socklen_t t = sizeof(remote);
//    int client_socket = 0;
//
//    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
//        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
//        exit(1);
//    }
//
//    handle_client(client_socket);
//
//    return 0;
//}
