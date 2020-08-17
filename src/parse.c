/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"
#include "message.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message* msg) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        msg->status= INCORRECT_FORMAT;
    }
    return token;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/
//Usage create(col, col_name, tbl_var)

DbOperator* parse_create_col(char* create_arguments, message* msg) {
    char* col_name = next_token(&create_arguments, msg);
    char* table_name = next_token(&create_arguments, msg);
    if (msg->status == INCORRECT_FORMAT) {
        return NULL;
    }
    col_name = trim_quotes(col_name);
    int last_char = strlen(table_name) - 1;
    if (table_name[last_char] != ')') {
        msg->status = INCORRECT_FORMAT;
        return NULL;
    }
    table_name[last_char] = '\0';
    cs165_log(stdout, "table name: %s\n", table_name);
    cs165_log(stdout, "col name: %s\n", col_name);
    //check that table indeed exists
    Table* table = (Table*) find_context(db_catalog, table_name, TABLE);
    if(table == NULL){
        msg->status = OBJECT_NOT_FOUND;
        cs165_log(stdout, "Table does not exist\n");
        return NULL;
    }
    //check if this table can hold one more column
    if(table->col_count == table->col_capacity){
        msg->status = TABLE_FULL;
        cs165_log(stdout, "Table reaches pre-set column capacity\n");
        return NULL;
    }
    //check that no duplicate column with the same name
    char full_col_name[strlen(table_name)+strlen(col_name)+2];
    strcpy(full_col_name, table_name);
    strcat(full_col_name, ".");
    strcat(full_col_name, col_name);
    full_col_name[strlen(table_name)+strlen(col_name)+1]='\0';
    if(find_context(db_catalog, full_col_name, GCOLUMN) != NULL){
        msg->status = OBJECT_ALREADY_EXISTS;
        cs165_log(stdout, "Duplicative column name\n");
        return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _COLUMN;
    strcpy(dbo->operator_fields.create_operator.name, full_col_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.table = table;
    return dbo;
}

//Usage create(tbl, t_name, db_name, col_cnt)
DbOperator* parse_create_tbl(char* create_arguments, message* msg) {
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, msg);
    char* db_name = next_token(create_arguments_index, msg);
    char* col_cnt = next_token(create_arguments_index, msg);

    // not enough arguments
    if (msg->status == INCORRECT_FORMAT) {
        return NULL;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';
    cs165_log(stdout, "table name: %s\n", table_name);
    cs165_log(stdout, "db name: %s\n", db_name);
    cs165_log(stdout, "col_count: %s\n", col_cnt);
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        msg->status = OBJECT_NOT_FOUND;
        cs165_log(stdout, "No db or wrong db name\n");
        return NULL; //QUERY_UNSUPPORTED
    }
    //check that no duplicate table with the same name
    char full_table_name[strlen(current_db->name)+strlen(table_name)+2];
    strcpy(full_table_name, current_db->name);
    strcat(full_table_name, ".");
    strcat(full_table_name, table_name);
    full_table_name[strlen(current_db->name)+strlen(table_name)+1] = '\0';
    if(find_context(db_catalog, full_table_name, TABLE) != NULL){
        //table with this name has been created before this request
        msg->status = OBJECT_ALREADY_EXISTS;
        cs165_log(stdout, "Duplicative table name\n");
        return NULL;
    }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        msg->status = INVALID_ARGUMENT;
        cs165_log(stdout, "Column count not positive\n");
        return NULL;
    }
    // make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _TABLE;
    strcpy(dbo->operator_fields.create_operator.name, full_table_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.col_count = column_cnt;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/


DbOperator* parse_create_db(char* create_arguments, message* msg) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return NULL;
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return NULL;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return NULL;
        }
        // make create operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
        dbo->operator_fields.create_operator.create_type = _DB;
        strcpy(dbo->operator_fields.create_operator.name, db_name);
        msg->status = OK_DONE;
        return dbo;
    }
}

//Usage: create(idx,<col_name>,[btree, sorted], [clustered, unclustered])
DbOperator* parse_create_idx(char* create_arguments, message* msg){
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, msg);
    char* idx_type = next_token(create_arguments_index, msg);
    char* cluster_type = next_token(create_arguments_index, msg);
    
    // not enough arguments
    if (msg->status == INCORRECT_FORMAT) {
        return NULL;
    }
    
    char *col_name_copy, *to_free;
    col_name_copy = to_free = malloc((strlen(col_name)+1) * sizeof(char));
    strcpy(col_name_copy, col_name);
    char* first_dot_pointer = strsep(&col_name_copy, ".");
    char* second_dot_pointer = strsep(&col_name_copy, ".");
    char full_table_name[64];
    strcpy(full_table_name, first_dot_pointer); //copy db name
    strcat(full_table_name, ".");
    strcat(full_table_name, second_dot_pointer);
    
    Table* table = (Table*) find_context(db_catalog, full_table_name, TABLE);
    if(table == NULL){
        msg->status = OBJECT_NOT_FOUND;
        cs165_log(stdout, "Table does not exist\n");
        return NULL;
    }
    
    //check that that column indeed exists
    GCHandle* gch1 = (GCHandle*) find_context(db_catalog, col_name, GCOLUMN);
    if(gch1 == NULL || gch1->type != COLUMN){
        msg->status = OBJECT_NOT_FOUND;
        cs165_log(stdout, "target column does not exist\n");
        return NULL;
    }
    Column* col = gch1->p.column;
    
    IndexType it = NONE;
    if(strcmp(idx_type, "btree") == 0){
        if(strcmp(cluster_type, "clustered") == 0){
            it = BTREE_CLUSTERED;
        }else if(strcmp(cluster_type, "unclustered") == 0){
            it = BTREE_UNCLUSTERED;
        }
    }else if(strcmp(idx_type, "sorted") == 0){
        if(strcmp(cluster_type, "clustered") == 0){
            it = SORTED_CLUSTERED;
        }else if(strcmp(cluster_type, "unclustered") == 0){
            it = SORTED_UNCLUSTERED;
        }
    }
    if(it == NONE){
        msg->status = QUERY_UNSUPPORTED;
        return NULL;
    }
    
    DbOperator* dbo = malloc(1 * sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _IDX;
    strcpy(dbo->operator_fields.create_operator.name, col_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.table = table;
    dbo->operator_fields.create_operator.column = col;
    dbo->operator_fields.create_operator.it = it;
    free(to_free);
    return dbo;
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator* parse_create(char* create_arguments, message* msg) {
    DbOperator* dbo = NULL;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, msg);
        if (msg->status == INCORRECT_FORMAT) {
            return NULL;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                dbo = parse_create_db(tokenizer_copy, msg);
            } else if (strcmp(token, "tbl") == 0) {
                dbo = parse_create_tbl(tokenizer_copy, msg);
            } else if (strcmp(token, "col") == 0) {
                dbo = parse_create_col(tokenizer_copy, msg);
            } else if (strcmp(token, "idx") == 0) {
                dbo = parse_create_idx(tokenizer_copy, msg);
            }else {
                msg->status = UNKNOWN_COMMAND;
            }
        }
    } else {
        msg->status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return dbo;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/
//usage: relational_insert(<tbl_var>, [INT1], [INT2])
DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        query_command = trim_parenthesis(query_command); // get rid of )
        char** command_index = &query_command;
        // parse table input
        char* table_name = next_token(command_index, send_message);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        cs165_log(stdout, "table name: %s\n", table_name);
        // check the context table and make sure it exists.
        Table* insert_table = (Table*) find_context(db_catalog, table_name, TABLE);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        cs165_log(stdout, "table indeed exists\n");
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            cs165_log(stdout, "get value: %s\n", token);
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        cs165_log(stdout, "insert_table col_count: %zd\n", insert_table->col_count);
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            cs165_log(stdout, "number of given values not match table count\n");
            free (dbo);
            return NULL;
        } 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

//Usage1: <vec_pos>=select(<col_name>,<low>,<high>)
//Usage2: <vec_pos>=select(<posn_vec>,<val_vec>,<low>,<high>)
DbOperator* parse_select(char* query_command, ContextTable* client_context_table, message* msg){
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc((strlen(query_command)+1) * sizeof(char));
    strcpy(tokenizer_copy, query_command);
    size_t arg_count = 1;
    //first iterate through query command to find out how many arguments are there
    for(size_t i=0;tokenizer_copy[i];i++){
        if(tokenizer_copy[i] == ','){
            arg_count++;
        }
    }
    cs165_log(stdout, "Made through arg count loop \n");
    //check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) != 0) {
        cs165_log(stdout, "Command format incorrect \n");
        msg->status = UNKNOWN_COMMAND;
        return NULL;
    }
    tokenizer_copy++;
    tokenizer_copy = trim_parenthesis(tokenizer_copy);
    GCHandle* gch1 = NULL;
    GCHandle* gch2 = NULL;
    ComparatorType ct1;
    ComparatorType ct2;
    long int lowerbound = 0;
    long int upperbound = 0;
    if (arg_count < 3) {
        cs165_log(stdout, "missing arguments \n");
        msg->status = INCORRECT_FORMAT;
        return NULL;
    }else if (arg_count == 3) {
        char* col_name = next_token(&tokenizer_copy, msg);
        char* low = next_token(&tokenizer_copy, msg);
        char* high = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "missing next_token arguments \n");
            return NULL;
        }
        //check if col_name is a true column in db first
        gch1 = (GCHandle*) find_context(db_catalog, col_name, GCOLUMN);
        if (gch1 == NULL){
            //if null, check if col_name is a client variable
            gch1 = (GCHandle*) find_context(client_context_table, col_name, GCOLUMN);
            if(gch1 == NULL){
                //something is wrong
                cs165_log(stdout, "cannot find necessary select context\n");
                msg->status = OBJECT_NOT_FOUND;
                return NULL;
            }
        }
        if(strcmp(low, "null") != 0){
            cs165_log(stdout, "first comparator not null \n");
            ct1 = GREATER_THAN_OR_EQUAL;
            lowerbound = atoi(low);
        }else{
            ct1 = NO_COMPARISON;
        }
        if(strcmp(high, "null") != 0){
            cs165_log(stdout, "second comparator not null \n");
            ct2 = LESS_THAN;
            upperbound = atoi(high);
        }else{
            ct2 = NO_COMPARISON;
        }
    }else if (arg_count == 4){
        char* pos_vec = next_token(&tokenizer_copy, msg);
        char* val_vec = next_token(&tokenizer_copy, msg);
        char* low = next_token(&tokenizer_copy, msg);
        char* high = next_token(&tokenizer_copy, msg);
        
        //check if pos_vec is a true column in db first
        gch1 = (GCHandle*) find_context(db_catalog, pos_vec, GCOLUMN);
        if (gch1 == NULL){
            //if null, check if col_name is a client variable
            gch1 = (GCHandle*) find_context(client_context_table, pos_vec, GCOLUMN);
            if(gch1 == NULL){
                //something is wrong
                cs165_log(stdout, "cannot find necessary select context\n");
                msg->status = OBJECT_NOT_FOUND;
                return NULL;
            }
        }
        //check if val_vec is a true column in db first
        gch2 = (GCHandle*) find_context(db_catalog, val_vec, GCOLUMN);
        if (gch2 == NULL){
            //if null, check if val_vec is a client variable
            gch2 = (GCHandle*) find_context(client_context_table, val_vec, GCOLUMN);
            if(gch2 == NULL){
                //something is wrong
                cs165_log(stdout, "cannot find necessary select context\n");
                msg->status = OBJECT_NOT_FOUND;
                return NULL;
            }
        }
        if(strcmp(low, "null") != 0){
            cs165_log(stdout, "firt comparator not null \n");
            ct1 = GREATER_THAN_OR_EQUAL;
            lowerbound = atoi(low);
        }else{
            ct1 = NO_COMPARISON;
        }
        if(strcmp(high, "null") != 0){
            cs165_log(stdout, "second comparator not null \n");
            ct2 = LESS_THAN;
            upperbound = atoi(high);
        }else{
            ct2 = NO_COMPARISON;
        }
    }else{
        msg->status = INCORRECT_FORMAT;
        cs165_log(stdout, "Command format incorrect \n");
        return NULL;
    }
    cs165_log(stdout, "Made through main parse select loop \n");
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SELECT;
    dbo->operator_fields.select_operator.gch1 = gch1;
    dbo->operator_fields.select_operator.gch2 = gch2;
    dbo->operator_fields.select_operator.comparator.lowerbound = lowerbound;
    dbo->operator_fields.select_operator.comparator.upperbound = upperbound;
    dbo->operator_fields.select_operator.comparator.ct1 = ct1;
    dbo->operator_fields.select_operator.comparator.ct2 = ct2;
    free(to_free);
    return dbo;
}

// Usage: <vec_val>=fetch(<col_var>,<vec_pos>)
DbOperator* parse_fetch(char* query_command, ContextTable* client_context_table, message* msg){
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc((strlen(query_command)+1) * sizeof(char));
    strcpy(tokenizer_copy, query_command);
    size_t arg_count = 1;
    //first iterate through query command to find out how many arguments are there
    for(size_t i=0;tokenizer_copy[i];i++){
        if(tokenizer_copy[i] == ','){
            arg_count++;
        }
    }
    //check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) != 0) {
        cs165_log(stdout, "Command format incorrect \n");
        msg->status = UNKNOWN_COMMAND;
        return NULL;
    }
    tokenizer_copy++;
    tokenizer_copy = trim_parenthesis(tokenizer_copy);
    GCHandle* gch1 = NULL;
    GCHandle* gch2 = NULL;
    if (arg_count < 2) {
        cs165_log(stdout, "Command format incorrect \n");
        msg->status = INCORRECT_FORMAT;
        return NULL;
    }else if (arg_count == 2) {
        char* col_name = next_token(&tokenizer_copy, msg);
        char* pos_vec = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "Command next_token format incorrect \n");
            return NULL;
        }
        //check if col_name is a true column in db first
        gch1 = (GCHandle*) find_context(db_catalog, col_name, GCOLUMN);
        if (gch1 == NULL){
            //if null, check if col_name is a client variable
            gch1 = (GCHandle*) find_context(client_context_table, col_name, GCOLUMN);
            if(gch1 == NULL){
                //something is wrong
                cs165_log(stdout, "cannot find necessary fetch context \n");
                msg->status = OBJECT_NOT_FOUND;
                return NULL;
            }
        }
        //check if pos_vec is a true column in db first
        gch2 = (GCHandle*) find_context(db_catalog, pos_vec, GCOLUMN);
        if (gch2 == NULL){
            //if null, check if col_name is a client variable
            gch2 = (GCHandle*) find_context(client_context_table, pos_vec, GCOLUMN);
            if(gch2 == NULL){
                //something is wrong
                cs165_log(stdout, "cannot find necessary fetch context \n");
                msg->status = OBJECT_NOT_FOUND;
                return NULL;
            }
        }
    }else{
        cs165_log(stdout, "Command format incorrect \n");
        msg->status = INCORRECT_FORMAT;
        return NULL;
    }
   

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = FETCH;
    dbo->operator_fields.fetch_operator.gch1 = gch1;
    dbo->operator_fields.fetch_operator.gch2 = gch2;
    free(to_free);
    return dbo;
}

// Usage 1: <agg_val>=agg(<vec_val>)
// Usage 2: <agg_pos>, <agg_val>=agg(<vec_pos>,<vec_val>)
DbOperator* parse_aggregate(char* query_command, AggregateType type, ContextTable* client_context_table, message* msg){
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc((strlen(query_command)+1) * sizeof(char));
    strcpy(tokenizer_copy, query_command);
    size_t arg_count = 1;
    //first iterate through query command to find out how many arguments are there
    for(size_t i=0;tokenizer_copy[i];i++){
        if(tokenizer_copy[i] == ','){
            arg_count++;
        }
    }
    //check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) != 0) {
        msg->status = UNKNOWN_COMMAND;
        cs165_log(stdout, "format not right\n");
        return NULL;
    }
    tokenizer_copy++;
    tokenizer_copy = trim_parenthesis(tokenizer_copy);
    GCHandle* gch1 = NULL;
    GCHandle* gch2 = NULL;
    if (arg_count < 1) {
        msg->status = INCORRECT_FORMAT;
        cs165_log(stdout, "get arg count less than 1\n");
        return NULL;
    } else if (arg_count == 1){
        char* val_vec = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get first argument right\n");
            return NULL;
        }
        //check if val_vec is a true column in db first
        gch1 = (GCHandle*) find_context(db_catalog, val_vec, GCOLUMN);
        if (gch1 == NULL){
            //if null, check if val_vec is a client variable
            gch1 = (GCHandle*) find_context(client_context_table, val_vec, GCOLUMN);
            if(gch1 == NULL){
                //something is wrong
                cs165_log(stdout, "cannot get context for first argument string: %s \n", val_vec);
                msg->status = OBJECT_NOT_FOUND;
                return NULL;
            }
        }
    } else if (arg_count == 2){
        char* pos_vec = next_token(&tokenizer_copy, msg);
        char* val_vec = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get two arguments right\n");
            return NULL;
        }
        //check if pos_vec is a true column in db first
        gch1 = (GCHandle*) find_context(db_catalog, pos_vec, GCOLUMN);
        if (gch1 == NULL){
            //if null, check if col_name is a client variable
            gch1 = (GCHandle*) find_context(client_context_table, pos_vec, GCOLUMN);
            if(gch1 == NULL){
                //something is wrong
                msg->status = OBJECT_NOT_FOUND;
                cs165_log(stdout, "cannot get context for first argument string: %s \n", pos_vec);
                return NULL;
            }
        }
        //check if pos_vec is a true column in db first
        gch2 = (GCHandle*) find_context(db_catalog, val_vec, GCOLUMN);
        if (gch2 == NULL){
            //if null, check if col_name is a client variable
            gch2 = (GCHandle*) find_context(client_context_table, val_vec, GCOLUMN);
            if(gch2 == NULL){
                //something is wrong
                cs165_log(stdout, "cannot get context for second argument string: %s \n", val_vec);
                msg->status = OBJECT_NOT_FOUND;
                return NULL;
            }
        }
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = AGGREGATE;
    dbo->operator_fields.aggregate_operator.gch1 = gch1;
    dbo->operator_fields.aggregate_operator.gch2 = gch2;
    dbo->operator_fields.aggregate_operator.type = type;
    free(to_free);
    return dbo;
}

//Usage: print(<vec_val1>,...)
DbOperator* parse_print(char* query_command, ContextTable* client_context_table, message* msg){
    query_command = trim_parenthesis(query_command);
    char *tokenizer_copy, *to_free1;
    tokenizer_copy = to_free1 = malloc((strlen(query_command)+1) * sizeof(char));
    strcpy(tokenizer_copy, query_command);
    size_t variable_count = 1;
    //first iterate through query command to find out how many arguments are there
    for(size_t i=0;query_command[i];i++){
        if(query_command[i] == ','){
            variable_count++;
        }
    }
    char** variable_list;
    char** to_free2;
    variable_list = to_free2 = calloc(variable_count, sizeof(char*));
    //calloc is needed so that we can check for null later
    GCHandle** gch_list = malloc(variable_count * sizeof(GCHandle*));
    for(size_t i=0;i<variable_count;i++){
        variable_list[i] = next_token(&tokenizer_copy, msg);
        gch_list[i] = (GCHandle*) find_context(client_context_table, variable_list[i], GCOLUMN);
        if(gch_list[i] == NULL){
            gch_list[i] = (GCHandle*) find_context(db_catalog, variable_list[i], GCOLUMN);
            if(gch_list[i] == NULL){
                cs165_log(stdout, "missing variable name: %s\n", variable_list[i]);
                cs165_log(stdout, "variable not found \n");
                msg->status = OBJECT_NOT_FOUND;
                return NULL;
            }
            //TODO: what if we cannot find this variable in both context tables?
        }
    }
    cs165_log(stdout, "variable list found and generated \n");
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = PRINT;
    dbo->operator_fields.print_operator.gch_list = gch_list;
    dbo->operator_fields.print_operator.gch_count = variable_count;
    free(to_free1);
    free(to_free2);
    return dbo;
}

//Usage: load(db_name,table_name,col_count)\n
DbOperator* parse_load(char* query_command, message* msg){
    query_command = trim_parenthesis(query_command);
    query_command = trim_whitespace(query_command);
    char** command_index = &query_command;
    char* db_name = next_token(command_index, msg);
    char* full_table_name = next_token(command_index, msg);
    char* col_cnt = next_token(command_index, msg);
    size_t col_count = atoi(col_cnt);
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        msg->status = OBJECT_NOT_FOUND;
        cs165_log(stdout, "No db or wrong db name\n");
        return NULL;
    }
    Table* table = find_context(db_catalog, full_table_name, TABLE);
    if(table == NULL){
        msg->status = OBJECT_NOT_FOUND;
        cs165_log(stdout, "Table named %s does not exist\n", full_table_name);
        return NULL;
    }
    if(table->col_count != col_count){
        msg->status = OBJECT_NOT_FOUND;
        cs165_log(stdout, "column count received %zd not matched table column count %zd\n", col_cnt, table->col_count);
        return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = LOAD;
    dbo->operator_fields.load_operator.table = table;
    return dbo;
}

//Usage: shutdown\n
DbOperator* parse_shutdown(char* query_command, message* msg){
    (void) query_command;
    if (!current_db) {
        msg->status = OBJECT_NOT_FOUND;
        cs165_log(stdout, "No db exist at the moment");
        return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SHUTDOWN;
    dbo->operator_fields.shutdown_operator.place_holder = 1;
    return dbo;
}

DbOperator* parse_batch_queries(char* query_command, message* msg){
    (void) query_command;
    (void) msg;
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = BATCH_MODE_BEGIN;
    return dbo;
}

DbOperator* parse_batch_execute(char* query_command, message* msg){
    (void) query_command;
    (void) msg;
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = BATCH_MODE_EXECUTE;
    return dbo;
}
//Usage: join(<vec_val1>,<vec_pos1>,<vec_val2>,<vec_pos2>, [hash,nested-loop,...])
DbOperator* parse_join(char* query_command, ContextTable* client_context_table, message* msg){
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc((strlen(query_command)+1) * sizeof(char));
    strcpy(tokenizer_copy, query_command);
    size_t arg_count = 1;
    //first iterate through query command to find out how many arguments are there
    for(size_t i=0;tokenizer_copy[i];i++){
        if(tokenizer_copy[i] == ','){
            arg_count++;
        }
    }
    //check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) != 0) {
        msg->status = UNKNOWN_COMMAND;
        cs165_log(stdout, "format not right\n");
        return NULL;
    }
    tokenizer_copy++;
    tokenizer_copy = trim_parenthesis(tokenizer_copy);
    GCHandle* gch_val_vec1 = NULL;
    GCHandle* gch_pos_vec1 = NULL;
    GCHandle* gch_val_vec2 = NULL;
    GCHandle* gch_pos_vec2 = NULL;
    JoinType jt = NESTED_LOOP;
    if (arg_count != 5) {
        msg->status = INCORRECT_FORMAT;
        cs165_log(stdout, "get arg count less than 5\n");
        return NULL;
    } else {
        char* val_vec1 = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get first argument right\n");
            return NULL;
        }
        gch_val_vec1 = (GCHandle*) find_context(client_context_table, val_vec1, GCOLUMN);
        if(gch_val_vec1 == NULL || gch_val_vec1->type != RESULT){
            //something is wrong
            cs165_log(stdout, "cannot get context for first argument string: %s \n", val_vec1);
            msg->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        char* pos_vec1 = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get second argument right\n");
            return NULL;
        }
        gch_pos_vec1 = (GCHandle*) find_context(client_context_table, pos_vec1, GCOLUMN);
        if(gch_pos_vec1 == NULL || gch_pos_vec1->type != RESULT || gch_pos_vec1->p.result->data_type != INT){
            //something is wrong
            cs165_log(stdout, "cannot get context for second argument string: %s \n", pos_vec1);
            msg->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        if(gch_val_vec1->p.result->num_tuples != gch_pos_vec1->p.result->num_tuples){
            cs165_log(stdout, "val vec 1 size != pos vec 1 size\n");
            msg->status = INCORRECT_FORMAT;
            return NULL;
        }
        char* val_vec2 = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get third argument right\n");
            return NULL;
        }
        gch_val_vec2 = (GCHandle*) find_context(client_context_table, val_vec2, GCOLUMN);
        if(gch_val_vec2 == NULL || gch_pos_vec1->type != RESULT){
            //something is wrong
            cs165_log(stdout, "cannot get context for third argument string: %s \n", val_vec2);
            msg->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        char* pos_vec2 = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get fourth argument right\n");
            return NULL;
        }
        gch_pos_vec2 = (GCHandle*) find_context(client_context_table, pos_vec2, GCOLUMN);
        if(gch_pos_vec2 == NULL || gch_pos_vec2->type != RESULT || gch_pos_vec2->p.result->data_type != INT){
            //something is wrong
            cs165_log(stdout, "cannot get context for fourth argument string: %s \n", pos_vec2);
            msg->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        if(gch_val_vec2->p.result->num_tuples != gch_pos_vec2->p.result->num_tuples){
            cs165_log(stdout, "val vec 2 size != pos vec 2 size\n");
            msg->status = INCORRECT_FORMAT;
            return NULL;
        }
        
        if(gch_val_vec1->p.result->data_type != gch_val_vec2->p.result->data_type){
            cs165_log(stdout, "val vec 1 data type != val vec 2 data type\n");
            msg->status = INCORRECT_FORMAT;
            return NULL;
        }
        char* join_arg = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get fifth argument right\n");
            return NULL;
        }
        if(strcmp(join_arg, "nested-loop") == 0){
            jt = NESTED_LOOP;
        }else if(strcmp(join_arg, "hash") == 0){
            jt = HASH;
        }else{
            cs165_log(stdout, "join type not supported\n");
            msg->status = INCORRECT_FORMAT;
            return NULL;
        }
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = JOIN;
    dbo->operator_fields.join_operator.val_vec1 = gch_val_vec1->p.result;
    dbo->operator_fields.join_operator.pos_vec1 = gch_pos_vec1->p.result;
    dbo->operator_fields.join_operator.val_vec2 = gch_val_vec2->p.result;
    dbo->operator_fields.join_operator.pos_vec2 = gch_pos_vec2->p.result;
    dbo->operator_fields.join_operator.jt = jt;
    free(to_free);
    return dbo;
}

//Usage: relational_delete(<tbl_var>,<vec_pos>)
DbOperator* parse_delete(char* query_command, ContextTable* client_context_table, message* msg){
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc((strlen(query_command)+1) * sizeof(char));
    strcpy(tokenizer_copy, query_command);
    size_t arg_count = 1;
    //first iterate through query command to find out how many arguments are there
    for(size_t i=0;tokenizer_copy[i];i++){
        if(tokenizer_copy[i] == ','){
            arg_count++;
        }
    }
    //check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) != 0) {
        msg->status = UNKNOWN_COMMAND;
        cs165_log(stdout, "format not right\n");
        return NULL;
    }
    tokenizer_copy++;
    tokenizer_copy = trim_parenthesis(tokenizer_copy);
    Table* table = NULL;
    GCHandle* gch_pos_vec = NULL;
    if(arg_count != 2){
        msg->status = INCORRECT_FORMAT;
        cs165_log(stdout, "get arg count != 2\n");
        return NULL;
    }else{
        char* table_name = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get first argument right\n");
            return NULL;
        }
        table = (Table*) find_context(db_catalog, table_name, TABLE);
        if(table == NULL){
            msg->status = OBJECT_NOT_FOUND;
            cs165_log(stdout, "cannot get target table for delete \n");
            return NULL;
        }
        char* pos_vec = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get second argument right\n");
            return NULL;
        }
        gch_pos_vec = (GCHandle*) find_context(client_context_table, pos_vec, GCOLUMN);
        if(gch_pos_vec == NULL || gch_pos_vec->type != RESULT || gch_pos_vec->p.result->data_type != INT){
            //something is wrong
            cs165_log(stdout, "cannot get context for pos_vec for delete operation\n");
            msg->status = OBJECT_NOT_FOUND;
            return NULL;
        }
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = DELETE;
    dbo->operator_fields.delete_operator.table = table;
    dbo->operator_fields.delete_operator.pos_vec = gch_pos_vec->p.result;
    free(to_free);
    return dbo;
}

//Usage: relational_update(<col_var>,<vec_pos>,[INT])
DbOperator* parse_update(char* query_command, ContextTable* client_context_table, message* msg){
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc((strlen(query_command)+1) * sizeof(char));
    strcpy(tokenizer_copy, query_command);
    size_t arg_count = 1;
    //first iterate through query command to find out how many arguments are there
    for(size_t i=0;tokenizer_copy[i];i++){
        if(tokenizer_copy[i] == ','){
            arg_count++;
        }
    }
    //check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) != 0) {
        msg->status = UNKNOWN_COMMAND;
        cs165_log(stdout, "format not right\n");
        return NULL;
    }
    tokenizer_copy++;
    tokenizer_copy = trim_parenthesis(tokenizer_copy);
    Column* col = NULL;
    GCHandle* gch_col = NULL;
    GCHandle* gch_pos_vec = NULL;
    Table* table = NULL;
    int new_value = 0;
    if(arg_count != 3){
        msg->status = INCORRECT_FORMAT;
        cs165_log(stdout, "get arg count != 3\n");
        return NULL;
    }else{
        char* col_name = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get first argument right\n");
            return NULL;
        }
        gch_col = (GCHandle*) find_context(db_catalog, col_name, GCOLUMN);
        if(gch_col == NULL || gch_col->type != COLUMN){
            msg->status = OBJECT_NOT_FOUND;
            cs165_log(stdout, "cannot get target column for update \n");
            return NULL;
        }
        col = gch_col->p.column;
        if(col == NULL){
            msg->status = OBJECT_NOT_FOUND;
            cs165_log(stdout, "corrupted GCHandle: cannot get target column for update \n");
            return NULL;
        }
        
        char* pos_vec = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get second argument right\n");
            return NULL;
        }
        gch_pos_vec = (GCHandle*) find_context(client_context_table, pos_vec, GCOLUMN);
        if(gch_pos_vec == NULL || gch_pos_vec->type != RESULT || gch_pos_vec->p.result->data_type != INT){
            //something is wrong
            cs165_log(stdout, "cannot get context for pos_vec for delete operation\n");
            msg->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        
        char* value = next_token(&tokenizer_copy, msg);
        if(msg->status == INCORRECT_FORMAT){
            cs165_log(stdout, "cannot get third argument right\n");
            return NULL;
        }
        new_value = atoi(value);
        
        //we also need table
        char full_table_name[MAX_SIZE_NAME];
        char *col_name_copy, *to_free1;
        col_name_copy = to_free1 = malloc((strlen(col_name)+1) * sizeof(char));
        strcpy(col_name_copy, col_name);
        char* first_dot_pointer = strsep(&col_name_copy, ".");
        char* second_dot_pointer = strsep(&col_name_copy, ".");
        strcpy(full_table_name, first_dot_pointer);
        strcat(full_table_name, ".");
        strcat(full_table_name, second_dot_pointer);
        free(to_free1);
        table = (Table*) find_context(db_catalog, full_table_name, TABLE);
        if(table == NULL){
            msg->status = OBJECT_NOT_FOUND;
            cs165_log(stdout, "cannot get target table for update \n");
            return NULL;
        }
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = UPDATE;
    dbo->operator_fields.update_operator.table = table;
    dbo->operator_fields.update_operator.col = col;
    dbo->operator_fields.update_operator.pos_vec = gch_pos_vec->p.result;
    dbo->operator_fields.update_operator.new_value = new_value;
    free(to_free);
    return dbo;
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 * 
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse? 
 *      What if such command requires multiple arguments?
 **/
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ContextTable* client_context_table) {
    // a second option is to malloc the dbo here (instead of inside the parse commands). Either way, you should track the dbo
    // and free it when the variable is no longer needed. 
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator));

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }
    char *equals_pointer = strchr(query_command, '=');\
    size_t client_variable_count = 0;
    char client_variables[2][HANDLE_MAX_SIZE];
    if (equals_pointer != NULL) {
        // handle exists, store here.
        client_variable_count=1;
        *equals_pointer = '\0';
        char *client_variable_p, *to_free;
        client_variable_p = to_free = malloc((strlen(query_command)+1) * sizeof(char));
        strcpy(client_variable_p, query_command);
        for(size_t i=0;client_variable_p[i];i++){
            if(client_variable_p[i] == ','){
                client_variable_count++;
            }
        }
        if(client_variable_count==1){
            strcpy(client_variables[0], client_variable_p);
            cs165_log(stdout, "client variable 1: %s\n", client_variables[0]);
        }else{
            //we have two handles
            strcpy(client_variables[0], next_token(&client_variable_p, send_message));
            strcpy(client_variables[1], next_token(&client_variable_p, send_message));
            cs165_log(stdout, "client variable 1: %s\n", client_variables[0]);
            cs165_log(stdout, "client variable 2: %s\n", client_variables[1]);
        }
        query_command = ++equals_pointer;
        free(to_free);
    }

    cs165_log(stdout, "QUERY: %s", query_command);

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);
    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command, send_message);
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "select", 6) == 0){
        query_command += 6;
        dbo = parse_select(query_command, client_context_table, send_message);
    } else if (strncmp(query_command, "fetch", 5) == 0){
        query_command += 5;
        dbo = parse_fetch(query_command, client_context_table, send_message);
    } else if (strncmp(query_command, "max", 3) == 0){
        query_command += 3;
        dbo = parse_aggregate(query_command, MAX, client_context_table, send_message);
    } else if (strncmp(query_command, "min", 3) == 0){
        query_command += 3;
        dbo = parse_aggregate(query_command, MIN, client_context_table, send_message);
    } else if (strncmp(query_command, "avg", 3) == 0){
        query_command += 3;
        dbo = parse_aggregate(query_command, AVG, client_context_table, send_message);
    } else if (strncmp(query_command, "sum", 3) == 0){
        query_command += 3;
        dbo = parse_aggregate(query_command, SUM, client_context_table, send_message);
    } else if (strncmp(query_command, "add", 3) == 0){
        query_command += 3;
        dbo = parse_aggregate(query_command, ADD, client_context_table, send_message);
    } else if (strncmp(query_command, "sub", 3) == 0){
        query_command += 3;
        dbo = parse_aggregate(query_command, SUB, client_context_table, send_message);
    } else if (strncmp(query_command, "print", 5) == 0){
        query_command += 5;
        dbo = parse_print(query_command, client_context_table, send_message);
    } else if (strncmp(query_command, "load", 4) == 0){
        query_command += 4;
        dbo = parse_load(query_command, send_message);
    } else if (strncmp(query_command, "shutdown", 8) == 0){
        query_command += 8;
        dbo = parse_shutdown(query_command, send_message);
    } else if (strncmp(query_command, "batch_queries()", 15) == 0){
        query_command += 15;
        dbo = parse_batch_queries(query_command, send_message);
    } else if (strncmp(query_command, "batch_execute()", 15) == 0){
        query_command += 15;
        dbo = parse_batch_execute(query_command, send_message);
    } else if (strncmp(query_command, "join", 4) == 0){
        query_command += 4;
        dbo = parse_join(query_command, client_context_table, send_message);
    } else if (strncmp(query_command, "relational_delete", 17) == 0){
        query_command +=17;
        dbo = parse_delete(query_command, client_context_table, send_message);
    } else if (strncmp(query_command, "relational_update", 17) == 0){
        query_command +=17;
        dbo = parse_update(query_command, client_context_table, send_message);
    }

    if(dbo == NULL){
        send_message->status = INCORRECT_FORMAT;
        return dbo;
    }else{
        send_message->status = OK_DONE;
    }
    
    dbo->client_fd = client_socket;
    dbo->context_table = client_context_table;
    for(size_t i=0;i<client_variable_count;i++){
        strcpy(dbo->client_variables[i], client_variables[i]);
    }
    dbo->client_variables_num = client_variable_count;
    return dbo;
}
