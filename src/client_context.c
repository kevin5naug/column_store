#include <string.h>
#include "cs165_api.h"
#include "client_context.h"
#include "utils.h"
/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 * 
 */

//select a prime number
#define INITIAL_TABLE_SLOT 103

ContextTable* initialize_context(){
    ContextTable* ct = malloc(sizeof(ContextTable));
    ct->buckets = malloc(INITIAL_TABLE_SLOT * sizeof(ContextNode*));//need to use calloc instead to make sure bucket heads are NULL
    for(size_t i=0;i<INITIAL_TABLE_SLOT;i++){
        ct->buckets[i] = NULL;
    }
    ct->size = INITIAL_TABLE_SLOT;
    return ct;
}

size_t hash(char* key){
    size_t keylen = strlen(key);
    size_t value = 0;
    int temp;
    for(size_t i=0;i<keylen;i++){
        char c = key[i];
        temp = c - '0';
        value = value * 10 + temp;
    }
    return value % INITIAL_TABLE_SLOT;
}

void insert_context(ContextTable* ct, char* context_name, void* context_p, ContextType context_type){
    size_t pos = hash(context_name);
    ContextNode* pre = NULL;
    ContextNode* cur = ct->buckets[pos];
    while(cur != NULL){
        if(strcmp(cur->name, context_name) == 0 && cur->type == context_type){
            //client might reassign the same variable. MUST replace value here.
            //free memory associated with this pointer
            if(cur->type == GCOLUMN){
                //our parser has ensured that we won't insert duplicate columns and tables. Hence, we only need to free memory for client named variable.
                GCHandle* gch = (GCHandle*) cur->p;
                if(gch->type == RESULT){
                    Result* res = gch->p.result;
                    free(res->payload);
                    free(res);
                }
                free(gch);
            }
            cur->p = context_p;
            return;
        }
        pre = cur;
        cur = cur->next;
    }

    //once we have found the place, create new node and do the insertion
    ContextNode* new_node = malloc(sizeof(ContextNode));
    new_node->p = context_p;
    strcpy(new_node->name, context_name);
    new_node->type = context_type;
    new_node->next = NULL;
    if(cur == ct->buckets[pos]){
        //bucket head empty
        ct->buckets[pos] = new_node;
    }else{
        pre->next = new_node;
    }
}


void* find_context(ContextTable* ct, char* context_name, ContextType context_type) {
    size_t pos = hash(context_name);
    ContextNode* cur = ct->buckets[pos];
    if(cur == NULL){
        return NULL;
    }
    while(cur->type != context_type || strcmp(context_name, cur->name) != 0){
        cur = cur->next;
        if(cur == NULL){
            return NULL;
        }
    }
    if(cur != NULL){
        return cur->p;
    }
    return NULL;
}

void clean_context_table(ContextTable* ct){
    ContextNode* cur = NULL;
    ContextNode* to_free = NULL;
    GCHandle* gch;
    Result* result;
    for(size_t i=0;i<ct->size;i++){
        cur = ct->buckets[i];
        to_free = NULL;
        while(cur != NULL){
            to_free = cur;
            cur = cur->next;
            cs165_log(stdout, "before freeing ContextNode \n");
            if(to_free->type == GCOLUMN){
                // might cause error if we try to free tables as well ? because client context table should not contain ANY table and because db catalog table should have been freed by now in shutdown.
                // check for memory leakage here later
                gch = (GCHandle*) to_free->p;
                if(gch->type == RESULT){
                    // might cause error if we try to free columns as well ? because client context table should not contain ANY column and because db catalog column should have been freed by now in shut down.
                    // check for memory leakage here later
                    result = gch->p.result;
                    cs165_log(stdout, "working with gch\n");
                    free(result->payload);
                    free(result);
                }
                cs165_log(stdout, "before freeing this gch handle \n");
                free(gch);
            }
            free(to_free);
            cs165_log(stdout, "after freeing ContextNode \n");

        }
    }
    cs165_log(stdout, "before freeing buckets \n");
    free(ct->buckets);
    cs165_log(stdout, "before freeing context table \n");
    free(ct);
    
}

/**
*  Getting started hint:
* 		What other entities are context related (and contextual with respect to what scope in your design)?
* 		What else will you define in this file?
**/
