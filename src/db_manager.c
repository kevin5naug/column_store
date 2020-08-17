#include <string.h>
#include <sys/socket.h>
#include "cs165_api.h"
#include "client_context.h"

// In this class, there will always be only one active database at a time
Db *current_db;
ContextTable* db_catalog;

/*initially when we create a database, we reserve memory that is large enough
 to hold 100 (different types of) tables
 */
const size_t TABLE_CAPACITY=10;
const size_t INITIAL_COLUMN_LENGTH_CAPACITY=20000000;

/* 
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(char* name, size_t col_capacity, Db* db, message* msg) {
    (void) db;
    if(current_db->tables_size == current_db->tables_capacity){
        //We run out of space previously reserved for database tables
        current_db->tables=realloc(current_db->tables, current_db->tables_capacity * 2 * sizeof(Table));
        current_db->tables_capacity = current_db->tables_capacity*2;
    }

    //setup the new table
    Table *new_table = &(current_db->tables[current_db->tables_size]);
    strcpy(new_table->name, name);
    //use calloc to suppress valgrind uninitialized warning
    new_table->columns = calloc(col_capacity, sizeof(Column));
    new_table->col_count = 0;
    new_table->col_capacity = col_capacity;
    new_table->table_length = 0;
    new_table->table_length_capacity = INITIAL_COLUMN_LENGTH_CAPACITY;
    current_db->tables_size++;
    insert_context(db_catalog, name, (void*) new_table, TABLE);

    msg->status = OK_DONE;
    return new_table;
}

/* 
 * Similarly, this method is meant to create a database.
 */
void create_db(char* db_name, message* msg) {
    //check if we already have a database
    if(current_db != NULL){
        msg->status = OBJECT_ALREADY_EXISTS;
        return;
    }
    
    current_db = malloc(sizeof(Db));
    strcpy(current_db->name, db_name);
    //use calloc to suppress valgrind uninitialized warning
    current_db->tables = calloc(TABLE_CAPACITY, sizeof(Table));
    current_db->tables_size = 0;
    current_db->tables_capacity = TABLE_CAPACITY;
    db_catalog = initialize_context();
	
    msg->status = OK_DONE;
    return;
}

//TODO: Do we have to include Db* to check that we are creating columns within the right db?
Column* create_column(char* name, Db* db, Table* table, message* msg){
    (void) db;
    Column* new_column = &(table->columns[table->col_count]);
    strcpy(new_column->name, name);
    //use calloc to suppress valgrind uninitialized warning
    new_column->data = calloc(table->table_length_capacity, sizeof(int));
    new_column->size = 0;
    new_column->index_file = NULL;
    new_column->it = NONE;
    new_column->clustered = 0;
    table->col_count++;
    
    GCHandle* gch = malloc(1 * sizeof(GCHandle));
    gch->p.column = new_column;
    strcpy(gch->name, name);
    gch->type = COLUMN;
    insert_context(db_catalog, name, (void*) gch, GCOLUMN);
    msg->status = OK_DONE;
    return new_column;
}

void create_idx(IndexType it, Table* table, Column* col, message* msg){
    col->it = it;
    if(it==SORTED_CLUSTERED || it==BTREE_CLUSTERED){
        //all columns of a table follow the sort order of a leading column
        for(size_t j=0;j<table->col_count;j++){
            table->columns[j].clustered = 1;
        }
    }
    if(it==SORTED_UNCLUSTERED){
        ColumnIndex* ci = malloc(1 * sizeof(ColumnIndex));
        ci->key_vec = malloc(table->table_length_capacity * sizeof(int));
        ci->pos_vec = malloc(table->table_length_capacity * sizeof(int));
        col->index_file = (void*) ci;
    }
    msg->status = OK_DONE;
}
		    


