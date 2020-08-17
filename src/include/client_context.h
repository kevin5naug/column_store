#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

ContextTable* initialize_context();
void insert_context(ContextTable* ct, char* context_name, void* context_p, ContextType context_type);
void* find_context(ContextTable* ct, char* context_name, ContextType context_type);
void clean_context_table(ContextTable* ct);
#endif
