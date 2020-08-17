/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE
/**
 * client.c
 *  CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024
#define LINE_BUFFER_SIZE 1024
#define MAX_DATA_SIZE 10000000 //1e7
char* next_token(char** tokenizer, message* msg) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        msg->status = INCORRECT_FORMAT;
    }
    return token;
}

void print_result(char* variable_list, int client_socket, message* msg){
    message send_message;
    size_t variable_count = 1;
    for(size_t i=0;variable_list[i];i++){
        if(variable_list[i] == ','){
            variable_count++;
        }
    }
    char query[DEFAULT_STDIN_BUFFER_SIZE];
    sprintf(query, "print(%s)\n", variable_list);
    send_message.payload = query;
    send_message.length = strlen(query);

    if(send(client_socket, &send_message, sizeof(message), 0) == -1){
        log_err("Failed to send print meta data message header.");
        exit(1);
    }

    if(send(client_socket, send_message.payload, send_message.length, 0) == -1){
        log_err("Failed to send print meta data message payload.");
        exit(1);
    }
    
    //receive all requested data
    size_t tuples_num = 0;
    recv(client_socket, &tuples_num, sizeof(size_t), 0);
    //cs165_log(stdout, "total tuples num received from server: %zd \n", tuples_num);
    DataType* dt_list = malloc(variable_count * sizeof(DataType));
   // cs165_log(stdout, "first malloc done \n");
    void** column_list= malloc(variable_count * sizeof(void*));
    //cs165_log(stdout, "second malloc done \n");
    int* temp_int;
    double* temp_double;
    long* temp_long;
    for(size_t j=0;j<variable_count;j++){
        recv(client_socket, &(dt_list[j]), sizeof(DataType), 0);
        //cs165_log(stdout, "data type %d received from server\n", dt_list[j]);
        if(dt_list[j] == INT){
            temp_int = malloc(tuples_num * sizeof(int));
            for(size_t i=0;i<tuples_num;i++){
                recv(client_socket, &(temp_int[i]), sizeof(int), 0);
                //cs165_log(stdout, "received one int from the server\n", j);
            }
            column_list[j] = (void*) temp_int;
        }else if(dt_list[j] == FLOAT){
            temp_double = malloc(tuples_num * sizeof(double));
            for(size_t i=0;i<tuples_num;i++){
                recv(client_socket, &(temp_double[i]), sizeof(double), 0);
                //cs165_log(stdout, "received one double from the server\n", j);
            }
            column_list[j] = (void*) temp_double;
        }else{
            //dealing with long type
            temp_long = malloc(tuples_num * sizeof(long));
            for(size_t i=0;i<tuples_num;i++){
                recv(client_socket, &(temp_long[i]), sizeof(long), 0);
                //cs165_log(stdout, "received one long from the server\n", j);
            }
            column_list[j] = (void*) temp_long;
        }
    }
    
    //print out all received data
    for(size_t i=0;i<tuples_num;i++){
        for(size_t j=0;j<variable_count;j++){
            if(dt_list[j] == INT){
                temp_int = (int*) column_list[j];
                if(j != variable_count-1){
                    printf("%d,", temp_int[i]);
                }else{
                    printf("%d\n", temp_int[i]);
                }
            }else if(dt_list[j] == FLOAT){
                temp_double = (double*) column_list[j];
                if(j != variable_count-1){
                    printf("%.2f,", temp_double[i]);
                }else{
                    printf("%.2f\n", temp_double[i]);
                }
            }else{
                //dealing with long type
                temp_long = (long*) column_list[j];
                if(j != variable_count-1){
                    printf("%ld,", temp_long[i]);
                }else{
                    printf("%ld\n", temp_long[i]);
                }
            }
        }
    }
    
    free(dt_list);
    for(size_t j=0;j<variable_count;j++){
        free(column_list[j]);
    }
    free(column_list);
    msg->status = OK_DONE;
    //cs165_log(stdout, "print done \n");
}

void load_file(char* fname, int client_socket, message* msg){
    message send_message;
    FILE* f = fopen(fname, "r");
    if(f == 0){
        msg->status = FILE_NOT_FOUND;
        cs165_log(stdout, "file name: %s NOT FOUND", fname);
        return;
    }
    
    //find meta data from first line first
    char line[LINE_BUFFER_SIZE];
    char* line_p = line;
    fgets(line, LINE_BUFFER_SIZE, f);
    line_p = trim_newline(line_p);
    char full_table_name[64];
    char current_db_name[64];
    char *line_copy, *to_free;
    line_copy = to_free = malloc((strlen(line_p)+1) * sizeof(char));
    strcpy(line_copy, line);
    char* first_column_name = next_token(&line_copy, msg);
    //cs165_log(stdout, "first column name: %s\n", first_column_name);
    char* first_dot_pointer = strsep(&first_column_name, ".");
    //cs165_log(stdout, "first dot pointer string: %s\n", first_dot_pointer);
    char* second_dot_pointer = strsep(&first_column_name, ".");
    //cs165_log(stdout, "second dot pointer string: %s\n", second_dot_pointer);
    strcpy(current_db_name, first_dot_pointer);
    strcpy(full_table_name, first_dot_pointer);
    strcat(full_table_name, ".");
    strcat(full_table_name, second_dot_pointer);
    size_t col_count = 1;
    for(size_t i=0;line[i];i++){
        if(line[i] == ','){
            col_count++;
        }
    }
    //cs165_log(stdout, "get db name: %s, full table name: %s\n", current_db_name, full_table_name);
    char query[DEFAULT_STDIN_BUFFER_SIZE];
    sprintf(query, "load(%s,%s,%zu)\n", current_db_name, full_table_name, col_count);
    send_message.payload = query;
    send_message.length = strlen(query);

    if(send(client_socket, &send_message, sizeof(message), 0) == -1){
        log_err("Failed to send load meta data message header.");
        exit(1);
    }

    if(send(client_socket, send_message.payload, send_message.length, 0) == -1){
        log_err("Failed to send load meta data message payload.");
        exit(1);
    }
    
    free(to_free);
    
    //send tuples from the remaining lines next
    size_t tuples_count = 0;
    size_t capacity = MAX_DATA_SIZE;
    int** tuples = malloc(capacity * sizeof(int*));
    line_copy = NULL;
    to_free = NULL;
    while(fgets(line, LINE_BUFFER_SIZE, f) != NULL){
        line_p = line;
        line_p = trim_newline(line_p);
        line_copy = to_free = malloc((strlen(line_p)+1) * sizeof(char));
        strcpy(line_copy, line);
        int* tuple = malloc(sizeof(int) * col_count);
        int val = 0;
        for(size_t i=0;i<col_count;i++){
            char* token = next_token(&line_copy, msg);
            val = atoi(token);
            tuple[i] = val;
        }
        tuples[tuples_count] = tuple;
        tuples_count++;
        if(tuples_count > capacity){
            capacity = capacity * 2;
            tuples = realloc(tuples, capacity * sizeof(int*));
        }
        free(to_free);
    }

    //tell server how many tuples we are sending first
    if(send(client_socket, &tuples_count, sizeof(size_t), 0) == -1){
        log_err("Failed to send load meta data: tuples count");
        exit(-1);
    }
    //ask server to insert all these tuples one by one
    for(size_t i=0;i<tuples_count;i++){
        if(send(client_socket, tuples[i], sizeof(int) * col_count, 0) == -1){
            log_err("Failed to send a tuple for load");
            exit(-1);
        }
    }
    msg->status = OK_DONE;
}

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("-- Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

    log_info("-- Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

/**
 * Getting Started Hint:
 *      What kind of protocol or structure will you use to deliver your results from the server to the client?
 *      What kind of protocol or structure will you use to interpret results for final display to the user?
 *      
**/
int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;
    send_message.status = 0;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        //two special commands require additional client server communication
        if(strncmp(read_buffer, "print", 5) == 0){
            //usage: print(<vec_val1>,...)
            char variable_list[DEFAULT_STDIN_BUFFER_SIZE];
            char* read_buffer_p = read_buffer;
            read_buffer_p += 5;
            read_buffer_p = trim_newline(read_buffer_p);
            read_buffer_p = trim_parenthesis(read_buffer_p);
            strcpy(variable_list, read_buffer_p);
            print_result(variable_list, client_socket, &send_message);
            continue;
        }else if(strncmp(read_buffer, "load", 4) == 0){
            //usage1: load("/path/to/myfile.txt")
            //usage2: load("./data/myfile.txt")
            char fname[DEFAULT_STDIN_BUFFER_SIZE];
            char* read_buffer_p = read_buffer;
            read_buffer_p += 4;
            read_buffer_p = trim_newline(read_buffer_p);
            read_buffer_p = trim_parenthesis(read_buffer_p);
            read_buffer_p = trim_quotes(read_buffer_p);
            strcpy(fname, read_buffer_p);
            //all communication needed for load will be dealt with in load_file function
            load_file(fname, client_socket, &send_message);
            continue;
        } /*I have tried to put a batch mode for user that won't transmit the batched queries to the server untill user signals batch execute.
           But was not able to get it to work when we have tons of queries batched together.
           Perhaps it is related to the max payload size a socket can transmit at a time?*/

            // Only process input that is greater than 1 character.
            // Convert to message and send the message and the
            // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1) {
            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message header.");
                exit(1);
            }

            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send query payload.");
                exit(1);
            }

            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
                if ((recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) &&
                    (int) recv_message.length > 0) {
                    // Calculate number of bytes in response package
                    int num_bytes = (int) recv_message.length;
                    char payload[num_bytes + 1];

                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        payload[num_bytes] = '\0';
                        printf("%s\n", payload);
                    }
                }
            }
            else {
                if (len < 0) {
                    log_err("Failed to receive message.");
                }
                else {
                    log_info("-- Server closed connection\n");
                }
                exit(1);
            }
        }
//        if(strncmp(read_buffer, "shutdown", 8) == 0){
//            break;
//        }
    }
    close(client_socket);
    return 0;
}
