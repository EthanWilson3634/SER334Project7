#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <pthread.h>
#include "InstanceHost-1.h"

typedef struct instance{
    pthread_t thread;
    struct instance* next; // Simulate a list of instances
} instance;

struct host{
    instance* instances;
    int numInstances;
};

host* host_create(){
    // Initialize the host
    host* new_host = (host *) malloc(sizeof(host));
    new_host->instances = NULL;

    return new_host;
}

void host_destroy(host** h){
    instance* iterator = (*h)->instances;


    // Join all threads
    for(int i = 0; i < (*h)->numInstances; i++){
        pthread_join(iterator->thread, NULL);
        iterator = iterator->next;
    }

    // Free the memory allocated
    free((*h)->instances);
    free(*h);
    *h = NULL;
    h = NULL;
    iterator = NULL;
}

/**
 * Function to pass to a server (thread) to process data.
 * @param args A job_node struct containing a batch of jobs to be processed
 * @return NULL
 */
void* process_batch(void* args){
    struct job_node* jobs = (struct job_node *) args;
    while(jobs != NULL){
        *jobs->data_result = (int) pow(jobs->data, 2);
        jobs = jobs->next;
    }
    return NULL;
}

/**
* Creates a new server instance (i.e., thread) to handle processing the items
* contained in a batch (i.e., a listed list of job_node). InstanceHost will
* maintain a list of active instances, and if the host is requested to
* shutdown, ensures that all jobs are completed.
*
* @param job_batch_list A list containing the jobs in a batch to process.
*/
void host_request_instance(host* h, struct job_node* batch){
    printf("LoadBalancer: Received batch and spinning up new instance.\n");

    if(h->instances == NULL){
        h->instances = (instance *) malloc(sizeof(instance));
        pthread_create(&h->instances->thread, NULL, &process_batch, (void*) batch);
    } else {
        // Iterate to the end of the list
        instance* iterator = h->instances;
        while(iterator->next != NULL){
            iterator = iterator->next;
        }
        // Create new instance at the end of the list and pass it the process_batch function
        instance* new_instance = (instance *) malloc(sizeof(instance));
        pthread_create(&new_instance->thread, NULL, &process_batch, (void*) batch);
        // Attach the instance to the list
        iterator->next = new_instance;
    }

    // Increment the list count this variable is useful for destruction
    h->numInstances++;
}