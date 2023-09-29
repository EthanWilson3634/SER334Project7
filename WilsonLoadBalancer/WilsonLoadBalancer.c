#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "LoadBalancer-1.h"

struct balancer{
    int batch_size;
    int job_count;
    struct job_node* jobs;
    pthread_mutex_t job_request_mutex;
    host* instance_host;
};

balancer* balancer_create(int batch_size){
    // Initialize the balancer
    balancer* lb = (balancer*) malloc(sizeof(balancer));
    lb->batch_size = batch_size;
    lb->job_count = 0;
    lb->jobs = NULL;
    lb->instance_host = host_create();

    pthread_mutex_init(&lb->job_request_mutex, 0);

    return lb;
}

void balancer_destroy(balancer** lb){
    if((*lb)->jobs != NULL){
        host_request_instance((*lb)->instance_host, (*lb)->jobs);
    }

    host** hPtrPtr = &(*lb)->instance_host;
    host_destroy(hPtrPtr);

    struct job_node* iterator = (*lb)->jobs;
    for(int i = 0; i < (*lb)->job_count; i++){
        free((*lb)->jobs);
        iterator = iterator->next;
        (*lb)->jobs = iterator;
    }

    free(*lb);
    *lb = NULL;
    lb = NULL;
}

/**
 * Adds a job to the load balancer. If enough jobs have been added to fill a
 * batch, will request a new instance from InstanceHost. When job is complete,
 * *data_return will be updated with the result.
 *
 * @param user_id the id of the user making the request.
 * @param data the data the user wants to process.
 * @param data_return a pointer to a location to store the result of processing.
 */
void balancer_add_job(balancer* lb, int user_id, int data, int* data_return){
    printf("LoadBalancer: Received new job from user #%d to process data=%d"
           " and store it at %p.\n", user_id, data, data_return);

    // Create the new job
    struct job_node* new_job = (struct job_node *) malloc(sizeof(struct job_node));
    new_job->user_id = user_id;
    new_job->data = data;
    new_job->data_result = data_return;
    *new_job->data_result = -1;
    new_job->next = NULL;

    // Acquire the mutex lock
    pthread_mutex_lock(&lb->job_request_mutex);

    // Add the job to the list
    if(lb->jobs == NULL){
        // This job becomes the head of the list
        lb->jobs = new_job;
    } else {
        // Add the job to the end of the list
        struct job_node* iterator = lb->jobs;
        while(iterator->next != NULL){
            iterator = iterator->next;
        }
        iterator->next = new_job;
    }

    lb->job_count++;

    // Release the mutex lock
    pthread_mutex_unlock(&lb->job_request_mutex);

    // If there are enough jobs create a new instance host
    if(lb->job_count >= lb->batch_size){
        host_request_instance(lb->instance_host, lb->jobs);

        // Reset the lb job list
        lb->job_count = 0;
        lb->jobs = NULL;
    }
}


