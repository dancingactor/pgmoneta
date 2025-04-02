/*
 * Copyright (C) 2025 The pgmoneta community
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list
 *    of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this
 *    list of conditions and the following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may
 *    be used to endorse or promote products derived from this software without specific
 *    prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <pgmoneta.h>
#include <workers.h>
#include <logging.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#define NUM_WORKERS 4
#define NUM_TASKS 100

// Global counter for task verification.
static int completed_tasks = 0;
static pthread_mutex_t counter_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Test task data structure.
 * Note: It embeds struct worker_common and has an extra task_id field.
 * To copy the full structure (not just the worker_common part), we provide
 * our own add function wrapper.
 */
typedef struct {
    struct worker_common common;
    int task_id;
} test_task_data;

/*
 * Simple task function that increments the counter.
 * Do not free the task data here because the deque's destroy callback (destroy_data_wrapper)
 * will free it.
 */
static void increment_counter(struct worker_common* wc)
{
   test_task_data* data = (test_task_data*)wc;
   
   // Simulate some work.
   usleep(10000);  // 10ms
   
   pthread_mutex_lock(&counter_mutex);
   completed_tasks++;
   printf("Task %d completed, total: %d\n", data->task_id, completed_tasks);
   pthread_mutex_unlock(&counter_mutex);
}

/*
 * Wrapper for adding a task that allocates and copies the full structure.
 */
int test_pgmoneta_workers_add(struct workers* workers, 
                              void (*function)(struct worker_common*), 
                              struct worker_common* wc, 
                              size_t size)
{
   struct worker_common* task_wc = malloc(size);
   if (task_wc == NULL)
   {
      pgmoneta_log_error("Could not allocate memory for task");
      return 1;
   }
   
   memcpy(task_wc, wc, size);
   task_wc->function = function;
   
   struct value_config config = {0};
   config.destroy_data = destroy_data_wrapper; // Use the same destroy callback as in workers.c
   pgmoneta_deque_add_with_config(workers->queue, NULL, (uintptr_t)task_wc, &config);
   
   semaphore_post(workers->has_tasks);
   
   return 0;
}

/* 
 * Forward declarations for functions defined in workers.c.
 * (They must be accessible to our wrapper.)
 */
extern void semaphore_post(struct semaphore* semaphore);
extern void destroy_data_wrapper(uintptr_t data);

int main(void)
{
   struct workers* workers = NULL;
   int result = 0;
   int i;
   test_task_data* task_data = NULL;
   
   printf("Starting test...\n");

   printf("Initializing worker pool with %d workers...\n", NUM_WORKERS);
   if (pgmoneta_workers_initialize(NUM_WORKERS, &workers))
   {
      fprintf(stderr, "Failed to initialize workers\n");
      return 1;
   }
   if (workers == NULL)
   {
      fprintf(stderr, "Worker initialization returned success but workers is NULL\n");
      return 1;
   }
   printf("Created %d workers successfully\n", NUM_WORKERS);
   completed_tasks = 0;
   
   // Add a few tasks for a basic functionality test.
   printf("Adding 5 test tasks...\n");
   for (i = 0; i < 5; i++)
   {
      task_data = malloc(sizeof(test_task_data));
      if (task_data == NULL)
      {
         fprintf(stderr, "Failed to allocate task data\n");
         return 1;
      }
      
      memset(task_data, 0, sizeof(test_task_data));
      task_data->common.workers = workers;
      task_data->common.function = increment_counter;
      task_data->task_id = i;
      
      printf("Adding task %d...\n", i);
      if (test_pgmoneta_workers_add(workers, increment_counter, &task_data->common, sizeof(test_task_data)))
      {
         fprintf(stderr, "Failed to add task %d\n", i);
         free(task_data);
         return 1;
      }
      printf("Task %d added successfully\n", i);
   }
   
   printf("Waiting for initial tasks to complete...\n");
   pgmoneta_workers_wait(workers);
   
   printf("Tasks completed: %d (expected: 5)\n", completed_tasks);
   if (completed_tasks != 5)
   {
      fprintf(stderr, "ERROR: Only %d of 5 tasks completed\n", completed_tasks);
      result = 1;
      goto cleanup;
   }
   printf("Initial test passed\n");
   
   // Reset counter and add more tasks.
   completed_tasks = 0;
   printf("Adding %d tasks...\n", NUM_TASKS);
   for (i = 0; i < NUM_TASKS; i++)
   {
      task_data = malloc(sizeof(test_task_data));
      if (task_data == NULL)
      {
         fprintf(stderr, "Failed to allocate task data\n");
         return 1;
      }
      
      memset(task_data, 0, sizeof(test_task_data));
      task_data->common.workers = workers;
      task_data->common.function = increment_counter;
      task_data->task_id = i;
      
      if (test_pgmoneta_workers_add(workers, increment_counter, &task_data->common, sizeof(test_task_data)))
      {
         fprintf(stderr, "Failed to add task %d\n", i);
         free(task_data);
         return 1;
      }
   }
   
   printf("Waiting for tasks to complete...\n");
   pgmoneta_workers_wait(workers);
   
   if (completed_tasks != NUM_TASKS)
   {
      fprintf(stderr, "ERROR: Only %d of %d tasks completed\n", completed_tasks, NUM_TASKS);
      result = 1;
      goto cleanup;
   }
   printf("All %d tasks completed successfully\n", NUM_TASKS);
   
   // Test worker_input creation.
   {
      struct worker_input* wi = NULL;
      printf("Testing worker_input creation...\n");
      if (pgmoneta_create_worker_input("testdir", "source", "destination", 5, workers, &wi))
      {
         fprintf(stderr, "Failed to create worker input\n");
         result = 1;
         goto cleanup;
      }
      
      if (wi == NULL)
      {
         fprintf(stderr, "Worker input creation returned success but wi is NULL\n");
         result = 1;
         goto cleanup;
      }
      
      if (strcmp(wi->directory, "testdir") != 0 ||
          strcmp(wi->from, "source") != 0 ||
          strcmp(wi->to, "destination") != 0 ||
          wi->level != 5 ||
          wi->common.workers != workers)
      {
         fprintf(stderr, "Worker input creation test failed - incorrect values\n");
         result = 1;
         goto cleanup;
      }
      printf("Worker input creation test passed\n");
      free(wi);
   }
   
   // Stress test: many short tasks.
   completed_tasks = 0;
   printf("Adding %d tasks for stress test...\n", NUM_TASKS * 2);
   for (i = 0; i < NUM_TASKS * 2; i++)
   {
      task_data = malloc(sizeof(test_task_data));
      if (task_data == NULL)
      {
         fprintf(stderr, "Failed to allocate task data during stress test\n");
         result = 1;
         goto cleanup;
      }
      
      memset(task_data, 0, sizeof(test_task_data));
      task_data->common.workers = workers;
      task_data->common.function = increment_counter;
      task_data->task_id = i + NUM_TASKS;
      
      if (test_pgmoneta_workers_add(workers, increment_counter, &task_data->common, sizeof(test_task_data)))
      {
         fprintf(stderr, "Failed to add task %d during stress test\n", i);
         free(task_data);
         result = 1;
         goto cleanup;
      }
   }
   
   pgmoneta_workers_wait(workers);
   
   if (completed_tasks != NUM_TASKS * 2)
   {
      fprintf(stderr, "ERROR: Only %d of %d tasks completed during stress test\n",
              completed_tasks, NUM_TASKS * 2);
      result = 1;
   }
   else
   {
      printf("Stress test passed, all tasks processed\n");
   }
   
cleanup:
   if (workers)
   {
      printf("Destroying worker pool...\n");
      pgmoneta_workers_destroy(workers);
      printf("Worker pool destroyed\n");
   }
   
   printf("Test completed with result: %s\n", result == 0 ? "SUCCESS" : "FAILURE");
   
   return result;
}
