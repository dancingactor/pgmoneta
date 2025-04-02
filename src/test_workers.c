/*
 * Copyright (C) 2025 The pgmoneta community
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or other
 * materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may
 * be used to endorse or promote products derived from this software without specific
 * prior written permission.
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

#define NUM_WORKERS 4
#define NUM_TASKS 100

// Shared counter for task verification
static int completed_tasks = 0;
static pthread_mutex_t counter_mutex = PTHREAD_MUTEX_INITIALIZER;

// Test data structure to pass to tasks
typedef struct {
    struct worker_common common;
    int task_id;
} test_task_data;

// Simple task function that increments the counter
static void
increment_counter(struct worker_common* wc)
{
   // Check for NULL pointer
   if (wc == NULL)
   {
      fprintf(stderr, "ERROR: NULL worker_common pointer in task function\n");
      return;
   }

   // Cast to our test data structure
   test_task_data* data = (test_task_data*)wc;
   
   // Simulate some work
   usleep(10000);  // 10ms
   
   // Increment counter safely
   pthread_mutex_lock(&counter_mutex);
   completed_tasks++;
   printf("Task %d completed, total: %d\n", data->task_id, completed_tasks);
   pthread_mutex_unlock(&counter_mutex);
   
   // Free the task data that was allocated for this task
   free(data);
}

int
main(int argc, char** argv)
{
   struct workers* workers = NULL;
   test_task_data* task_data = NULL;
   int i;
   int result = 0;
   
   printf("Starting test...\n");

   // Skip logging initialization for basic testing
   // This is often a source of errors if logging system isn't fully set up
   printf("Skipping logging initialization for basic tests\n");
   
   printf("Testing worker implementation (with internal deque)...\n");
   
   // Initialize worker pool
   printf("Initializing worker pool with %d workers...\n", NUM_WORKERS);
   if (pgmoneta_workers_initialize(NUM_WORKERS, &workers))
   {
      fprintf(stderr, "Failed to initialize workers\n");
      result = 1;
      goto error;
   }
   
   if (workers == NULL)
   {
      fprintf(stderr, "Worker initialization returned success but workers is NULL\n");
      result = 1;
      goto error;
   }
   
   printf("Created %d workers successfully\n", NUM_WORKERS);
   
   // Reset completed tasks counter
   completed_tasks = 0;
   
   // Add just a few tasks first to test basic functionality
   printf("Adding 5 test tasks to worker queue...\n");
   for (i = 0; i < 5; i++)
   {
      // Allocate a new task data structure for each task
      task_data = (test_task_data*)malloc(sizeof(test_task_data));
      if (task_data == NULL)
      {
         fprintf(stderr, "Failed to allocate task data\n");
         result = 1;
         goto error;
      }
      
      // Initialize task data
      memset(task_data, 0, sizeof(test_task_data));
      task_data->common.workers = workers;
      task_data->common.function = increment_counter;
      task_data->task_id = i;
      
      printf("Adding task %d...\n", i);
      // Add task to worker queue (which internally uses deque)
      if (pgmoneta_workers_add(workers, increment_counter, &task_data->common))
      {
         fprintf(stderr, "Failed to add task %d\n", i);
         free(task_data);
         result = 1;
         goto error;
      }
      printf("Task %d added successfully\n", i);
   }
   
   // Wait for all tasks to complete
   printf("Waiting for initial 5 tasks to complete...\n");
   pgmoneta_workers_wait(workers);
   
   // Check if all tasks completed
   printf("Tasks completed: %d (expected: 5)\n", completed_tasks);
   if (completed_tasks == 5)
   {
      printf("SUCCESS: Initial test tasks completed successfully\n");
   }
   else
   {
      fprintf(stderr, "ERROR: Only %d of 5 tasks completed\n", completed_tasks);
      result = 1;
      goto error;
   }

   // Continue with more tests if the basic functionality works
   printf("\nBasic functionality test passed, continuing with full test suite...\n");

   // Reset the counter
   completed_tasks = 0;

   // Now try the original test with more tasks
   printf("Adding %d tasks to worker queue...\n", NUM_TASKS);
   for (i = 0; i < NUM_TASKS; i++)
   {
      // Allocate a new task data structure for each task
      task_data = (test_task_data*)malloc(sizeof(test_task_data));
      if (task_data == NULL)
      {
         fprintf(stderr, "Failed to allocate task data\n");
         result = 1;
         goto error;
      }
      
      // Initialize task data
      memset(task_data, 0, sizeof(test_task_data));
      task_data->common.workers = workers;
      task_data->common.function = increment_counter;
      task_data->task_id = i;
      
      // Add task to worker queue (which internally uses deque)
      if (pgmoneta_workers_add(workers, increment_counter, &task_data->common))
      {
         fprintf(stderr, "Failed to add task %d\n", i);
         free(task_data);
         result = 1;
         goto error;
      }
   }
   
   // Wait for all tasks to complete
   printf("Waiting for tasks to complete...\n");
   pgmoneta_workers_wait(workers);
   
   // Check if all tasks completed
   if (completed_tasks == NUM_TASKS)
   {
      printf("SUCCESS: All %d tasks completed successfully\n", completed_tasks);
   }
   else
   {
      fprintf(stderr, "ERROR: Only %d of %d tasks completed\n", completed_tasks, NUM_TASKS);
      result = 1;
      goto error;
   }
   
   // Test worker_input creation
   struct worker_input* wi = NULL;
   printf("Testing worker_input creation...\n");
   if (pgmoneta_create_worker_input("testdir", "source", "destination", 5, workers, &wi))
   {
      fprintf(stderr, "Failed to create worker input\n");
      result = 1;
      goto error;
   }
   
   if (wi == NULL)
   {
      fprintf(stderr, "Worker input creation returned success but wi is NULL\n");
      result = 1;
      goto error;
   }

   // Verify worker_input was created correctly
   if (strcmp(wi->directory, "testdir") != 0 ||
       strcmp(wi->from, "source") != 0 ||
       strcmp(wi->to, "destination") != 0 ||
       wi->level != 5 || 
       wi->common.workers != workers)
   {
      fprintf(stderr, "Worker input creation test failed - incorrect values\n");
      result = 1;
      goto error;
   }
   
   printf("Worker input creation test passed\n");
   
   // Test worker pool under stress
   printf("\nTesting worker pool under stress with many short tasks...\n");
   
   // Reset counter
   completed_tasks = 0;
   
   // Add many short tasks
   for (i = 0; i < NUM_TASKS * 2; i++)
   {
      task_data = (test_task_data*)malloc(sizeof(test_task_data));
      if (task_data == NULL)
      {
         fprintf(stderr, "Failed to allocate task data during stress test\n");
         result = 1;
         goto error;
      }
      
      memset(task_data, 0, sizeof(test_task_data));
      task_data->common.workers = workers;
      task_data->common.function = increment_counter;
      task_data->task_id = i + NUM_TASKS;
      
      if (pgmoneta_workers_add(workers, increment_counter, &task_data->common))
      {
         fprintf(stderr, "Failed to add task %d during stress test\n", i);
         free(task_data);
         result = 1;
         goto error;
      }
   }
   
   pgmoneta_workers_wait(workers);
   
   if (completed_tasks == NUM_TASKS * 2)
   {
      printf("SUCCESS: Stress test completed, all %d tasks processed\n", NUM_TASKS * 2);
   }
   else
   {
      fprintf(stderr, "ERROR: Stress test failed, only %d of %d tasks completed\n", 
              completed_tasks, NUM_TASKS * 2);
      result = 1;
   }
   
   printf("\nAll tests completed.\n");
   
error:
   // Free worker input if allocated
   if (wi != NULL)
   {
      free(wi);
   }
   
   // Clean up workers
   if (workers != NULL)
   {
      printf("Destroying worker pool...\n");
      pgmoneta_workers_destroy(workers);
      printf("Worker pool destroyed\n");
   }
   
   // Skip logging shutdown for basic testing
   printf("Test completed with result: %s\n", result == 0 ? "SUCCESS" : "FAILURE");
   
   return result;
} 