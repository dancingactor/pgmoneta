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

// Simple task function that increments the counter
static void
increment_counter(struct worker_common* wc)
{
   // Simulate some work
   usleep(10000);  // 10ms
   
   // Increment counter safely
   pthread_mutex_lock(&counter_mutex);
   completed_tasks++;
   pthread_mutex_unlock(&counter_mutex);
}

int
main(int argc, char** argv)
{
   struct workers* workers = NULL;
   struct worker_common task;
   int i;
   int result = 0;
   
   // Initialize logging
   pgmoneta_log_init();
   pgmoneta_log_level(PGMONETA_LOGGING_LEVEL_INFO);
   
   printf("Testing worker implementation with deque...\n");
   
   // Initialize worker pool
   if (pgmoneta_workers_initialize(NUM_WORKERS, &workers))
   {
      fprintf(stderr, "Failed to initialize workers\n");
      result = 1;
      goto error;
   }
   
   printf("Created %d workers\n", NUM_WORKERS);
   
   // Setup task
   memset(&task, 0, sizeof(struct worker_common));
   task.workers = workers;
   
   // Add multiple tasks
   printf("Adding %d tasks...\n", NUM_TASKS);
   for (i = 0; i < NUM_TASKS; i++)
   {
      if (pgmoneta_workers_add(workers, increment_counter, &task))
      {
         fprintf(stderr, "Failed to add task %d\n", i);
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
   }
   
error:
   // Clean up
   if (workers != NULL)
   {
      pgmoneta_workers_destroy(workers);
   }
   
   pgmoneta_log_shutdown();
   
   return result;
} 