#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"
// pthread_create() only allows one argument worker function
// mapper threads need the mapping function for each pair, the private list of
// assigned pairs, an array of shared reducer bucket pointers, an array of
// mutexes for the buckets, and the number of buckets map_reduce will handle the
// whole process. map_worker and reduce_worker will have instructions for
// individual threads
typedef struct {
  mapper_t mapper;
  kvlist_t *input;
  kvlist_t **buckets;
  pthread_mutex_t *locks;
  size_t num_reducer;
} map_args_t;
// needs the reducer function, intermediate pair bucket, and private output list
typedef struct {
  reducer_t reducer;
  kvlist_t *bucket;
  kvlist_t *output;
} reduce_args_t;

static void *map_worker(void *arg) {
  // get thread arguments
  map_args_t *args = arg;
  // private intermedaite list
  kvlist_t *intermediate = kvlist_new();
  kvlist_iterator_t *iterator = kvlist_iterator_new(args->input);
  kvpair_t *pair;
  // execute mapper
  // go through every pair and call the mapper function
  while ((pair = kvlist_iterator_next(iterator)) != NULL) {
    args->mapper(pair, intermediate);
  }
  kvlist_iterator_free(&iterator);
  // shuffle results
  iterator = kvlist_iterator_new(intermediate);
  while ((pair = kvlist_iterator_next(iterator)) != NULL) {
    // hash function on a pair's key, mod to hash into a bucket index from 0 to
    // num_reducer-1 clone pairs and append clones to the shared bucket
    size_t bucket = hash(pair->key) % args->num_reducer;
    pthread_mutex_lock(&args->locks[bucket]);
    kvlist_append(args->buckets[bucket], kvpair_clone(pair));
    pthread_mutex_unlock(&args->locks[bucket]);
  }
  // cleanup
  kvlist_iterator_free(&iterator);
  kvlist_free(&intermediate);
  return NULL;
}

static void *reduce_worker(void *arg) {
  // sort bucket by key for easier grouping
  reduce_args_t *args = arg;
  kvlist_sort(args->bucket);
  kvlist_iterator_t *iterator = kvlist_iterator_new(args->bucket);
  kvpair_t *pair = kvlist_iterator_next(iterator);
  // group identical keys
  while (pair != NULL) {
    char *key = pair->key;
    // key for currentg roup and empty list to hold pairs with that key
    kvlist_t *group = kvlist_new();
    // add matching pairs to group
    do {
      kvlist_append(group, kvpair_clone(pair));
      pair = kvlist_iterator_next(iterator);
      // make sure we haven't hit the bottom of the bucket and next pair is
      // still the same key
    } while (pair != NULL && strcmp(pair->key, key) == 0);
    // call reducer for each key group
    args->reducer(key, group, args->output);
    kvlist_free(&group);
  }
  // clean up iterator when finishedx
  kvlist_iterator_free(&iterator);
  return NULL;
}
void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t *input, kvlist_t *output) {
  // prevent possible division by zero and zero length arrays
  if (num_mapper == 0 || num_reducer == 0) {
    return;
  }
  // declare arrays
  // input list per mapper
  kvlist_t *splits[num_mapper];
  // shuffle list per reducer
  kvlist_t *buckets[num_reducer];
  // private output lists per reducer
  kvlist_t *results[num_reducer];
  // mutex per shuffle bucket
  pthread_mutex_t locks[num_reducer];
  // mapper thread ids
  pthread_t map_threads[num_mapper];
  // reducer thread ids
  pthread_t reducer_threads[num_reducer];
  map_args_t map_args[num_mapper];
  reduce_args_t reduce_args[num_reducer];
  // initialize the lists and mutexes
  for (size_t i = 0; i < num_mapper; i++) {
    splits[i] = kvlist_new();
  }
  for (size_t i = 0; i < num_reducer; i++) {
    buckets[i] = kvlist_new();
    results[i] = kvlist_new();
    // initialize a mutex for each reducer bucket
    if (pthread_mutex_init(&locks[i], NULL) != 0) {
      fprintf(stderr, "map_reduce: mutex fail\n");
      exit(EXIT_FAILURE);
    }
  }
  kvlist_iterator_t *iterator = kvlist_iterator_new(input);
  kvpair_t *pair;
  size_t index = 0;
  // loop through the original input list and on each pair mod num_mapper to
  // distribute pairs round robin clone to not change the callers input
  while ((pair = kvlist_iterator_next(iterator)) != NULL) {
    kvlist_append(splits[index % num_mapper], kvpair_clone(pair));
    index++;
  }
  kvlist_iterator_free(&iterator);
  // create mapper threads and fill argument structure
  for (size_t i = 0; i < num_mapper; i++) {
    map_args[i].mapper = mapper;
    map_args[i].input = splits[i];
    map_args[i].buckets = buckets;
    map_args[i].locks = locks;
    map_args[i].num_reducer = num_reducer;
    // where to store thread id, defualt thread attributes, function that
    // executes in thread, arguments for particular thread
    if (pthread_create(&map_threads[i], NULL, map_worker, &map_args[i]) != 0) {
      fprintf(stderr, "map_reduce: mapper thread failed\n");
      exit(EXIT_FAILURE);
    }
  }
  // join mapper threads
  for (size_t i = 0; i < num_mapper; i++) {
    pthread_join(map_threads[i], NULL);
  }
  // create reducer threads with one bucket and one private result list
  for (size_t i = 0; i < num_reducer; i++) {
    reduce_args[i].reducer = reducer;
    reduce_args[i].bucket = buckets[i];
    reduce_args[i].output = results[i];
    if (pthread_create(&reducer_threads[i], NULL, reduce_worker,
                       &reduce_args[i]) != 0) {
      fprintf(stderr, "map_reduce: reducer thread failed\n");
      exit(EXIT_FAILURE);
    }
  }
  // join reducer threads
  for (size_t i = 0; i < num_reducer; i++) {
    pthread_join(reducer_threads[i], NULL);
  }
  // combine output and clean
  for (size_t i = 0; i < num_reducer; i++) {
    // move pairs from results[i] to caller output list
    kvlist_extend(output, results[i]);

    kvlist_free(&results[i]);
    kvlist_free(&buckets[i]);
    pthread_mutex_destroy(&locks[i]);
  }
  for (size_t i = 0; i < num_mapper; i++) {
    kvlist_free(&splits[i]);
  }
}
