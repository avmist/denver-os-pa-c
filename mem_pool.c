/*
 * Created by Ivo Georgiev on 2/9/16.
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = MEM_FILL_FACTOR;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = MEM_EXPAND_FACTOR;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = MEM_FILL_FACTOR;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = MEM_EXPAND_FACTOR;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = MEM_FILL_FACTOR;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = MEM_EXPAND_FACTOR;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;



/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    // ensure that it's called only once until mem_free
    if(pool_store != NULL)
        return ALLOC_CALLED_AGAIN;
    // allocate the pool store with initial capacity
    unsigned i;
    pool_store = (pool_mgr_pt*)calloc(MEM_POOL_STORE_INIT_CAPACITY,sizeof(pool_mgr_t));
    if(!pool_store) return ALLOC_FAIL;
    pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
    return ALLOC_OK;
    // note: holds pointers only, other functions to allocate/deallocate
}

alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    if(!pool_store) return ALLOC_CALLED_AGAIN;

    // make sure all pool managers have been deallocated
    int i;
    for(i = 0; i < pool_store_size; i++){
        mem_pool_close((pool_pt)pool_store[i]);
    }
    // can free the pool store array
    free(pool_store);
    // update static variables
    pool_store = NULL;
    pool_store_size = 0;
    pool_store_capacity = 0;
    return ALLOC_OK;
}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {
    // make sure there the pool store is allocated
    if(!pool_store)
        return NULL;
    // expand the pool store, if necessary
    if(_mem_resize_pool_store() != ALLOC_OK) return NULL;
    // allocate a new mem pool mgr
    pool_mgr_pt pool_mgr = (pool_mgr_pt)calloc(1,sizeof(pool_mgr_t));
    // check success, on error return null
    if(!pool_mgr) return NULL;
    // allocate a new memory pool
    pool_mgr->pool.mem = (char*)malloc(size);
    printf("Pool allocated\n");
    // check success, on error deallocate mgr and return null
    if(!(pool_mgr->pool.mem)){
        free(pool_mgr);
        return NULL;
    }
    // allocate a new node heap
    pool_mgr->node_heap = (node_pt)calloc(MEM_NODE_HEAP_INIT_CAPACITY,sizeof(node_t));

    // check success, on error deallocate mgr/pool and return null
    if(!(pool_mgr->node_heap)){
        free(pool_mgr->pool.mem);
        free(pool_mgr);
        return NULL;
    }
    // allocate a new gap index
    pool_mgr->gap_ix = (gap_pt)calloc(MEM_GAP_IX_INIT_CAPACITY,sizeof(gap_t));
    pool_mgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;
    // check success, on error deallocate mgr/pool/heap and return null
    if(!(pool_mgr->gap_ix)){
        free(pool_mgr->node_heap);
        free(pool_mgr->pool.mem);
        free(pool_mgr);
        return NULL;
    }
    // assign all the pointers and update meta data:
    //   initialize top node of node heap
    pool_mgr->node_heap[0].next = NULL;
    pool_mgr->node_heap[0].prev = NULL;
    pool_mgr->node_heap[0].used = 1;
    pool_mgr->node_heap[0].allocated = 0;
    pool_mgr->node_heap[0].alloc_record.size = size;
    pool_mgr->node_heap[0].alloc_record.mem = pool_mgr->pool.mem;
    //   initialize top node of gap index
    _mem_add_to_gap_ix(pool_mgr,size,&(pool_mgr->node_heap[0]));
    //   initialize pool mgr
    pool_mgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
    pool_mgr->used_nodes = 1;
    pool_mgr->pool.alloc_size = size;
    pool_mgr->pool.total_size = size;
    pool_mgr->pool.num_gaps = 1;
    pool_mgr->pool.num_allocs = 0;
    pool_mgr->pool.policy = policy;
    //   link pool mgr to pool store
    pool_store_size++;
    pool_store[pool_store_size-1] = pool_mgr;
    // return the address of the mgr, cast to (pool_pt)
    return (pool_pt) pool_mgr;
}

alloc_status mem_pool_close(pool_pt pool) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt mgr = (pool_mgr_pt) pool;
    // check if this pool is allocated
    if(!mgr){
        return ALLOC_NOT_FREED;
    }
    // check if pool has only one gap
    if(mgr->pool.num_gaps == 1){
        return ALLOC_NOT_FREED;
    }
    // check if it has zero allocations
    if(mgr->pool.num_allocs != 0) return ALLOC_NOT_FREED;
    // free memory pool
    free(mgr->pool.mem);
    // free node heap
    free(mgr->node_heap);
    // free gap index
    free(mgr->gap_ix);
    // find mgr in pool store and set to null
    for(unsigned i = 0; i < pool_store_size; i++)
        if(pool_store[i] == mgr)
            pool_store[i] = NULL;
    // note: don't decrement pool_store_size, because it only grows
    // free mgr
    free(mgr);

    return ALLOC_OK;
}

alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt mgr = (pool_mgr_pt)pool;
    gap_pt gap;
    node_pt newNode = NULL;
    int newGapSize = 0;
    // check if any gaps, return null if none
    if(pool->num_gaps == 0)
        return NULL;
    // expand heap node, if necessary, quit on error
    if(_mem_resize_node_heap(mgr) != ALLOC_OK){
        return NULL;
    }
    // check used nodes fewer than total nodes, quit on error
    if(mgr->total_nodes < mgr->used_nodes){
        printf("More allocations than nodes\n");
        return NULL;
    }
    // get a node for allocation
    node_pt node = NULL;
    // if FIRST_FIT, then find the first sufficient node in the node heap
    if(mgr->pool.policy == FIRST_FIT){
        for(int i = 0; i < mgr->total_nodes; i++){
            if(mgr->node_heap[i].allocated == 0 && mgr->node_heap[i].alloc_record.size >= size){
                node = &(mgr->node_heap[i]);
                break;
            }
        }
    }
    // if BEST_FIT, then find the first sufficient node in the gap index
    else if(mgr->pool.policy == BEST_FIT){
        int best_ix = 0;
        node = mgr->gap_ix[0].node;
        for(int i = 0; i < mgr->gap_ix_capacity; i++){
            if(mgr->node_heap[i].allocated == 0 && mgr->node_heap[i].alloc_record.size >= size && mgr->node_heap[i].alloc_record.size < mgr->node_heap[best_ix].alloc_record.size){
                node = mgr->gap_ix[i].node;
                best_ix = i;
            }
        }
    }

    // check if node found
    if(!node){
        printf("Node not found!\n");
        return NULL;
    }
    // update metadata (num_allocs, alloc_size)
    mgr->pool.num_allocs++;
    mgr->pool.alloc_size += size;
    // calculate the size of the remaining gap, if any
    newGapSize = node->alloc_record.size - size;
    // remove node from gap index
    if(_mem_remove_from_gap_ix(mgr,size,node) != ALLOC_OK){
        printf("Could not remove from gap ix\n");
        return NULL;
    }
    // convert gap_node to an allocation node of given size
    node->allocated = 1;
    node->alloc_record.size = size;
    // adjust node heap:
    //   if remaining gap, need a new node
    if(newGapSize){
        //   find an unused one in the node heap
        for(int i = 0; i < mgr->total_nodes; i++){
            if(mgr->node_heap[i].used == 0){
                newNode = &(mgr->node_heap[i]);
                break;
            }
        }
        //   make sure one was found
        if(!newNode) return NULL;
        //   initialize it to a gap node
        //   update metadata (used_nodes)
        mgr->used_nodes++;

        //   update linked list (new node right after the node for allocation)
        node->next = newNode;
        newNode->prev = node;
        //   add to gap index
        if(_mem_add_to_gap_ix(mgr, newGapSize, newNode) != ALLOC_OK)
            return NULL;
        //   check if successful
    }
    // return allocation record by casting the node to (alloc_pt)
    return (alloc_pt) node;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {
    node_pt node_to_del = NULL, node_to_add = NULL, next = NULL, prev = NULL;
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt mgr = (pool_mgr_pt) pool;
    // get node from alloc by casting the pointer to (node_pt)
    node_pt node = (node_pt) alloc;
    // find the node in the node heap
    for(int i = 0; i < mgr->total_nodes; i++){
        if(&(mgr->node_heap[i]) == node){
            node_to_del = &(mgr->node_heap[i]);
            break;
        }
    }
    if(!node_to_del) return ALLOC_FAIL;
    next = node_to_del->next; prev = node_to_del->prev;
    // this is node-to-delete
    // make sure it's found
    // convert to gap node
    node_to_del->allocated = 0;
    // update metadata (num_allocs, alloc_size)
    mgr->pool.num_allocs--;
    mgr->pool.alloc_size--;
    // if the next node in the list is also a gap, merge into node-to-delete
    if(next && next->allocated == 0){
        //   remove the next node from gap index
        //   check success
        //   add the size to the `node-to-delete`
        node_to_del->alloc_record.size += next->alloc_record.size;
        //   update node as unused
        next->used = 0;
        //   update metadata (used nodes)
        mgr->used_nodes--;
        //   update linked list:
        if (node_to_del->next->next) {
            node_to_del->next->next->prev = node_to_del;
            node_to_del->next = node_to_del->next->next;
        }
        else
            node_to_del->next = NULL;
        next->next = NULL;
        next->prev = NULL;
        node_to_add = node_to_del;
    }
    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    if(prev && prev->allocated == 0){
    //   remove the previous node from gap index
        if(_mem_remove_from_gap_ix(mgr,prev->alloc_record.size,prev) != ALLOC_OK)
            return ALLOC_FAIL;
    //   check success
    //   add the size of node-to-delete to the previous
        prev->alloc_record.size += node_to_del->alloc_record.size;
    //   update node-to-delete as unused
        node_to_del->used = 0;
    //   update metadata (used_nodes)
        mgr->used_nodes--;
    //   update linked list
        if (node_to_del->next) {
            prev->next = next;
            node_to_del->next->prev = prev;
        }
        else {
            prev->next = NULL;
        }
        next = NULL;
        prev = NULL;
        //   change the node to add to the previous node!
        node_to_add = prev;
    }
    // add the resulting node to the gap index
    if(node_to_add != NULL){
        if(_mem_add_to_gap_ix(mgr,node_to_add->alloc_record.size, node_to_add) != ALLOC_OK)
            return ALLOC_FAIL;
    }
    // check success
    return ALLOC_OK;
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    // get the mgr from the pool
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;
    // allocate the segments array with size == used_nodes
    pool_segment_pt segs = calloc(pool_mgr->used_nodes, sizeof(pool_segment_t));
    // check successful
    assert(segs);
    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    for(int i = 0; i < pool_mgr->used_nodes; i++){
        segs[i].size = pool_mgr->node_heap[i].alloc_record.size;
        segs[i].allocated = pool_mgr->node_heap[i].allocated;
    }
    *segments = segs;
    *num_segments = pool_mgr->used_nodes;
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    // check if necessary
    if (((float) pool_store_size / pool_store_capacity)
        > MEM_POOL_STORE_FILL_FACTOR) {
            pool_mgr_pt* new_mgr = (pool_mgr_pt*)realloc(pool_store,pool_store_capacity*MEM_EXPAND_FACTOR*sizeof(pool_mgr_pt));
            if(!new_mgr) return ALLOC_FAIL;
            pool_store = new_mgr;
            pool_store_capacity *= MEM_EXPAND_FACTOR;
        }
    // don't forget to update capacity variables
    return ALLOC_OK;
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    // see above
    if(pool_mgr->used_nodes >= pool_mgr->total_nodes*MEM_NODE_HEAP_FILL_FACTOR){
        node_pt newNode = (node_pt)realloc(pool_mgr->node_heap,pool_mgr->total_nodes*MEM_NODE_HEAP_EXPAND_FACTOR*sizeof(node_t));
        if(!newNode) return ALLOC_FAIL;
        pool_mgr->node_heap = newNode;
        pool_mgr->total_nodes *= MEM_NODE_HEAP_EXPAND_FACTOR;
    }
    return ALLOC_OK;
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    // see above
    if(pool_mgr->used_nodes >= pool_mgr->total_nodes*MEM_GAP_IX_FILL_FACTOR){
        gap_pt newGap = (gap_pt)realloc(pool_mgr->gap_ix, pool_mgr->gap_ix_capacity*MEM_GAP_IX_EXPAND_FACTOR*sizeof(gap_t));
        if(!newGap) return ALLOC_FAIL;
        pool_mgr->gap_ix = newGap;
        pool_mgr->gap_ix_capacity *= MEM_GAP_IX_EXPAND_FACTOR;
    }
    return ALLOC_OK;
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {

    // expand the gap index, if necessary (call the function)

    if(_mem_resize_gap_ix(pool_mgr) != ALLOC_OK) return ALLOC_FAIL;

    node->used = 1;
    node->allocated = 0;
    node->alloc_record.size = size;
    // add the entry at the end
    pool_mgr->gap_ix[pool_mgr->gap_ix_capacity].node = node;
    pool_mgr->gap_ix[pool_mgr->gap_ix_capacity].size = size;
    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps++;
    // sort the gap index (call the function)
    return _mem_sort_gap_ix(pool_mgr);
    // check success
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node) {
    // find the position of the node in the gap index
    node_pt temp = NULL;
    for(int i = 0; i < pool_mgr->gap_ix_capacity; i++){
        if(pool_mgr->gap_ix[i].node == node)
            temp = pool_mgr->gap_ix[i].node;
    }
    // loop from there to the end of the array:
    for(int i = 1; i < pool_mgr->gap_ix_capacity; i++)
    //    pull the entries (i.e. copy over) one position up
        pool_mgr->gap_ix[i-1] = pool_mgr->gap_ix[i];
    //    this effectively deletes the chosen node

    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps--;
    pool_mgr->gap_ix_capacity--;
    // zero out the element at position num_gaps!
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = NULL;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size=0;
    return ALLOC_OK;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    for(int i = 1; i < pool_mgr->pool.num_gaps-1; i++){
        for(int j = 1; j < pool_mgr->pool.num_gaps-2; j++){
            //    if the size of the current entry is less than the previous (u - 1)
            if(pool_mgr->gap_ix[j].size < pool_mgr->gap_ix[j-1].size){
                //       swap them (by copying) (remember to use a temporary variable)
                gap_t gap = pool_mgr->gap_ix[j];
                pool_mgr->gap_ix[j] = pool_mgr->gap_ix[i];
                pool_mgr->gap_ix[i] = gap;
            }
        }
    }
    return ALLOC_OK;
}
