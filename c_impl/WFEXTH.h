//
// Created by Tal on 12/06/2020.
//

#ifndef C_IMPL_WFEXTH_H
#define C_IMPL_WFEXTH_H

#include <stdatomic.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "xxHash/xxhash.h"

#define SIZE_OF_BUCKET 2
#define MAX_NUM_OF_THREADS 64

enum type {INS, DEL};
enum status_ht {TRUE, FALSE, FAIL};

struct item {
    uint64_t key;
    int value;
};

struct  Operation {
    enum type type;
    uint64_t key;
    int value;
    int seqnum;
};

struct Result {
    enum status_ht status;
    int seqnum;
};

struct Bstate {
    struct item items[SIZE_OF_BUCKET];
    int applied[MAX_NUM_OF_THREADS];
    struct Result results[MAX_NUM_OF_THREADS];
};

struct Bucket {
    int prefix;
    int depth;
    struct Bstate* BState_p;
    int toggle[MAX_NUM_OF_THREADS];
};

struct Dstate {
    int depth;
    struct Bucket** dir;
};

struct tuple {
    bool status;
    int value;
};



bool insert(uint64_t key, int value, int id);

void ApplyWFOp(struct Bucket* b, int id);

enum status_ht ExecOnBucket(struct Bstate* bs, struct Operation* op);

void ResizeWF();

bool ApplyPendingResize(struct Dstate* d, struct Bucket* bFull);

void DirectoryUpdate(struct Dstate* d, struct Bucket** barr, struct Bucket* btemp);

struct Bucket** SplitBucket(struct Bucket* b);


void initHashTable();

struct tuple Lookup(uint64_t key);

#endif //C_IMPL_WFEXTH_H
