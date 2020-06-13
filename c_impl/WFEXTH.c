
#include "WFEXTH.h"
#include "xxHash/xxhash.c"
#define MAGIC_NUMBER 0xffabcd
#define MAX_TABLE_SIZE 64

struct Operation* help[MAX_NUM_OF_THREADS] = {0};
atomic_int opSeqnum[MAX_NUM_OF_THREADS] = {0};
volatile struct Dstate* ht = {0};

static uint64_t Prefix(uint64_t const key, int const depth) {
    int shift = 64 - depth;
    uint64_t mask = ~(uint64_t)((1 << (shift)) - 1);
    uint64_t prefix = (uint64_t)((key & mask) >> shift);
    return prefix;
}

static void initBstate(struct Bstate* bs) {
    memset(bs->applied, 0, sizeof(bool)*64);
    bs->items[0].key = MAGIC_NUMBER;
    bs->items[1].key = MAGIC_NUMBER;
}

static void initBucket(struct Bucket* b, int prefix){
    memset(b->toggle,0, sizeof(bool)*MAX_NUM_OF_THREADS);
    b->depth = 1;
    b->prefix = prefix;
    b->BState_p = malloc(sizeof(struct Bstate));
    initBstate(b->BState_p);
}

void initHashTable(){
    ht = malloc(sizeof(struct Dstate));
    ht->depth = 1;
    ht->dir = malloc(sizeof(struct Bucket*)*2);
    ht->dir[0] = malloc(sizeof(struct Bucket));
    ht->dir[1] = malloc(sizeof(struct Bucket));
    initBucket(ht->dir[0], 0);
    initBucket(ht->dir[1], 1);
}

struct tuple Lookup(uint64_t key) {
    volatile struct Dstate* htl = ht;
    key = XXH64(&key, sizeof(key), 7);
    struct Bstate* bs = htl->dir[Prefix(key, htl->depth)]->BState_p;
    struct tuple ret = {false, -1};
    for(int i = 0; i < SIZE_OF_BUCKET; i++) {
        if(bs->items[i].key == key){
            ret.value = bs->items[i].value;
            ret.status = true;
            return ret;
        }
    }
    return ret;
}

bool insert(uint64_t key, int value, int id) {
insert_again:
    opSeqnum[id]++;
    key = XXH64(&key, sizeof(key), 7);
    assert(key != MAGIC_NUMBER);
    struct Operation* op = malloc(sizeof(struct Operation));
    struct Operation oper = { .type =INS, .key =key, .value = value, .seqnum =opSeqnum[id]};

    memcpy(op, &oper, sizeof(struct Operation));
    help[id] = op;
    volatile struct Dstate* htl = ht;

    ApplyWFOp(htl->dir[Prefix(key, htl->depth)], id);

    htl = ht;
    if(htl->dir[Prefix(key, htl->depth)]->BState_p->results[id].seqnum != opSeqnum[id])
        ResizeWF();

    htl = ht;

    bool succuess = (htl->dir[Prefix(key,htl->depth)]->BState_p->results[id].status == TRUE);
    if(succuess)
        return succuess;
    else
        goto insert_again;
}

static void copyBstate(struct Bstate* oldb, struct Bstate* newb) {
    for(int i = 0; i < MAX_NUM_OF_THREADS; i++) {
        newb->results[i] = oldb->results[i];
        newb->applied[i] = oldb->applied[i];
    }

    for(int i = 0; i < SIZE_OF_BUCKET; i++) {
        newb->items[i] = oldb->items[i];
    }
}

void copyToggle(bool newtoggle[MAX_NUM_OF_THREADS], const bool origtoggle[MAX_NUM_OF_THREADS]) {
    for(int i = 0; i < MAX_NUM_OF_THREADS; i++) {
        newtoggle[i] = origtoggle[i];
    }
}

static void copyBucket(struct Bucket* oldb, struct Bucket* newb) {
    newb->BState_p = oldb->BState_p;
    newb->depth = oldb->depth;
    newb->prefix = oldb->prefix;
    copyToggle(newb->toggle, oldb->toggle);
}

void ApplyWFOp(struct Bucket* b, int id) {
    b->toggle[id] = !b->toggle[id];
    bool t[MAX_NUM_OF_THREADS] = {0};
    for(int k = 0; k < 2; k++) {
        struct Bstate* oldb = b->BState_p;
        struct Bstate* newb = malloc(sizeof(struct Bstate));
        copyBstate(oldb, newb);
        copyToggle(t, b->toggle);

        for(int i = 0; i < MAX_NUM_OF_THREADS; i++) {
            if(t[i] == newb->applied[i]) continue;
            if(help[i] && newb->results[i].seqnum < help[i]->seqnum) {
                newb->results[i].status = ExecOnBucket(newb, help[i]);
                if(newb->results[i].status != FAIL) {
                    newb->results[i].seqnum = help[i]->seqnum;
                }
            }
        }
        copyToggle(newb->applied, t);
        atomic_compare_exchange_strong(&b->BState_p, &oldb, newb);
    }
}

static bool isBstateFull(struct Bstate* bs) {
    for(int i = 0; i < SIZE_OF_BUCKET; i++) {
        if(bs->items[i].key == MAGIC_NUMBER)
            return false;
    }
    return true;
}

static int getItemIndex(struct Bstate* bs, uint64_t key) {
    for(int i = 0; i < SIZE_OF_BUCKET; i++) {
        if (bs->items[i].key == key)
            return i;
    }
    for(int i = 0; i < SIZE_OF_BUCKET; i++) {
        if(bs->items[i].key == MAGIC_NUMBER)
            return i;
    }
}

enum status_ht ExecOnBucket(struct Bstate* bs, struct Operation* op) {
    if(isBstateFull(bs))
        return FAIL;
    else {
        int exist = getItemIndex(bs, op->key);
        if(op->type == INS) {
            bs->items[exist].value = op->value;
            bs->items[exist].key = op->key;
            return TRUE;
        }
        else {
            bs->items[exist].key = MAGIC_NUMBER;
            return TRUE;
        }
    }
}

static volatile struct Dstate* CopyDstate(volatile struct Dstate* newD, volatile struct Dstate* oldD) {

    newD->depth = oldD->depth;
    int size = 1 << newD->depth;
    newD->dir = malloc(sizeof(struct Bucket*)*(size));
    for(int i = 0; i < size; i++) {
        newD->dir[i] = oldD->dir[i];
    }
    return newD;
}

void ResizeWF() {
    for(int k = 0; k < 2; k ++) {
        volatile struct Dstate* oldD = ht;
        volatile struct Dstate* newD;
        newD = malloc(sizeof(struct Dstate));
        newD = CopyDstate(newD, oldD);

        for(int j = 0; j < MAX_NUM_OF_THREADS; j++) {
            if(!help[j])
                continue;
            struct Bucket* b = newD->dir[Prefix(help[j]->key, newD->depth)];
            if(isBstateFull(b->BState_p) && b->BState_p->results[j].seqnum < help[j]->seqnum){
                ApplyPendingResize((struct Dstate *) newD, b);
            }
        }
        atomic_compare_exchange_strong(&ht, &oldD, newD);
    }
}

void ApplyPendingResize(struct Dstate* d, struct Bucket* bFull) {
    for(int j = 0; j < MAX_NUM_OF_THREADS; j++) {
        if(help[j] && Prefix(help[j]->key, bFull->depth) == bFull->prefix) {
            if(bFull->BState_p->results[j].seqnum < help[j]->seqnum) {
                struct Bucket* bdest = d->dir[Prefix(help[j]->key, d->depth)];
                while(isBstateFull(bdest->BState_p)) {
                    struct Bucket** buckarr;
                    buckarr = SplitBucket(bdest);
                    DirectoryUpdate(d, buckarr);
                    bdest = d->dir[Prefix(help[j]->key, d->depth)];
                }
                bdest->BState_p->results[j].status = ExecOnBucket(bdest->BState_p, help[j]);
                bdest->BState_p->results[j].seqnum = help[j]->seqnum;
            }
        }
    }
}

struct Bucket** SplitBucket(struct Bucket* b ) {
    struct Bucket** buckarr = malloc(sizeof(struct Bucket*)*2);

    buckarr[0] = malloc(sizeof(struct Bucket));
    buckarr[1] = malloc(sizeof(struct Bucket));
    copyBucket(b, buckarr[0]);

    buckarr[0]->depth = b->depth + 1;
    buckarr[0]->prefix = b->prefix << 1;

    buckarr[0]->BState_p = malloc(sizeof(struct Bstate));
    for(int i = 0; i < SIZE_OF_BUCKET; i++) {
        buckarr[0]->BState_p->items[i].key = MAGIC_NUMBER;
    }
    for(int i = 0; i < MAX_NUM_OF_THREADS; i++) {
        buckarr[0]->BState_p->results[i] = b->BState_p->results[i];
        buckarr[0]->BState_p->applied[i] = buckarr[0]->toggle[i];
    }

    copyBucket(buckarr[0], buckarr[1]);
    buckarr[1]->BState_p = malloc(sizeof(struct Bstate));
    copyBstate(buckarr[0]->BState_p, buckarr[1]->BState_p);
    buckarr[1]->prefix = buckarr[0]->prefix + 1;


    //split items -----
    int index1 = 0;
    int index2 = 0;
    for(int i = 0; i < SIZE_OF_BUCKET; i++) {
        if(Prefix(b->BState_p->items[i].key, buckarr[0]->depth) == buckarr[0]->prefix) {
            buckarr[0]->BState_p->items[index1].key = b->BState_p->items[i].key;
            buckarr[0]->BState_p->items[index1].value = b->BState_p->items[i].value;
            index1++;
        }
        else {
            buckarr[1]->BState_p->items[index2].key = b->BState_p->items[i].key;
            buckarr[1]->BState_p->items[index2].value = b->BState_p->items[i].value;
            index2++;
        }
    }
    return buckarr;
}


void DirectoryUpdate(struct Dstate* d, struct Bucket** barr) {
    for(int i = 0; i < 2 ; i++) {
        if( barr[i]->depth > d->depth) {
            //double dir size
            int size = 1 << (d->depth + 1);
            int indexold = 0;
            struct Bucket**  newDir = malloc(sizeof(struct Bucket*)* size);
            for(uint64_t j = 0; j < size; j++) {
                if(Prefix(j << (MAX_TABLE_SIZE - (d->depth +1)), d->dir[indexold]->depth) == d->dir[indexold]->prefix) {
                    newDir[j] = d->dir[indexold];
                }
                else {
                    indexold++;
                    j--;
                }
            }
            struct Bucket**  temp = d->dir;
            d->dir = newDir;
            d->depth++;
            free(temp);
        }
        //insert new buckets
        for(uint64_t j = 0; j < 1 << d->depth; j ++) {
            if(Prefix(j << (MAX_TABLE_SIZE - d->depth), barr[i]->depth) == barr[i]->prefix)
                d->dir[j] = barr[i];
        }
    }
}

