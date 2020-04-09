//
// Created by Alon on 30/03/2020.
//

#ifndef EWRHT_HASHMAP_H
#define EWRHT_HASHMAP_H

#define BUCKET_SIZE 2
#define NUMBER_OF_THREADS 64
#define NOT_FOUND -1
#define FULL_BUCKET -1


#include <iostream>
#include <pthread.h>
#include <atomic>

struct BigWord{
    uint8_t Data[16];
};


template<typename Key, typename Value>
class hashmap {
    // private:
    struct Couple { // TODO: rename?
        Key key;
        Value *value; //changed to pointer to have a way of knowing if the bucket is full
    };
    enum Op_type {
        INS, DEL
    };
    enum Status_type {
        TRUE, FALSE, FAIL
    };
    struct Operation {
        Op_type type;
        Key key;
        Value value;
        int seqnum;
    };
    struct Result {
        Status_type status;
        int seqnum;
    };
    struct Tuple{
        bool status;
        Value value;
    };

    class BState {
        Couple items[BUCKET_SIZE]; // TODO: fixed size number of items is 2?
        Result results[NUMBER_OF_THREADS]; // array size n
        BigWord applied{}; // bit array of size 128

    public:
        BState() {
            for(Couple c : items){
                c.value = nullptr;
            }
        }
        BState(const BState &b) {
            for(int i = 0; i < BUCKET_SIZE; i++)
                items[i] = b.items[i];

            for(int i = 0; i < NUMBER_OF_THREADS; i++)
                results[i] = b.results[i];

            applied = b.applied;
        }
        ~BState() {
            delete[] items;
            delete[] results;
        }

    };

    struct Bucket {
        int prefix; // TODO: bit-string saved as type int?
        int depth;
        volatile std::atomic<BState*> state; //TODO: volatile means un-cacheable this is the CAS API demands, but maybe we can find a better CAS that doesn't require this since this is more exspensive
        BigWord toggle; // 128 bit array
    };

    struct DState {
        int depth;
        Bucket *dir;  // array size 2^depth
    };


    // Global variables of the class:

    DState *ht;
    Operation help[NUMBER_OF_THREADS]; // array size n
    int opSeqnum[NUMBER_OF_THREADS]; // This will be an array of size N and each each thread will only have access his memory space



    // Inner function section:

    /*** We can make this function a constant with a flag later on ***/

    //Also returns the last avialable space in the array
    int BucketFull(BState* b) {
        for(int i = 0; i < BUCKET_SIZE; i ++) {
            if (b->items[i].value == nullptr)
                return i;
        }
        return FULL_BUCKET;
    }

    int existInBucket(Couple items[], Couple c) {
        for(int i = 0; i < BUCKET_SIZE; i++) {
            if(c == items[i])
                return i;
        }
        return NOT_FOUND;
    }

    void Flip(BigWord &bitArray, unsigned int id) {
        bitArray.Data[(id/8)] |= (int8_t)(1 << (id % 8)); // I came up with this myself, I think I deserve a medal
    }

    bool bitOn(BigWord bitarray, unsigned int id){
        auto mask = (int8_t)(1 << (id % 8));
        auto temp = bitarray.Data[(id/8)];
        temp &= mask;
        return temp == mask;
    }


    void ApplyWFOp(Bucket &b, unsigned int id) {
        std::atomic<BState*> newBState_atomic;
        unsigned long oldToggle;
        BState *oldBState, *newBState;

        Flip(b.toggle, id); // mark as worked on by thread id

        for(int i = 0; i < 2; i++) {
            oldBState = b.state->load(std::memory_order_relaxed); // this is the most efficient access, it is suppose to work since the algo already covers all the cases
            newBState = new BState(oldBState);
            oldToggle = b.toggle;

            for(unsigned int j = 0; (j < NUMBER_OF_THREADS) &&  (bitOn(oldToggle, j) != bitOn(oldBState.applied, j)); j++) {
                if(newBState->results[j].seqnum < help[j].seqnum) {
                    newBState->results[j].status = ExecOnBucket(newBState, help[j]);
                    if(newBState->results[j].status != FAIL)
                        newBState->results[j].seqnum = help[j].seqnum;
                }
            }

            newBState->applied = oldToggle;
            newBState_atomic.store(newBState, std::memory_order_relaxed); //TODO: Not sure this is the the right way to store

            if(std::atomic_compare_exchange_weak<BState*>(oldBState, oldBState,  newBState_atomic))
                delete oldBState;
            else
                delete newBState;
        }
    }

    Status_type ExecOnBucket(BState *b, Operation const &op) {
        int updateID = 0;
        int freeID = 0;
        Couple *temp;
        auto *c = new Couple; //TODO: new throws exceptions? Maybe create a class for couple to make this cleaner
        c->value = new Value;
        *c->value = op.value;
        c->key = op.key;

        freeID = (BucketFull(b));
        if (freeID == FULL_BUCKET) {
            delete c->value;
            delete c;
            return FAIL;
        }

        else {
            updateID = existInBucket(b->items, c);
            if (updateID == NOT_FOUND) {
                if (op.type == INS) {
                    temp = b->items[freeID];
                    b->items[freeID] = c;
                    delete temp->value;
                    delete temp;
                }
            }
            else {
                if(op.type == INS) {
                    temp = b->items[updateID];
                    b->items[updateID] = c;
                    delete temp->value;
                    delete temp;
                }
                else {
                    delete b->items[updateID].value;
                    b->items[updateID].value = nullptr;
                }
            }
        }
    }

    Bucket *SplitBucket(Bucket const &b); // returns 2 new Buckets

    bool DirectoryUpdate(DState &d, Bucket const blist[]);

    bool ApplyPendingResize(DState &d, Bucket &bFull);

    bool ResizeWF();

    std::string Prefix(Key, int);


public:
    hashmap() { // Constructor.
        int init_depth = 0; // should we receive expected size of the hashmap in order to make less
        ht = new DState; // TODO: init DState! I think it might be easier with DState class and constructors
        //maybe init threads here not certain yet
    };

    Tuple lookup(Key const &key) const& { // return type from lookup is "const&" ?
        DState htl = ht;
        std::size_t hashed_key = std::hash<Key>(key); // we will use std hash for now
        BState bs = htl.dir[hashed_key];
        for (Couple c : bs.items) {
            if (c.key == key) return {true, *c.value};
        }
        return {false, 0}; // TODO: fix (detailed at the doc form)
    }



    enum Status_type insert(Key const &key, Value const &value, unsigned int id) {

        help[id] = {INS, key, value, ++opSeqnum[id]};
        DState htl = ht;
        std::size_t hashed_key = std::hash<Key>(key); // TODO: fix hash
        ApplyWFOp(htl.dir[hashed_key]);

        htl = ht;
        if(htl.dir[hashed_key].state->results[id].seqnum != opSeqnum[id])
            ResizeWF();
        htl = ht;
        return htl.dir[hashed_key].state->results[id].status;
    };



    bool remove(Key const &key); // delete is a saved word so I used remove

};



#endif //EWRHT_HASHMAP_H
