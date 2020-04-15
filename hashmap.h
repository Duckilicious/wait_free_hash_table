//
// Created by Alon on 30/03/2020.
//

#ifndef EWRHT_HASHMAP_H
#define EWRHT_HASHMAP_H

#define BUCKET_SIZE (2)
#define NUMBER_OF_THREADS (64)
#define NOT_FOUND (-1)
#define FULL_BUCKET (-1)
#define BIGWORD_SIZE (16)
#define POW(exp) ((unsigned)2 << (unsigned)(exp))

#include <iostream>
#include <pthread.h>
#include <atomic>
#include <cassert>

template<typename Key, typename Value>
class hashmap {
    // private:
    struct Triple {
        size_t hash;
        Key key;
        Value value;
    };

    enum Op_type {
        INS, DEL
    };

    enum Status_type {
        TRUE, FALSE, FAIL
    };

    class Operation {
    public:
        Op_type type;
        Key key;
        Value value;
        int seqnum;
        size_t hash;

        Operation(Op_type t, Key k, Value v, int seq, size_t h) :
            type(t), key(k), value(v), seqnum(seq), hash(h) {};
    };

    struct Result {
        Status_type status;
        int seqnum;
    };

    class BigWord {
        uint8_t Data[BIGWORD_SIZE];
        BigWord() : Data() {}

        BigWord(BigWord const& b) : Data() {
            for (int i = 0; i < BIGWORD_SIZE; ++i) {
                this->Data[i] = b.Data[i];
            }
        }

        BigWord operator=(BigWord b) = delete;

        ~BigWord() = default;

        void setBit(unsigned int id) {
            Data[id/8] |= (int8_t)(1 << (id % 8));
        }

        void clearBit(unsigned int id){
            Data[id/8] &= (uint8_t)~(1 << (id % 8));
        }

        bool testBit(unsigned int id) const {
            return ((Data[id/8] & (1 << (id % 8))) != 0);
        }
    };

    class BState {
        Triple *items[BUCKET_SIZE]; // if pointer = nullptr place is free
        Result results[NUMBER_OF_THREADS]; // array size n
        BigWord applied{}; // bit array of size 128

    public:
        BState() : items(), results(), applied() {}
        BState(const BState &b, bool with_items = true) : items() { // copy constructor
            if (with_items) {
                for (int i = 0; i < BUCKET_SIZE; i++)
                    this->items[i] = b.items[i];
            }

            for(int i = 0; i < NUMBER_OF_THREADS; i++)
                this->results[i] = b.results[i];

            this->applied(b.applied);
        }
        BState operator=(BState b) = delete; // we can implement assignment constructor if we will need to, but it cannot be the default so for now so it's blocked

        bool insertItem(Triple const *t) {
            for (Triple* &item : items) {
                if (!item) {
                    item = &t;
                    return true;
                }
            }
            return false; // bucket is full
        }

        int BucketFull() const { //Returns the free entry if it exists, else -1
            for (int i = 0; i < BUCKET_SIZE; i ++) {
                if (this->items[i] == nullptr)
                    return i;
            }
            return FULL_BUCKET;
        }

        ~BState() = default;
    };

    class Bucket {
        int prefix;
        size_t depth;
        volatile std::atomic<BState*> state; //TODO: volatile means un-cacheable this is the CAS API demands, but maybe we can find a better CAS that doesn't require this since this is more exspensive
        BigWord toggle; // 128 bit array

    public:
        Bucket() : prefix(), depth(), state(std::atomic<int>(new BState())), toggle() {}

        Bucket(const Bucket &b) : prefix(b.prefix), depth(b.depth),
            state(std::atomic<BState*>(new BState(*(b->state->load(std::memory_order_relaxed))))),
            toggle(b.toggle) { } // copy constructor

        Bucket operator=(Bucket b) = delete; // we can implement assignment constructor if we will need to, but it cannot be the default so for now so it's blocked

        ~Bucket() = default;
    };

    class DState {
        size_t depth;
        Bucket *dir[];  // allocated array with size 2^depth

        size_t getDepth(){
            return depth;
        }
    public:
        explicit DState(size_t depth) : depth(depth){
            assert(0 < depth && depth < 20);
            dir = new Bucket*[POW(depth)]();
            // instead of including <cmath> used quick power trick "<<"
            // auto initiated with nullptr
        }

        DState(const DState &b) = delete; // we can implement copy constructor if we will need to, but it cannot be the default so for now so it's blocked

        DState operator=(DState b) = delete; // we can implement assignment constructor if we will need to, but it cannot be the default so for now so it's blocked

        void EnlargeDir() {
            Bucket *new_dir[] = new Bucket*[POW(depth + 1)]();
            unsigned int dir_size = (unsigned)0x1 << (unsigned)(depth);

            for (int i = 0; i < dir_size; ++i) {
                new_dir[(i << 1) + 0] = dir[i];
                new_dir[(i << 1) + 1] = dir[i];
            }

            delete [] dir;
            dir = new_dir; // NOTE: This is a bug the pointer to the array will contain garbage when you exit this function
            depth++;
        }

        ~DState() {
            delete[] dir;
        }
    };

    /*** Global variables of the class goes below: ***/
    volatile std::atomic<DState*> ht;
    Operation *help[NUMBER_OF_THREADS]; // array size n
    int opSeqnum[NUMBER_OF_THREADS]; // This will be an array of size N and each thread will only access to its memory space

    /*** Inner function section goes below: ***/

    int existInBucket(Triple* const items[], Triple const& c) {
        for(int i = 0; i < BUCKET_SIZE; i++) {
            if (c.key == *items[i].key)
                return i;
        }
        return NOT_FOUND;
    }

    void ApplyWFOp(Bucket &b, unsigned int id) {
        std::atomic<BState*> newBState_atomic;
        BigWord oldToggle;
        BState *oldBState, *newBState;

        b.toggle.setBit(id); // mark as worked on by thread id

        for(int i = 0; i < 2; i++) {
            oldBState = b.state->load(std::memory_order_relaxed); // this is the most efficient access, it is suppose to work since the algo already covers all the cases
            newBState = new BState(oldBState);
            oldToggle = b.toggle;

            for(unsigned int j = 0; (j < NUMBER_OF_THREADS) && (oldToggle.testBit(j) != oldBState->applied.testBit(j)); j++) {
                if(newBState->results[j].seqnum < help[j]->seqnum) {
                    newBState->results[j].status = ExecOnBucket(newBState, *help[j]);
                    if(newBState->results[j].status != FAIL)
                        newBState->results[j].seqnum = help[j]->seqnum;
                }
            }

            newBState->applied = oldToggle;
            newBState_atomic.store(newBState, std::memory_order_relaxed); //TODO: Not sure this is the the right way to store

            if (std::atomic_compare_exchange_weak<BState*>(oldBState, oldBState,  newBState_atomic))
                delete oldBState;
            else
                delete newBState;
        }
    }

    Status_type ExecOnBucket(BState *b, Operation const &op) { //TODO this function will now not work correctly
        int updateID = 0;
        int freeID = 0;
        Triple *temp;
        auto *c = new Triple; //TODO: new throws exceptions? Maybe create a class for couple to make this cleaner
        c->value = new Value;
        *c->value = op.value;
        c->key = op.key;

        freeID = (b->BucketFull());
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

    Bucket *SplitBucket(Bucket const &b) { // returns 2 new Buckets
        const BState bs = *(b.state->load(std::memory_order_relaxed));
        // init 2 new Buckets:
        Bucket const *res = new Bucket*[2];
        BState const *bs0 = new BState(bs, false); // copy constructor without items
        BState const *bs1 = new BState(bs, false);
        bs0->applied(b.toggle); // b0.state.applied <- b0.toggle
        bs1->applied(b.toggle);
        res[0] = new Bucket((b.prefix << 1) + 0, b.depth + 1, bs0, BigWord()); // maybe should init with {}
        res[1] = new Bucket((b.prefix << 1) + 1, b.depth + 1, bs1, BigWord());

        // split the items between the new buckets:
        for (int i = 0; i < BUCKET_SIZE; ++i) {
            assert(bs.items[i]); // bucket should be full for splitting
            bool inserted_ok = false; // here for the debugging
            if (Prefix(bs.items[i]->key, res[0].depth) == res[0].prefix)
                inserted_ok = bs0->insertItem(bs.items[i]);
            else
                inserted_ok = bs1->insertItem(bs.items[i]);
            assert(inserted_ok);
        }
        return res;
    }

    void DirectoryUpdate(DState &d, Bucket const *blist[2]) {
        for (Bucket const *b : blist) {
            if (b->depth > d.depth) d.EnlargeDir();
            // TODO: "entries" part is really heavy and should be shorten: (I need help with that)
            for (size_t e = 0; e < POW(d.depth); ++e) {
                if (Prefix(e, b->depth) == b->prefix)
                    d.dir[e] = b;
            }
        }
    }

    bool ApplyPendingResize(DState &d, Bucket const& bFull) {
        for (int j = 0; j < NUMBER_OF_THREADS; ++j) {
            if (help[j] && Prefix(help[j]->key, bFull.depth) == bFull.prefix) {
                BState const& bs = *(bFull.state->load(std::memory_order_relaxed));
                if (bs.results[j].seqnum < help[j]->seqnum) {
                    Bucket *bDest = d.dir[Prefix(help[j]->key, d.depth)];
                    BState *bsDest = bDest->state->load(std::memory_order_relaxed);
                    while (bsDest->BucketFull()) {
                        Bucket splitted[2] = SplitBucket(bDest);
                        DirectoryUpdate(d, splitted);
                        bDest = d.dir[Prefix(help[j]->key, d.depth)];
                        bsDest = bDest->state->load(std::memory_order_relaxed);
                    }
                    bsDest->results[j].status = ExecOnBucket(bDest, help[j]);
                    bsDest->results[j].seqnum = help[j]->seqnum;
                }
            }
        }
    }

    bool ResizeWF() {
        for (int k = 0; k < 2; ++k) {
            DState *oldD = ht->load(std::memory_order_relaxed);
            DState const *newD = new DState(*oldD);
            for (int j = 0; j < NUMBER_OF_THREADS; ++j) {
                if (help[j]) {
                    Bucket *b = newD->dir[Prefix(help[j]->hash, newD->depth)];
                    BState &bs = *(b->state->load(std::memory_order_relaxed));
                    if (bs.BucketFull() && bs.results[j].seqnum < help[j]->seqnum) {
                        ApplyPendingResize(newD, *b);
                    }
                }
            }
            if (std::atomic_compare_exchange_weak<DState*>(ht, oldD,  newD))
                delete oldD;
            else
                delete newD;
        }
    }

    size_t Prefix(size_t hash, size_t depth) {
        assert(depth);
        int64_t mask = (1 << depth) - 1; //This will always result in all the first depth bits on
        int64_t prefix = (int64_t)hash & mask;
        return (size_t)prefix;
    }


public:
    hashmap() : ht(std::atomic<DState*>(new DState(1))), help(), opSeqnum() {};
    hashmap(hashmap&) = delete;
    hashmap operator=(hashmap) = delete;


    ~hashmap() {
        DState* d = *(ht->load(std::memory_order_relaxed));
        // TODO: delete all the buckets and...
        delete d;
    }

    struct Tuple {
        bool status;
        Value value;
    };

    Tuple lookup(Key const &key) const& { // return type from lookup is "const&" ?
        DState htl = *(ht->load(std::memory_order_relaxed));
        std::size_t hashed_key = std::hash<Key>(key); // we will use std hash for now
        BState bs = htl.dir[hashed_key];
        for (Triple *t : bs.items) {
            if (t->key == key) return {true, t->value};
        }
        return {false, 0}; // TODO: fix (detailed at the doc form)
    }


    enum Status_type insert(Key const &key, Value const &value, unsigned int id) {

        std::size_t hashed_key = std::hash<Key>(key); // TODO: fix hash
        help[id] = new Operation(INS, key, value, ++opSeqnum[id], hashed_key);
        DState htl = (ht->load(std::memory_order_relaxed));
        size_t hash_prefix =  Prefix(hashed_key, htl.getDepth());

        ApplyWFOp(htl.dir[hashed_key]);
        htl = *(ht->load(std::memory_order_relaxed));
        if (htl.dir[hash_prefix].state->results[id].seqnum != opSeqnum[id])
            ResizeWF();
        htl = *(ht->load(std::memory_order_relaxed));
        return htl.dir[hash_prefix].state->results[id].status;
    }



    bool remove(Key const &key); // delete is a saved word so I used remove

};



#endif //EWRHT_HASHMAP_H
