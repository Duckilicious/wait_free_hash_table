//
// Created by Alon on 30/03/2020.
//

#ifndef EWRHT_HASHMAP_H
#define EWRHT_HASHMAP_H

#define BUCKET_SIZE (2)
#define NUMBER_OF_THREADS (1)
#define NOT_FOUND (-1)
#define FULL_BUCKET (-1)
#define BIGWORD_SIZE (16)
#define POW(exp) ((unsigned)1 << (unsigned)(exp))

#include <iostream>
#include <pthread.h>
#include <atomic>
#include <cassert>
#include "../xxhash_cpp/include/xxhash.hpp"


enum Status_type {
    TRUE, FALSE, FAIL
};

template<typename Key, typename Value>
class hashmap {
    // private:
    struct Triple {
        xxh::hash_t<32> hash;
        Key key;
        Value value;
    };

    enum Op_type {
        INS, DEL
    };

    struct Operation {
        Op_type type;
        Key key;
        Value value;
        int seqnum;
        size_t hash;

        Operation(Op_type t, Key k, Value v, int seq, xxh::hash_t<32> h) :
                type(t), key(k), value(v), seqnum(seq), hash(h) {};
        Operation() : seqnum(0) {}; // todo: when this is used? maybe init all fields?
    };

    struct Result {
        Status_type status;
        int seqnum;
    };

    struct BigWord {
        uint8_t Data[BIGWORD_SIZE];

    public:
        BigWord() : Data() {}

        BigWord(BigWord const& b) : Data() {
            for (int i = 0; i < BIGWORD_SIZE; ++i) {
                this->Data[i] = b.Data[i];
            }
        }

        BigWord& operator=(BigWord const& b) {
            for (int i = 0; i < BIGWORD_SIZE; ++i) {
                this->Data[i] = b.Data[i];
            }
            return *this;
        }

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

        void flipBit(const unsigned int id) {
            if(this->testBit(id))
                clearBit(id);
            else
                setBit(id);
        }
    };

    struct BState {
        Triple *items[BUCKET_SIZE]; // if pointer = nullptr place is free
        Result results[NUMBER_OF_THREADS]; // array size n
        BigWord applied; // bit array of size 128

    public:
        BState() : items(), results(), applied() {}
        BState(BState const& old) : applied(old.applied){
            for (int i = 0; i < BUCKET_SIZE; i++)
                this->items[i] = old.items[i];

            for(int i = 0; i < NUMBER_OF_THREADS; i++)
                this->results[i] = old.results[i];
        };
        BState(const Result* const r, BigWord const& bw) : items(), applied(bw) {
            // this is a special constructor.
            for(int i = 0; i < NUMBER_OF_THREADS; i++)
                this->results[i] = r[i];
        }
        BState operator=(BState b) = delete; // we can implement assignment constructor if we will need to, but it cannot be the default so for now so it's blocked

        bool insertItem(Triple* const t) {
            for (Triple* &item : items) {
                if (!item) {
                    item = t;
                    return true;
                }
            }
            return false; // bucket is full
        }

        int BucketFull() const { // Returns the free entry if it exists, else -1
            for (int i = 0; i < BUCKET_SIZE; i ++) {
                if (this->items[i] == nullptr)
                    return i;
            }
            return FULL_BUCKET;
        }

        int existInBucket(Triple const& c) {
            for(int i = 0; i < BUCKET_SIZE; i++) {
                if (items[i] && c.key == items[i]->key)
                    return i;
            }
            return NOT_FOUND;
        }

        ~BState() = default; // TODO: maybe need to delete the items here? but they may use in other new BState
    };

    struct Bucket {
        int prefix; // TODO: should it be of type size_t?
        size_t depth;
        volatile std::atomic<BState*> state; //TODO: volatile means un-cacheable this is the CAS API demands, but maybe we can find a better CAS that doesn't require this since this is more exspensive
        BigWord toggle; // 128 bit array

    public:
        Bucket() : prefix(), depth(), toggle() {
            this->state.store(new BState(), std::memory_order_relaxed);
        }

        Bucket(const Bucket &b) : prefix(b.prefix), depth(b.depth), toggle(b.toggle) {
            this->state.store(new BState(*(b.state.load(std::memory_order_relaxed))), std::memory_order_relaxed);
        } // this is the copy constructor

        explicit Bucket(int p, size_t d, volatile std::atomic<BState*> s, BigWord t) :
                prefix(p), depth(d), state(s), toggle(t){}

        explicit Bucket(int p, size_t d, BState* s, BigWord t) : prefix(p), depth(d), toggle(t){
            this->state.store(s, std::memory_order_relaxed);
        }

        Bucket operator=(Bucket b) = delete; // we can implement assignment constructor if we will need to, but it cannot be the default so for now so it's blocked

        ~Bucket() {
            delete this->state.load(std::memory_order_relaxed);
        }
    };

    struct DState {
        size_t depth;
        Bucket **dir;  // allocated array with size 2^depth

    public:
        DState() : depth(1){
            assert(0 < depth && depth < 20);
            dir = new Bucket*[POW(depth)];
            dir[0] = new Bucket();
            dir[1] = new Bucket();
            dir[0]->depth = 1;
            dir[1]->depth = 1;
            dir[0]->prefix = 0;
            dir[1]->prefix = 1;

        }

        inline size_t getDepth() {
            return this->depth;
        }

        Bucket getDir() {
            return dir;
        }

        DState(const DState &d) : depth(d.depth) {
            dir = new Bucket*[POW(depth)];
            for (int i = 0; i < POW(depth); i++) {
                assert(d.dir[i]);
                dir[i] = new Bucket(*(d.dir[i]));
            }
        }

        DState operator=(DState b) = delete; // we can implement assignment constructor if we will need to, but it cannot be the default so for now so it's blocked

        void EnlargeDir() {
            Bucket **new_dir = new Bucket*[POW(depth + 1)]();
            for (int i = 0; i < POW(depth); ++i) {
                new_dir[(i << 1) + 0] = dir[i];
                new_dir[(i << 1) + 1] = dir[i];
            }

             delete[] dir; //third/second time we delete this it fails why? maybe because it's not an array anymore?
             dir = new_dir;
             depth++;
        }

        ~DState() {
            for (int i = 0; i < POW(depth); i++) {
                // if (dir[i]) delete *(dir[i]); TODO: delete the Buckets here? they may use bor the new DState
            }
            delete[] dir;
        }
    };



    /*** Global variables of the class goes below: ***/
    volatile std::atomic<DState*> ht;
    Operation *help[NUMBER_OF_THREADS];
    int opSeqnum[NUMBER_OF_THREADS]; // This will be an array of size N and each thread will only access to its memory space

    /*** Inner function section goes below: ***/


    void ApplyWFOp(Bucket *b, unsigned int id) {
        std::atomic<BState*> newBState_atomic;
        BigWord oldToggle;
        BState *oldBState, *newBState;

        b->toggle.flipBit(id); // mark as worked on by thread id

        for (int i = 0; i < 2; i++) {
            oldBState = b->state.load(std::memory_order_relaxed); // this is the most efficient access, it is suppose to work since the algo already covers all the cases
            newBState = new BState(*oldBState); // copy constructor, pointer assignment
            oldToggle = b->toggle; // copy constructor using operator=

            for (unsigned int j = 0; (j <= NUMBER_OF_THREADS); j++) {
                if (oldToggle.testBit(j) == oldBState->applied.testBit(j)) continue;
                if (newBState->results[j].seqnum < help[j]->seqnum) {
                    newBState->results[j].status = ExecOnBucket(newBState, *help[j]);
                     if (newBState->results[j].status != FAIL)
                        newBState->results[j].seqnum = help[j]->seqnum;
                }
            }

            newBState->applied = oldToggle; // copy constructor using operator=
            newBState_atomic.store(newBState, std::memory_order_relaxed);

            if (std::atomic_compare_exchange_weak<BState*>(&b->state, &oldBState,  newBState))
                delete oldBState;
            else
                delete newBState;
        }
    }

    Status_type ExecOnBucket(BState *b, Operation const &op) { //TODO this function will now not work correctly
        int updateID = 0;
        int freeID = 0;
        Triple *temp;
        auto *c = new Triple; //can create a more elegant c'tor

        c->value = op.value;
        c->key = op.key;
        c->hash = op.hash;

        freeID = (b->BucketFull());
        if (freeID == FULL_BUCKET) {
            delete c;
            return FAIL;
        }

        else {
            updateID = b->existInBucket(*c);
            if (updateID == NOT_FOUND) {
                if (op.type == INS) {
                    b->items[freeID] = c;
                }
            }
            else {
                if(op.type == INS) {
                    temp = b->items[updateID];
                    b->items[updateID] = c;
                    delete temp;
                }
                if(op.type == DEL) {
                    delete b->items[updateID];
                    b->items[updateID] = nullptr;
                }
            }
        }
        return TRUE;
    }

    Bucket** SplitBucket(Bucket *b) { // returns 2 new Buckets
        const BState bs = *(b->state.load(std::memory_order_relaxed));
        // init 2 new Buckets:
        Bucket **res = new Bucket*[2];
        BState* const bs0 = new BState(bs.results, b->toggle); // special constructor
        BState* const bs1 = new BState(bs.results, b->toggle);
        res[0] = new Bucket((b->prefix << 1) + 0, b->depth + 1, bs0, BigWord());
        res[1] = new Bucket((b->prefix << 1) + 1, b->depth + 1, bs1, BigWord());

        // split the items between the new buckets:
        for (int i = 0; i < BUCKET_SIZE; ++i) {
            assert(bs.items[i]); // bucket should be full for splitting
            bool inserted_ok = false; // here for the debugging
            if (Prefix(bs.items[i]->hash, res[0]->depth) == res[0]->prefix)
                inserted_ok = bs0->insertItem(bs.items[i]);
            else
                inserted_ok = bs1->insertItem(bs.items[i]);
            assert(inserted_ok);
        }
        return res;
    }

    void DirectoryUpdate(DState &d, Bucket* const *blist) {
        for (int i = 0; i < 2; ++i) {
            Bucket* b = blist[i];
            if (b->depth > d.depth) d.EnlargeDir();
            // TODO: "entries" part is really heavy and should be shorten: (I need help with that)
            for (size_t e = 0; e < POW(d.depth); ++e) {
                if (e == b->prefix)
                    // TODO: need to delete old bucket?
                    d.dir[e] = b;
            }
        }
    }

    void ApplyPendingResize(DState &d, Bucket const& bFull) {
        for (int j = 0; j < NUMBER_OF_THREADS; ++j) {
            if (help[j] && Prefix(help[j]->hash, bFull.depth) == bFull.prefix) {
                BState const& bs = *(bFull.state.load(std::memory_order_relaxed));
                if (bs.results[j].seqnum < help[j]->seqnum) {
                    Bucket *bDest = d.dir[Prefix(help[j]->hash, d.depth)];
                    BState *bsDest = bDest->state.load(std::memory_order_relaxed);
                    while (bsDest->BucketFull() == FULL_BUCKET) {
                        Bucket** const splitted = SplitBucket(bDest);
                        delete bDest; // delete old bucket
                        DirectoryUpdate(d, splitted);
                        delete[] splitted; //First time it works second/third time fails
                        bDest = d.dir[Prefix(help[j]->hash, d.depth)];
                        bsDest = bDest->state.load(std::memory_order_relaxed);
                    }
                    bsDest->results[j].status = ExecOnBucket(bsDest, *help[j]);
                    bsDest->results[j].seqnum = help[j]->seqnum;
                }
            }
        }
    }

    void ResizeWF() {
        for (int k = 0; k < 2; ++k) {
            DState *oldD = ht.load(std::memory_order_relaxed);
            DState *newD = new DState(*oldD);
            assert(newD->dir[0]);

            for (int j = 0; j < NUMBER_OF_THREADS; ++j) {
                if (help[j]) {
                    Bucket *b = newD->dir[Prefix(help[j]->hash, newD->depth)];
                    BState *bs = (b->state.load(std::memory_order_relaxed));
                    if (bs->BucketFull() && bs->results[j].seqnum < help[j]->seqnum) {
                        ApplyPendingResize(*newD, *b);
                    }
                }
            }
            // try replace to the new DState
            if (std::atomic_compare_exchange_weak<DState*>(&ht, &oldD,  newD))
                delete oldD;
            else
                delete newD;
        }
    }

    size_t Prefix(size_t const hash, size_t const depth) const {
        assert(depth);
        int shift = sizeof(size_t)*8 - depth;
        size_t mask = ~(size_t)((1 << (shift)) - 1);
        size_t prefix = (size_t)((hash & mask) >> shift);
        return prefix;
    }

public:
    hashmap() :  help(){
        ht.store(new DState(), std::memory_order_relaxed);
        for (auto &i : opSeqnum) {
            i = 0;
        }
    };
    hashmap(hashmap&) = delete;
    hashmap operator=(hashmap) = delete;


    ~hashmap() {
        DState* d = (ht.load(std::memory_order_relaxed));
        // TODO: delete the dir and all the buckets and all the Triples..
        delete d;
    }

    struct Tuple {
        bool status;
        Value value;
    };

    Tuple lookup(Key const &key) const& { // return type from lookup is "const&" ?
        size_t hashed_key = std::hash<Key>{}(key); // we will use std hash for now
        DState *htl = (ht.load(std::memory_order_relaxed));
        size_t hash_prefix = Prefix(hashed_key, htl->getDepth());
        BState *bs = htl->dir[hash_prefix]->state.load(std::memory_order_relaxed);
        for (Triple *t : bs->items) {
            // TODO: what if the BState gets deleted while this search?
            if (t && t->key == key) return {true, t->value};
        }
        return {false, 0};
    }
    
    bool insert(Key const &key, Value const &value, unsigned int const id) {

        opSeqnum[id]++;
        const void* kptr = &key;
        xxh::hash_t<32> hashed_key(xxh::xxhash<32>(kptr, sizeof(Key)));
        help[id] = new Operation(INS, key, value, opSeqnum[id], hashed_key);
        DState *htl = (ht.load(std::memory_order_relaxed));
        size_t hash_prefix = Prefix(hashed_key, htl->getDepth());

        ApplyWFOp(htl->dir[hash_prefix], id);

        htl = (ht.load(std::memory_order_relaxed));
        BState *bstate = htl->dir[hash_prefix]->state.load(std::memory_order_relaxed);
        if (bstate->results[id].seqnum != opSeqnum[id])
            ResizeWF();

        htl = (ht.load(std::memory_order_relaxed));
        bstate = htl->dir[hash_prefix]->state.load(std::memory_order_relaxed);
        return (bstate->results[id].status == TRUE);
    }

    void DebugPrintDir() {
        std::cout << "#" << std::endl;
        auto htl = ht.load(std::memory_order_relaxed);
        for(int i = 0; i < POW(htl->depth); i++) {
            BState *bs = htl->dir[i]->state.load(std::memory_order_relaxed);
            std::cout << "Entry: " << i << " points to bucket with prefix " << htl->dir[i]->prefix << "\n";
            std::cout << '\t' << "Items in Bstate: " << '\n';
            for (int j = 0; j < BUCKET_SIZE; j++) {
                if (bs->items[j] != nullptr) {
                    std::cout << '\t' << '\t' <<  "hash: " << bs->items[j]->hash  << " value: " << bs->items[j]->value
                    << " key: " << bs->items[j]->key << '\n';
                }
            }
        }
    }



    bool remove(Key const &key);

};



#endif //EWRHT_HASHMAP_H
