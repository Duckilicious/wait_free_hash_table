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
#define POW(exp) ((unsigned)1 << (unsigned)(exp))

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

    struct Operation {
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

        size_t getDepth() {
            return depth;
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

            for (int i = 0; i < POW(depth + 1); ++i) {
                new_dir[(i << 1) + 0] = dir[i];
                new_dir[(i << 1) + 1] = dir[i];
            }

            delete[] dir;
            dir = new_dir; // NOTE: This is a bug the pointer to the array will contain garbage when you exit this function
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

        b->toggle.setBit(id); // mark as worked on by thread id

        for (int i = 0; i < 2; i++) {
            oldBState = b->state.load(std::memory_order_relaxed); // this is the most efficient access, it is suppose to work since the algo already covers all the cases
            newBState = new BState(*oldBState); // copy constructor, pointer assignment
            oldToggle = b->toggle; // copy constructor using operator=

            for (unsigned int j = 0; (j < NUMBER_OF_THREADS); j++) {
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
    }

    Bucket** SplitBucket(Bucket *b) { // returns 2 new Buckets
        const BState bs = *(b->state.load(std::memory_order_relaxed));
        // init 2 new Buckets:
        Bucket **res = new Bucket*[2];
        BState* const bs0 = new BState(bs.results, b->toggle); // special constructor
        BState* const bs1 = new BState(bs.results, b->toggle);
        res[0] = new Bucket((b->prefix << 1) + 0, b->depth + 1, bs0, BigWord()); // maybe should init with {}
        res[1] = new Bucket((b->prefix << 1) + 1, b->depth + 1, bs1, BigWord());

        // split the items between the new buckets:
        for (int i = 0; i < BUCKET_SIZE; ++i) {
            assert(bs.items[i]); // bucket should be full for splitting
            bool inserted_ok = false; // here for the debugging
            if (Prefix(bs.items[i]->key, res[0]->depth) == res[0]->prefix)
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
                if (Prefix(e, b->depth) == b->prefix)
                    // TODO: need to delete old bucket?
                    d.dir[e] = b;
            }
        }
    }

    bool ApplyPendingResize(DState &d, Bucket const& bFull) {
        for (int j = 0; j < NUMBER_OF_THREADS; ++j) {
            if (help[j] && Prefix(help[j]->key, bFull.depth) == bFull.prefix) {
                BState const& bs = *(bFull.state.load(std::memory_order_relaxed));
                if (bs.results[j].seqnum < help[j]->seqnum) {
                    Bucket *bDest = d.dir[Prefix(help[j]->key, d.depth)];
                    BState *bsDest = bDest->state.load(std::memory_order_relaxed);
                    while (bsDest->BucketFull()) {
                        Bucket** const splitted = SplitBucket(bDest);
                        delete bDest; // delete old bucket
                        DirectoryUpdate(d, splitted);
                        delete[] splitted; // deletes only the array that holds the new Buckets
                        bDest = d.dir[Prefix(help[j]->key, d.depth)];
                        bsDest = bDest->state.load(std::memory_order_relaxed);
                    }
                    bsDest->results[j].status = ExecOnBucket(bsDest, *help[j]);
                    bsDest->results[j].seqnum = help[j]->seqnum;
                }
            }
        }
    }

    bool ResizeWF() {
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

    size_t Prefix(size_t hash, size_t depth) {
        assert(depth);
        int64_t mask = (1 << depth) - 1; //This will always result in all the first depth bits on
        int64_t prefix = (int64_t)hash & mask;
        return (size_t)prefix;
    }


public:
    hashmap() :  help(), opSeqnum() {
        ht.store(new DState(), std::memory_order_relaxed);
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
        DState *htl = (ht.load(std::memory_order_relaxed));
        std::size_t hashed_key = std::hash<Key>{}(key); // we will use std hash for now
        BState *bs = htl->dir[hashed_key]->state.load(std::memory_order_relaxed);
        for (Triple *t : bs->items) {
            // TODO: what if the BState gets deleted while this search?
            if (t && t->key == key) return {true, t->value};
        }
        return {false, 0};
    }


    enum Status_type insert(Key const &key, Value const &value, unsigned int id) {

        size_t hashed_key = std::hash<Key>{}(key); // TODO: fix hash
        help[id] = new Operation(INS, key, value, ++opSeqnum[id], hashed_key);
        DState *htl = (ht.load(std::memory_order_relaxed));
        size_t hash_prefix = Prefix(hashed_key, htl->getDepth());


        ApplyWFOp(htl->dir[hash_prefix], id);
        htl = (ht.load(std::memory_order_relaxed));
        BState *bstate = htl->dir[hash_prefix]->state.load(std::memory_order_relaxed);

        if (bstate->results[id].seqnum != opSeqnum[id])
            ResizeWF(); // TODO: going into resize when there is still room in the Bucket.. why?
        htl = (ht.load(std::memory_order_relaxed));
        bstate = htl->dir[hash_prefix]->state.load(std::memory_order_relaxed);
        return bstate->results[id].status;
    }



    bool remove(Key const &key);

};



#endif //EWRHT_HASHMAP_H
