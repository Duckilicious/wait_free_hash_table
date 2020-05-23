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
#define SIZE_OF_HASH (32)

#include <iostream> // for debugging
#include <atomic>
#include <cassert>
#include <bitset> // TODO using for the print only
#include "xxhash/include/xxhash.hpp"



template<typename Key, typename Value>
class hashmap {
    // private:
    enum Status_type {
        TRUE, FALSE, FAIL
    };

    struct Triple {
        xxh::hash_t<32> hash;
        Key key;
        Value value;

        Triple(xxh::hash_t<32> h, Key k, Value v ) :
            hash(h), key(k), value(v) {};
    };

    enum Op_type {
        INS, DEL
    };

    struct Operation {
        Op_type type;
        Key key;
        Value value;
        int seqnum;
        xxh::hash_t<32>  hash;

        Operation(Op_type t, Key k, Value v, int seq, xxh::hash_t<32> h) :
                type(t), key(k), value(v), seqnum(seq), hash(h) {};

        Operation(Op_type t, Key k, int seq, xxh::hash_t<32> h) :
                type(t), key(k), seqnum(seq), hash(h) {};
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
        BState(BState const& old) : applied(old.applied) {
            // TODO notice that copy constructor maybe should copy only the items?
            for (int i = 0; i < BUCKET_SIZE; i++)
                this->items[i] = old.items[i];

            for (int i = 0; i < NUMBER_OF_THREADS; i++)
                this->results[i] = old.results[i];
        };
        BState(const Result* const r, BigWord const& bw) : items(), applied(bw) {
            // this is a special constructor.
            for (int i = 0; i < NUMBER_OF_THREADS; i++)
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

        ~BState() = delete; // TODO: maybe need to delete the items here? but they may use in other new BState
    };

    struct Bucket {
        uint32_t prefix; // TODO: should it be of type size_t?
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

        explicit Bucket(uint32_t p, size_t d, volatile std::atomic<BState*> s, BigWord t) :
                prefix(p), depth(d), state(s), toggle(t){}

        explicit Bucket(uint32_t p, size_t d, BState* s, BigWord t) : prefix(p), depth(d), toggle(t){
            this->state.store(s, std::memory_order_relaxed);
        }

        Bucket operator=(Bucket b) = delete; // we can implement assignment constructor if we will need to, but it cannot be the default so for now so it's blocked

        ~Bucket() {
//            delete this->state.load(std::memory_order_relaxed); // TODO: smart pointers?
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

        DState(const DState &d) : depth(d.depth) {
            dir = new Bucket*[POW(depth)];
            for (int i = 0; i < POW(depth); i++) {
                assert(d.dir[i]);
                dir[i] = new Bucket(*(d.dir[i]));
                while (i+1 < POW(depth) && d.dir[i] == d.dir[i+1]) {
                    dir[i+1] = dir[i];
                    i++;
                }
            }
        }

        DState operator=(DState b) = delete; // we can implement assignment constructor if we will need to, but it cannot be the default so for now so it's blocked

        void EnlargeDir() {
            Bucket **new_dir = new Bucket*[POW(depth + 1)]();
            for (int i = 0; i < POW(depth); ++i) {
                new_dir[(i << 1) + 0] = dir[i];
                new_dir[(i << 1) + 1] = dir[i];
            }

//             delete[] dir; // TODO: smart pointers?
             dir = new_dir;
             depth++;
        }

        ~DState() {
            for (int i = 0; i < POW(depth); i++) {
                // if (dir[i]) delete *(dir[i]); TODO: delete the Buckets here? they may use bor the new DState
            }
//            delete[] dir; // TODO: smart pointers?
        }
    };



    /*** Global variables of the class goes below: ***/
    volatile std::atomic<DState*> ht;
    Operation *help[NUMBER_OF_THREADS];
    int opSeqnum[NUMBER_OF_THREADS]; // This will be an array of size N and each thread will only access to its memory space

    /*** Inner function section goes below: ***/


    void ApplyWFOp(Bucket *b, unsigned int id) {
        BigWord oldToggle;
        BState *oldBState, *newBState;

        b->toggle.flipBit(id); // mark as worked on by thread id

        for (int i = 0; i < 2; i++) {
            oldBState = b->state.load(std::memory_order_relaxed); // this is the most efficient access, it is suppose to work since the algo already covers all the cases
            newBState = new BState(*oldBState); // copy constructor, pointer assignment
            newBState->applied = BigWord(); // ADDED 11/05/2020
            oldToggle = b->toggle; // copy constructor using operator=

            for (unsigned int j = 0; (j <= NUMBER_OF_THREADS); j++) {
                if (oldToggle.testBit(j) == newBState->applied.testBit(j)) continue;
                assert(newBState->results[j].seqnum >= 0 && help[j]->seqnum > 0);
                if (newBState->results[j].seqnum < help[j]->seqnum) {
                    newBState->results[j].status = ExecOnBucket(newBState, *help[j]);
                     if (newBState->results[j].status != FAIL)
                        newBState->results[j].seqnum = help[j]->seqnum;
                }
            }

            newBState->applied = oldToggle; // copy constructor using operator=

            if (std::atomic_compare_exchange_weak<BState*>(&b->state, &oldBState,  newBState)) {}
//                delete oldBState; // TODO: smart pointers?
//            } else delete newBState;
        }
    }

    Status_type ExecOnBucket(BState *b, Operation const &op) {
        int updateID = 0;
        int freeID = 0;
        Triple *temp;

        freeID = (b->BucketFull());
        if (freeID == FULL_BUCKET) {
            return FAIL;
        }
        else {
            //Delete
            if (op.type == DEL) {
                delete b->items[updateID];
                b->items[updateID] = nullptr;
                return TRUE;
            }
            //Insert/update
            auto *c = new Triple(op.hash, op.key, op.value);
            updateID = b->existInBucket(*c);
            if (updateID == NOT_FOUND) {
                if (op.type == INS) {
                    b->items[freeID] = c;
                }
            }
            else {
                if (op.type == INS) {
                    temp = b->items[updateID];
                    b->items[updateID] = c;
//                    delete temp; // TODO: smart pointers?
                }
            }
        }
        return TRUE;
    }

    Bucket** SplitBucket(Bucket *b) { // returns 2 new Buckets
        const BState *bs = new BState(*(b->state.load(std::memory_order_relaxed))); // copied, todo: delete or smart pointers
        // init 2 new Buckets:
        Bucket **res = new Bucket*[2];
        BState* const bs0 = new BState(bs->results, b->toggle); // special constructor
        BState* const bs1 = new BState(bs->results, b->toggle);
        res[0] = new Bucket((b->prefix << 1) + 0, b->depth + 1, bs0, BigWord());
        res[1] = new Bucket((b->prefix << 1) + 1, b->depth + 1, bs1, BigWord());

        // split the items between the new buckets:
        for (int i = 0; i < BUCKET_SIZE; ++i) {
            assert(bs->items[i]); // bucket should be full for splitting
            bool inserted_ok = false; // here for the debugging
            if (Prefix(bs->items[i]->hash, res[0]->depth) == res[0]->prefix)
                inserted_ok = bs0->insertItem(bs->items[i]);
            else
                inserted_ok = bs1->insertItem(bs->items[i]);
            assert(inserted_ok);
        }
        return res;
    }

    void DirectoryUpdate(DState &d, Bucket* const *blist) {
        int b_index = 0;
        if (blist[b_index]->depth > d.depth) d.EnlargeDir();
        for (size_t e = 0; e < POW(d.depth); ++e) {
            // maybe use binary search somehow instead of moving one by one at begin?
            if (Prefix(e, blist[b_index]->depth, d.depth) == blist[b_index]->prefix) {
                // TODO: when need to delete old bucket? complicated
                d.dir[e] = blist[b_index];
                if (e + 1 < POW(d.depth) && Prefix(e+1, blist[b_index]->depth, d.depth) != blist[b_index]->prefix) {
                    if (b_index == 0) b_index++; // blist[1] will always be right after blist[0]
                    else break; // early stop
                }
            }
        }
    }

    void ApplyPendingResize(DState &d, Bucket const& bFull) {
        for (int j = 0; j < NUMBER_OF_THREADS; ++j) {
            if (help[j] && Prefix(help[j]->hash, bFull.depth) == bFull.prefix) {
                BState const& bs = *(bFull.state.load(std::memory_order_relaxed));
                assert(bs.results[j].seqnum >= 0 && help[j]->seqnum >= 0);
                if (bs.results[j].seqnum < help[j]->seqnum) {
                    Bucket *bDest = d.dir[Prefix(help[j]->hash, d.depth)];
                    BState *bsDest = bDest->state.load(std::memory_order_relaxed);
                    while (bsDest->BucketFull() == FULL_BUCKET) {
                        Bucket** const splitted = SplitBucket(bDest);
                        DirectoryUpdate(d, splitted);
//                        delete bDest; // TODO: smart pointers?
                        delete[] splitted;
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
                if (help[j]) { // different from the paper cause we might have (help[j] == nullptr)
                    Bucket *b = newD->dir[Prefix(help[j]->hash, newD->depth)];
                    BState *bs = (b->state.load(std::memory_order_relaxed));
                    if (bs->BucketFull() && bs->results[j].seqnum < help[j]->seqnum) {
                        ApplyPendingResize(*newD, *b);
                    }
                }
            }

            if (std::atomic_compare_exchange_weak<DState*>(&ht, &oldD,  newD)) {
//                delete oldD; // TODO: smart pointers?
            } else delete newD;
        }
    }

    uint32_t Prefix(xxh::hash_t<32> const hash, uint32_t const depth) const {
        assert(depth);
        int shift = SIZE_OF_HASH - depth;
        uint32_t mask = ~(uint32_t)((1 << (shift)) - 1);
        auto prefix = (uint32_t)((hash & mask) >> shift);
        return prefix;
    }

    uint32_t Prefix(size_t hash, uint32_t const depth, uint32_t start) const {
        assert(SIZE_OF_HASH - start - depth > 0); //BUG

        uint32_t mask = (uint32_t)((1 << (start)) - 1); // mask for the first (start) bits
        auto prefix = (uint32_t)(hash & mask) << (SIZE_OF_HASH - start) >> (SIZE_OF_HASH - depth); // clear bits bigger than start and smaller than depth
        return prefix;
    }

public:

    hashmap() :  help(){
        ht.store(new DState(), std::memory_order_relaxed);
        for (int &i : opSeqnum) i = 0;
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

    Tuple lookup(Key const &key) const& {
        const void* kptr = &key;
        xxh::hash_t<32> hashed_key(xxh::xxhash<32>(kptr, sizeof(Key)));
        DState *htl = (ht.load(std::memory_order_relaxed));
        size_t hash_prefix = Prefix(hashed_key, htl->getDepth());
        BState *bs = htl->dir[hash_prefix]->state.load(std::memory_order_relaxed);
        for (Triple *t : bs->items) {
            // TODO: what if the BState gets deleted while this search?
            if (t && t->key == key) return {true, t->value};
        }
//        this->DebugPrintDir(); // for debugging test05
        return {false, 0};
    }

    bool insert(Key const &key, Value const &value, unsigned int const id) {

        opSeqnum[id]++;
        const void* kptr = &key;
        xxh::hash_t<32> hashed_key(xxh::xxhash<32>(kptr, sizeof(Key)));
        help[id] = new Operation(INS, key, value, opSeqnum[id], hashed_key);
        DState *htl = (ht.load(std::memory_order_relaxed));
        uint32_t hash_prefix = Prefix(hashed_key, htl->getDepth());

        ApplyWFOp(htl->dir[hash_prefix], id);

        htl = (ht.load(std::memory_order_relaxed));
        const BState &bstate = *(htl->dir[hash_prefix]->state.load(std::memory_order_relaxed));
        assert(bstate.results[id].seqnum >= 0 && opSeqnum[id] >= 0);
        if (bstate.results[id].seqnum != opSeqnum[id])
            ResizeWF();

        htl = (ht.load(std::memory_order_relaxed));
        const BState &bstate2 = *(htl->dir[Prefix(hashed_key, htl->getDepth())]->state.load(std::memory_order_relaxed));
        if (bstate2.results[id].status != TRUE)
            std::cout << "\n## ERROR INSERTING " << key << "! ##\n"; // for debugging test05
        return (bstate2.results[id].status == TRUE);
    }

    void DebugPrintDir() const {
        std::cout << std::endl;
        auto htl = ht.load(std::memory_order_relaxed);
        for(int i = 0; i < POW(htl->depth); i++) {
            BState *bs = htl->dir[i]->state.load(std::memory_order_relaxed);
            std::cout << "Entries: [" << i << ",";
            while (i+1 < POW(htl->depth) && htl->dir[i] == htl->dir[i+1]) i++;
            if (i+1 < POW(htl->depth))
                assert(htl->dir[i]->depth != htl->dir[i+1]->depth || htl->dir[i]->prefix != htl->dir[i+1]->prefix);
            std::cout << i << "]\tpoints to bucket with prefix: ";
            for (int k = htl->dir[i]->depth - 1; k >= 0; --k) std::cout << ((htl->dir[i]->prefix >> k) & 1);
             std::cout << ".\tItems: " << std::endl;
            for (int j = 0; j < BUCKET_SIZE; j++) {
                if (bs->items[j] != nullptr) {
                    std::cout << "\t\t" <<  "(hash: " << std::bitset<SIZE_OF_HASH>(bs->items[j]->hash)  << ")\t\tvalue: " << bs->items[j]->value
                    << "\tkey: " << bs->items[j]->key << std::endl;
                }
            }
        }
    }

    bool remove(Key const &key, unsigned int const id) {
        opSeqnum[id]++;
        const void* kptr = &key;
        xxh::hash_t<32> hashed_key(xxh::xxhash<32>(kptr, sizeof(Key)));
        help[id] = new Operation(DEL, key, opSeqnum[id], hashed_key);
        DState *htl = (ht.load(std::memory_order_relaxed));
        uint32_t hash_prefix = Prefix(hashed_key, htl->getDepth());

        ApplyWFOp(htl->dir[hash_prefix], id);

        htl = (ht.load(std::memory_order_relaxed));
        BState *bstate = htl->dir[hash_prefix]->state.load(std::memory_order_relaxed);
        if (bstate->results[id].seqnum != opSeqnum[id])
            ResizeWF();

        htl = (ht.load(std::memory_order_relaxed));
        bstate = htl->dir[hash_prefix]->state.load(std::memory_order_relaxed);
        return (bstate->results[id].status == TRUE);
    }

};



#endif //EWRHT_HASHMAP_H












