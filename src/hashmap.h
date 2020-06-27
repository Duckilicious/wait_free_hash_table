//
// Created by Alon on 30/03/2020.
//

#ifndef EWRHT_HASHMAP_H
#define EWRHT_HASHMAP_H

#define BUCKET_SIZE (50)
#define NUMBER_OF_THREADS (64)
#define NOT_FOUND (-1)
#define FULL_BUCKET (-1)
#define BIGWORD_SIZE (16 * 8)
#define POW(exp) ((unsigned)1 << (unsigned)(exp))
#define SIZE_OF_HASH (32)

#include <iostream> // for debugging
#include <atomic>
#include <cassert>
#include <bitset> // TODO using for the print only
#include "xxhash/include/xxhash.hpp"


// Key & Value must have default constructor: Key() & Value()
template<typename Key, typename Value>
class hashmap {
    // private:
    enum Status_type {
        FALSE, TRUE, FAIL
    };

    struct Triple {
        bool valid_item;
        xxh::hash_t<32> hash;
        Key key;
        Value value;

        Triple() : valid_item(false) {}

        Triple(xxh::hash_t<32> h, Key k, Value v) :
                valid_item(true), hash(h), key(k), value(v) {};
    };

    enum Op_type {
        NONE, INS, DEL
    };

    struct Operation {
        Op_type type;
        Key key;
        Value value;
        int seqnum;
        xxh::hash_t<32> hash;

        Operation() : type(NONE) {}

        Operation(Op_type t, Key k, Value v, int seq, xxh::hash_t<32> h) :
                type(t), key(k), value(v), seqnum(seq), hash(h) {};

        Operation(Op_type t, Key k, int seq, xxh::hash_t<32> h) :
                type(t), key(k), value(), seqnum(seq), hash(h) {};
    };

    struct Result {
        Status_type status;
        int seqnum;
    };

    struct BigWord {
        bool Data[BIGWORD_SIZE];

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

        bool TestBit(unsigned int id) const {
            return Data[id];
        }

        void FlipBit(const unsigned int id) {
            Data[id] = !Data[id];
        }
    };

    struct BState {
        Triple items[BUCKET_SIZE]; // if (item_valid == false) place is free
        Result results[NUMBER_OF_THREADS]; // array size n
        BigWord applied; // bit array of size 128

    public:
        BState() : items(), results(), applied() {}

        BState(BState const &old) : applied(old.applied) {
            for (int i = 0; i < BUCKET_SIZE; i++)
                this->items[i] = old.items[i];

            for (int i = 0; i < NUMBER_OF_THREADS; i++)
                this->results[i] = old.results[i];
        };

        BState(const Result *const res, BigWord const &applied)
                : items(), applied(applied) {
            // this is a special constructor.
            for (int i = 0; i < NUMBER_OF_THREADS; i++)
                this->results[i] = res[i];
        }

        BState operator=(BState b) = delete;

        bool InsertItem(Triple const t) {
            for (Triple &item : items) {
                if (!item.valid_item) {
                    item = t;
                    return true;
                }
            }
            return false; // bucket is full
        }

        int BucketAvailability() const {
            // Returns the free entry if exists, else -1 for full bucket
            for (int i = 0; i < BUCKET_SIZE; i++) {
                if (!this->items[i].valid_item)
                    return i;
            }
            return FULL_BUCKET;
        }

        int GetItem(Triple const &c) const {
            for (int i = 0; i < BUCKET_SIZE; i++) {
                if (items[i].valid_item && c.key == items[i].key)
                    return i;
            }
            return NOT_FOUND;
        }

        ~BState() = default;
    };

    struct Bucket {
        uint32_t prefix;
        size_t depth;
        volatile std::atomic<BState *> state;
        BigWord toggle; // 128 bit array

    public:
        Bucket() : prefix(), depth(), toggle() {
            this->state.store(new BState(), std::memory_order_relaxed);
        }

        Bucket(const Bucket &b) = delete;

        explicit Bucket(uint32_t p, size_t d, BState *s, BigWord t)
            : prefix(p), depth(d), toggle(t) {
            this->state.store(s, std::memory_order_relaxed);
        }

        Bucket operator=(Bucket b) = delete;

        ~Bucket() {
            delete this->state.load(std::memory_order_relaxed); // fix this
        }
    };

    struct DState {
        size_t depth;
        Bucket **dir;  // allocated array with size 2^depth

    public:
        DState() : depth(1) {
            assert(0 < depth && depth < 20);
            dir = new Bucket *[POW(depth)];
            dir[0] = new Bucket();
            dir[1] = new Bucket();
            dir[0]->depth = 1;
            dir[1]->depth = 1;
            dir[0]->prefix = 0;
            dir[1]->prefix = 1;
        }

        inline size_t getDepth() const {
            return this->depth;
        }

        DState(const DState &d) : depth(d.depth) {
            dir = new Bucket *[POW(depth)];
            for (int i = 0; i < POW(depth); i++) {
                assert(d.dir[i]);
                dir[i] = d.dir[i];
            }
        }

        DState operator=(DState b) = delete;

        void EnlargeDir() {
            Bucket **next_dir = new Bucket *[POW(depth + 1)]();
            for (int i = 0; i < POW(depth); ++i) {
                next_dir[(i << 1) + 0] = dir[i];
                next_dir[(i << 1) + 1] = dir[i];
            }
//             delete[] dir; // fix this
            dir = next_dir;
            depth++;
        }

        ~DState() {
//            delete dir;
        }
    };


    /*** Global variables of the class goes below: ***/
    volatile std::atomic<DState *> ht;
    Operation help[NUMBER_OF_THREADS];
    unsigned long long opSeqnum[NUMBER_OF_THREADS]{}; // This will be an array of size N and each thread will only access to its memory space

    /*** Inner function section goes below: ***/


    void ApplyWFOp(Bucket *b, unsigned int id) {
        BigWord oldToggle;
        b->toggle.FlipBit(id); // mark as worked on by thread id

        for (int i = 0; i < 2; i++) {
            BState *oldBState = b->state.load(std::memory_order_relaxed);
            BState *nextBState = new BState(*oldBState); // copy constructor, pointer assignment
            oldToggle = b->toggle; // copy constructor using operator=

            for (unsigned int j = 0; j <= NUMBER_OF_THREADS; j++) {
                if (oldToggle.TestBit(j) == nextBState->applied.TestBit(j))
                    continue;
                assert(nextBState->results[j].seqnum >= 0 && help[j].seqnum > 0);
                if (nextBState->results[j].seqnum < help[j].seqnum) {
                    nextBState->results[j].status = ExecOnBucket(nextBState, help[j]);
                    if (nextBState->results[j].status != FAIL)
                        nextBState->results[j].seqnum = help[j].seqnum;
                }
            }
            nextBState->applied = oldToggle; // copy constructor using operator=

            if (std::atomic_compare_exchange_weak<BState *>(&b->state, &oldBState, nextBState)) {
                //   delete oldBState; // fix this
            } else {
                    delete nextBState;
            }
        }
    }

    Status_type ExecOnBucket(BState *b, Operation const &op) {

        int freeID = b->BucketAvailability();
        if (freeID == FULL_BUCKET) {
            return FAIL;
        } else {
            Triple c(op.hash, op.key, op.value);
            int updateID = b->GetItem(c);
            // case remove
            if (op.type == DEL) {
                if (updateID != NOT_FOUND) {
                    b->items[updateID].valid_item = false;
                }
                return TRUE;
            }
            // case insert or update
            if (updateID == NOT_FOUND) {
                if (op.type == INS) {
                    b->items[freeID] = c;
                }
            } else {
                if (op.type == INS) {
                    b->items[updateID] = c;
                }
            }
        }
        return TRUE;
    }

    Bucket **SplitBucket(Bucket *const b) { // returns 2 new Buckets
        const BState *const bs = b->state.load(std::memory_order_relaxed);
        // init and allocate 2 Buckets:
        Bucket **res = new Bucket *[2];
        BState *const bs0 = new BState(bs->results, b->toggle); // special constructor
        BState *const bs1 = new BState(bs->results, b->toggle);
        res[0] = new Bucket((b->prefix << 1) + 0, b->depth + 1, bs0, b->toggle);
        res[1] = new Bucket((b->prefix << 1) + 1, b->depth + 1, bs1, b->toggle);

        // split the items between the next buckets:
        for (int i = 0; i < BUCKET_SIZE; ++i) {
            assert(bs->items[i].valid_item); // bucket should be full for splitting
            bool inserted_ok = false; // here for debugging
            if (Prefix(bs->items[i].hash, res[0]->depth) == res[0]->prefix)
                inserted_ok = bs0->InsertItem(bs->items[i]);
            else
                inserted_ok = bs1->InsertItem(bs->items[i]);
            assert(inserted_ok);
        }
        return res;
    }

    void DirectoryUpdate(DState &d, Bucket *const *blist, Bucket *const old_bucket) {
        int b_index = 0;
        if (blist[b_index]->depth > d.depth) d.EnlargeDir();
        for (size_t e = 0; e < POW(d.depth); ++e) {
            // TODO: maybe use old_bucket somehow instead of moving one by one from zero?
            if (Prefix(e, blist[b_index]->depth, d.depth) == blist[b_index]->prefix) {
                assert(d.dir[e]->state.load(std::memory_order_relaxed)->BucketAvailability() == FULL_BUCKET);
                assert(old_bucket == d.dir[e]);
                d.dir[e] = blist[b_index];
                if (e + 1 < POW(d.depth) && Prefix(e + 1, blist[b_index]->depth, d.depth) != blist[b_index]->prefix) {
                    if (b_index == 0) b_index++; // blist[1] will always be right after blist[0]
                    else break; // early stop
                }
            }
        }
    }

    void ApplyPendingResize(DState &d, Bucket const &bFull) {
        for (int j = 0; j < NUMBER_OF_THREADS; ++j) {
            Operation const temp_help_j = help[j]; // assignment constructor
            if (temp_help_j.type != NONE && Prefix(temp_help_j.hash, bFull.depth) == bFull.prefix) {
                BState const &bs = *(bFull.state.load(std::memory_order_relaxed));
                assert(bs.results[j].seqnum >= 0 && temp_help_j.seqnum >= 0);
                if (bs.results[j].seqnum < temp_help_j.seqnum) {
                    Bucket *bDest = d.dir[Prefix(temp_help_j.hash, d.depth)];
                    BState *bsDest = bDest->state.load(std::memory_order_relaxed);
                    while (bsDest->BucketAvailability() == FULL_BUCKET) {
                        Bucket **const splitted = SplitBucket(bDest);
                        DirectoryUpdate(d, splitted, bDest);
//                        delete bDest; // fix this
                        delete[] splitted;
                        bDest = d.dir[Prefix(temp_help_j.hash, d.depth)];
                        bsDest = bDest->state.load(std::memory_order_relaxed);
                    }
//                    assert(help[j]->seqnum == temp_help_j->seqnum); // TODO: why this assert fails?
                    bsDest->results[j].status = ExecOnBucket(bsDest, temp_help_j);
                    bsDest->results[j].seqnum = temp_help_j.seqnum;
                }
            }
        }
    }

    void ResizeWF() {
        for (int k = 0; k < 2; ++k) {
            DState *oldD = ht.load(std::memory_order_relaxed);
            DState *nextD = new DState(*oldD);
            assert(nextD->dir[0]);

            for (int j = 0; j < NUMBER_OF_THREADS; ++j) {
                if (help[j].type != NONE) { // different from the paper cause we might have invalid op at help[j]
                    Bucket *b = nextD->dir[Prefix(help[j].hash, nextD->depth)];
                    BState const *bs = (b->state.load(std::memory_order_relaxed));
                    if (bs->BucketAvailability() == FULL_BUCKET && bs->results[j].seqnum < help[j].seqnum) {
                        ApplyPendingResize(*nextD, *b);
                    }
                }
            }

            if (std::atomic_compare_exchange_weak<DState *>(&ht, &oldD, nextD)) {
                return;
            } else {
                delete nextD;
            }
        }
    }

    uint32_t Prefix(xxh::hash_t<32> const hash, uint32_t const depth) const {
        assert(depth);
        int shift = SIZE_OF_HASH - depth;
        uint32_t mask = ~(uint32_t) ((1 << (shift)) - 1);
        auto prefix = (uint32_t) ((hash & mask) >> shift);
        return prefix;
    }

    uint32_t Prefix(size_t hash, uint32_t const depth, uint32_t start) const {
        // example in:  1111 1111
        // start = 5:   0001 1111
        // depth = 3:   0001 1100
        assert(start >= depth);

        uint32_t mask = (uint32_t) ((1 << (start)) - 1); // mask for the first (start) bits
        auto prefix = (uint32_t) (hash & mask) << (SIZE_OF_HASH - start) >> (SIZE_OF_HASH - depth); // clear bits bigger than start and smaller than depth
        return prefix;
    }

    bool MakeOp(xxh::hash_t<32> hashed_key, unsigned int const id) {
        // this is a joint function for insert and remove
        // operation to do is in help[id]
        assert(0 <= id && id < NUMBER_OF_THREADS);
        const BState *bstate;
        int run_times = 0;
        do {
            DState *htl = (ht.load(std::memory_order_relaxed));
            uint32_t hash_prefix = Prefix(hashed_key, htl->getDepth());

            ApplyWFOp(htl->dir[hash_prefix], id);

            htl = (ht.load(std::memory_order_relaxed));
            bstate = htl->dir[hash_prefix]->state.load(std::memory_order_relaxed);
            assert(bstate->results[id].seqnum >= 0 && opSeqnum[id] >= 0);
            if (bstate->results[id].seqnum != opSeqnum[id])
                ResizeWF();

            htl = ht.load(std::memory_order_relaxed);
            bstate = htl->dir[Prefix(hashed_key, htl->getDepth())]->state.load(std::memory_order_relaxed);
            ++run_times;
        }
        while (bstate->results[id].seqnum != opSeqnum[id]);
//        if (run_times > 1) std::cout << "" << run_times << "-"; // counts the failing ops
        return true;
    }

public:

    hashmap() {
        ht.store(new DState(), std::memory_order_relaxed);
        for (unsigned long long &i : opSeqnum) i = 0;
//        for (Operation &op : help) op.type = NONE;
    };

    hashmap(hashmap &) = delete;

    hashmap operator=(hashmap) = delete;

    ~hashmap() {
        DState *d = ht.load(std::memory_order_relaxed);
        // fix this
        delete d;
    }

    std::pair<bool, Value> lookup(Key const &key) const &{
        const void *kptr = &key;
        xxh::hash_t<32> hashed_key(xxh::xxhash<32>(kptr, sizeof(Key)));
        DState *htl = (ht.load(std::memory_order_relaxed));
        size_t hash_prefix = Prefix(hashed_key, htl->getDepth());
        BState const *bs = htl->dir[hash_prefix]->state.load(std::memory_order_relaxed);
        for (Triple t : bs->items) {
            if (t.valid_item && t.key == key) return {true, t.value};
        }
        return {false, Value()};
    }

    bool insert(Key const &key, Value const &value, unsigned int const id) {
        assert(0 <= id && id < NUMBER_OF_THREADS);
        ++opSeqnum[id];
        const void *kptr = &key;
        xxh::hash_t<32> hashed_key(xxh::xxhash<32>(kptr, sizeof(Key)));
        help[id] = Operation(INS, key, value, opSeqnum[id], hashed_key);
        return MakeOp(hashed_key, id);
    }

    void DebugPrintDir() const {
        std::cout << std::endl;
        auto htl = ht.load(std::memory_order_relaxed);
        for (int i = 0; i < POW(htl->depth); i++) {
            BState *bs = htl->dir[i]->state.load(std::memory_order_relaxed);
            std::cout << "Entries: [" << i << ",";
            while (i + 1 < POW(htl->depth) && htl->dir[i] == htl->dir[i + 1])
                i++;
            if (i + 1 < POW(htl->depth))
                assert(htl->dir[i]->depth != htl->dir[i + 1]->depth ||
                       htl->dir[i]->prefix != htl->dir[i + 1]->prefix);
            std::cout << i << "]\tpoints to bucket with prefix: ";
            for (int k = htl->dir[i]->depth - 1; k >= 0; --k)
                std::cout << ((htl->dir[i]->prefix >> k) & 1);
            std::cout << ".\tItems: " << std::endl;
            for (int j = 0; j < BUCKET_SIZE; j++) {
                if (bs->items[j] != nullptr) {
                    std::cout << "\t\t" << "(hash: "
                              << std::bitset<SIZE_OF_HASH>(bs->items[j]->hash)
                              << ")\t\tvalue: " << bs->items[j]->value
                              << "\tkey: " << bs->items[j]->key << std::endl;
                }
            }
        }
    }

    bool remove(Key const &key, unsigned int const id) {
        assert(0 <= id && id < NUMBER_OF_THREADS);
        ++opSeqnum[id];
        const void *kptr = &key;
        xxh::hash_t<32> hashed_key(xxh::xxhash<32>(kptr, sizeof(Key)));
        help[id] = Operation(DEL, key, opSeqnum[id], hashed_key);
        return MakeOp(hashed_key, id);
    }

};


#endif //EWRHT_HASHMAP_H












