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
#include <cassert>
#include <memory>
#include <bitset> // TODO using for the print only
#include "xxhash/include/xxhash.hpp"

//std used functions
using std::shared_ptr;
using std::make_shared;
using std::atomic_compare_exchange_weak;
using std::atomic_store;
using std::atomic_load;

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
        Result results[NUMBER_OF_THREADS];
        BigWord applied;

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

        /*Returns the free entry if exists, else -1 for full bucket */
        int BucketAvailability() const {
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
        shared_ptr<BState> state;
        BigWord toggle;

    public:
        Bucket() : prefix(), depth(), toggle() {
            atomic_store(&state, make_shared<BState>());
        }

        Bucket(const Bucket &b) = delete;

        explicit Bucket(uint32_t p, size_t d, shared_ptr<BState> s, BigWord t)
            : prefix(p), depth(d), toggle(t) {
            atomic_store(&state, s);
        }

        Bucket operator=(Bucket b) = delete;

        ~Bucket() = default;
    };

    struct Bucket_ptr {
        shared_ptr<Bucket> b_ptr;
    };

    struct DState {
        size_t depth;
        shared_ptr<Bucket_ptr[]> dir;  // Array of shared_ptr

    public:
        DState() : depth(1) {
            assert(0 < depth && depth < 20);
            shared_ptr<Bucket_ptr[]> dir_temp(new Bucket_ptr[POW(depth)]);
            dir = dir_temp;
            for (int i = 0; i < POW(depth); i++) {
                shared_ptr<Bucket> temp(new Bucket());
                dir[i].b_ptr = temp;
                dir[i].b_ptr->depth = 1;
                dir[i].b_ptr->prefix = i;
            }
        }

        inline size_t getDepth() const {
            return this->depth;
        }

        DState(const DState &d) : depth(d.depth) {
            shared_ptr<Bucket_ptr[]> dir_temp(new Bucket_ptr[POW(depth)]);
            dir = dir_temp;
            for (int i = 0; i < POW(depth); i++) {
                dir[i].b_ptr = d.dir[i].b_ptr;
            }
        }

        DState operator=(DState b) = delete;

        void EnlargeDir() {
            shared_ptr<Bucket_ptr[]> next_dir(new Bucket_ptr[POW(depth + 1)]);
            for (int i = 0; i < POW(depth); ++i) {
                next_dir[(i << 1) + 0].b_ptr = dir[i].b_ptr;
                next_dir[(i << 1) + 1].b_ptr = dir[i].b_ptr;
            }
            dir = next_dir;
            depth++;
        }

        ~DState() = default;
    };


    /*** Global variables of the class goes below: ***/
    /**@ht - a pointer to the most recent DState.
     * @help - an array of size N and each thread will only access its own space.
     * @opSeqnum - an array of size N each thread holds a counter that represent the
     * amount of operations it has done.
     * **/
    shared_ptr<DState> ht;
    Operation help[NUMBER_OF_THREADS];
    unsigned long long opSeqnum[NUMBER_OF_THREADS]{};

    /*** Inner function section goes below: ***/


    void ApplyWFOp(Bucket_ptr b, unsigned int id) {
        BigWord oldToggle;
        b.b_ptr->toggle.FlipBit(id); // mark as worked on by thread id

        for (int i = 0; i < 2; i++) {
            shared_ptr<BState> oldBState = atomic_load(&b.b_ptr->state);
            shared_ptr<BState> nextBState(new BState(*oldBState)); // copy
            // constructor, pointer assignment
            oldToggle = b.b_ptr->toggle; // copy constructor using operator=

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

            atomic_compare_exchange_weak(&b.b_ptr->state,
                    &oldBState, nextBState);
        }
    }

    Status_type ExecOnBucket(shared_ptr<BState> b, Operation const &op) {

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

    shared_ptr<Bucket_ptr[]> SplitBucket(Bucket_ptr const b) { // returns 2 new Buckets
        const shared_ptr<BState> bs = atomic_load(&b.b_ptr->state);
        shared_ptr<Bucket_ptr[]> res(new Bucket_ptr[2]);

        shared_ptr<BState> bs0(new BState(bs->results, b.b_ptr->toggle));
        shared_ptr<BState> bs1(new BState(bs->results, b.b_ptr->toggle));
        shared_ptr<Bucket> res0(new Bucket((b.b_ptr->prefix << 1) + 0, b.b_ptr->depth + 1, bs0, b.b_ptr->toggle));
        shared_ptr<Bucket> res1(new Bucket((b.b_ptr->prefix << 1) + 1, b.b_ptr->depth + 1, bs1, b.b_ptr->toggle));
        res[0].b_ptr = res0;
        res[1].b_ptr = res1;

        // split the items between the next buckets
        for (int i = 0; i < BUCKET_SIZE; ++i) {
            assert(bs->items[i].valid_item); // bucket should be full for splitting
            if (Prefix(bs->items[i].hash, res[0].b_ptr->depth) == res[0].b_ptr->prefix)
                bs0->InsertItem(bs->items[i]);
            else
                bs1->InsertItem(bs->items[i]);
        }
        return res;
    }

    void DirectoryUpdate(DState &d, shared_ptr<Bucket_ptr[]> blist, Bucket_ptr const old_bucket) {
        int b_index = 0;
        if (blist[b_index].b_ptr->depth > d.depth) d.EnlargeDir();
        for (size_t e = 0; e < POW(d.depth); ++e) {
            if (Prefix(e, blist[b_index].b_ptr->depth, d.depth) == blist[b_index].b_ptr->prefix) {
                assert(atomic_load(&d.dir[e].b_ptr->state)->BucketAvailability() == FULL_BUCKET);
                d.dir[e] = blist[b_index];
                if (e + 1 < POW(d.depth) && Prefix(e + 1, blist[b_index].b_ptr->depth, d.depth) != blist[b_index].b_ptr->prefix) {
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
                BState const &bs = *(atomic_load(&bFull.state));
                assert(bs.results[j].seqnum >= 0 && temp_help_j.seqnum >= 0);
                if (bs.results[j].seqnum < temp_help_j.seqnum) {
                    Bucket_ptr bDest = d.dir[Prefix(temp_help_j.hash, d.depth)];
                    shared_ptr<BState> bsDest = atomic_load(&bDest.b_ptr->state);
                    while (bsDest->BucketAvailability() == FULL_BUCKET) {
                        shared_ptr<Bucket_ptr[]> const splitted = SplitBucket(bDest);
                        DirectoryUpdate(d, splitted, bDest);
                        bDest = d.dir[Prefix(temp_help_j.hash, d.depth)];
                        bsDest = atomic_load(&bDest.b_ptr->state);
                    }
                    bsDest->results[j].status = ExecOnBucket(bsDest, temp_help_j);
                    bsDest->results[j].seqnum = temp_help_j.seqnum;
                }
            }
        }
    }

    void ResizeWF() {
        for (int k = 0; k < 2; ++k) {
            shared_ptr<DState> oldD = atomic_load(&ht);
            shared_ptr<DState> nextD(new DState(*oldD));

            for (int j = 0; j < NUMBER_OF_THREADS; ++j) {
                if (help[j].type != NONE) { // different from the paper cause we might have invalid op at help[j]
                    Bucket_ptr b = nextD->dir[Prefix(help[j].hash, nextD->depth)];
                    shared_ptr<BState> bs = (atomic_load(&b.b_ptr->state));
                    if (bs->BucketAvailability() == FULL_BUCKET && bs->results[j].seqnum < help[j].seqnum) {
                        ApplyPendingResize(*nextD, *b.b_ptr);
                    }
                }
            }

            if (atomic_compare_exchange_weak(&ht, &oldD, nextD))
                return;
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
        shared_ptr<BState> bstate;
        int run_times = 0;
        do {
            shared_ptr<DState> htl = atomic_load(&ht);
            uint32_t hash_prefix = Prefix(hashed_key, htl->getDepth());

            ApplyWFOp(htl->dir[hash_prefix], id);

            htl = atomic_load(&ht);
            bstate = atomic_load(&htl->dir[hash_prefix].b_ptr->state);
            assert(bstate->results[id].seqnum >= 0 && opSeqnum[id] >= 0);
            if (bstate->results[id].seqnum != opSeqnum[id])
                ResizeWF();

            htl = atomic_load(&ht);
            bstate = atomic_load(&htl->dir[Prefix(hashed_key, htl->getDepth())].b_ptr->state);
            ++run_times;
        }
        while (bstate->results[id].seqnum != opSeqnum[id]);
        return true;
    }

public:

    hashmap() {
        atomic_store(&ht, make_shared<DState>());
        for (unsigned long long &i : opSeqnum) i = 0;
    };

    hashmap(hashmap &) = delete;

    hashmap operator=(hashmap) = delete;

    ~hashmap() = default;

    std::pair<bool, Value> lookup(Key const &key) const &{
        const void *kptr = &key;
        xxh::hash_t<32> hashed_key(xxh::xxhash<32>(kptr, sizeof(Key)));
        shared_ptr<DState> htl = atomic_load(&ht);
        size_t hash_prefix = Prefix(hashed_key, htl->getDepth());
        shared_ptr<BState> bs = atomic_load(&htl->dir[hash_prefix].b_ptr->state);
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
        auto htl = atomic_load(&ht);
        for (int i = 0; i < POW(htl->depth); i++) {
            BState *bs = atomic_load(&htl->dir[i].b_ptr->state);
            std::cout << "Entries: [" << i << ",";
            while (i + 1 < POW(htl->depth) && htl->dir[i] == htl->dir[i + 1])
                i++;
            if (i + 1 < POW(htl->depth))
                assert(htl->dir[i].b_ptr->depth != htl->dir[i + 1].b_ptr->depth ||
                       htl->dir[i].b_ptr->prefix != htl->dir[i + 1].b_ptr->prefix);
            std::cout << i << "]\tpoints to bucket with prefix: ";
            for (int k = htl->dir[i].b_ptr->depth - 1; k >= 0; --k)
                std::cout << ((htl->dir[i].b_ptr->prefix >> k) & 1);
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












