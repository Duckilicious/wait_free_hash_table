//
// Created by Alon on 30/03/2020.
//

#ifndef EWRHT_HASHMAP_H
#define EWRHT_HASHMAP_H

#define BUCKET_SIZE 2
#define NUMBER_OF_THREADS 64


template<typename Key, typename Value>
class hashmap {
    // private:
    struct Couple { // TODO: rename?
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
    };
    struct Result {
        Status_type status;
        int seqnum;
    };
    struct BState {
        Couple itmes[BUCKET_SIZE]; // TODO: fixed size number of items is 2?
        bool *applied; // array size n
        Result *results; // array size n
    };
    struct Bucket {
        int prefix; // TODO: bit-string saved as type int?
        int depth;
        BState *state;
        bool *toggle; // array size n
    };
    struct DState {
        int depth;
        Bucket *dir;  // array size 2^depth
    };

    // Global variables of the class:

    int n; // maximum number of threads. maybe better use #define?
    DState *ht;
    Operation *help; // array size n
    int opSeqnum; // TODO: variable should be private per thread (?)

    // Inner function section:

    bool ApplyWFOp(Bucket &b);

    Status_type ExecOnBucket(BState &b, Operation const &op);

    Bucket *SplitBucket(Bucket const &b); // returns 2 new Buckets

    bool DirectoryUpdate(DState &d, Bucket const blist[]);

    bool ApplyPendingResize(DState &d, Bucket &bFull);

    bool ResizeWF();


public:
    hashmap() : n(NUMBER_OF_THREADS) { // Constructor.
        int init_depth = 0; // should we receive expected size of the hashmap in order to make less
        ht = new DState; // TODO: init DState! I think it might be easier with DState class and constructors
        help = new Operation[n];
    };

    Value lookup(Key const &key) const& { // return type from lookup is "const&" ?
        DState htl = ht;
        int hashed_key = 0; // TODO: fix hash
        BState bs = htl.dir[hashed_key];
        for (Couple c : bs.items) {
            if (c.key == key) return c.value;
        }
        return 0; // TODO: fix (detailed at the doc form)
    }

    bool insert(Key const &key, Value const &value);

    bool remove(Key const &key); // delete is a saved word so I used remove

};


#endif //EWRHT_HASHMAP_H
