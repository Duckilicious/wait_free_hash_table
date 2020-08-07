# Wait Free Extendible Hash Table

This repository contains an implementation of a wait free extendible hash table based on the following paper: [WFEXT](https://tropars.github.io/downloads/pdf/publications/spaa2018-FKR-WF_ext_hashing.pdf).

#### How it works
We will present a brief explanation on how this DS works.
There are 3 levels of hierarchy in this DS, DState, Buckets and BStates:

 - **DState** - Is the top level of the hierarchy it is essentially an array of pointers to buckets where each pointer is treated as a string of bits. In addition we hold a global pointer to the most recent DState at all times.
 - **Bucket** - Is the second in hierarchy and it contains a pointer to a BState, two status arrays which are use for the algorithm behind the DS, and is represented by 
a prefix of the bits that represent the pointer from the DState that points to it.
- **BState** - This is the last structure in the hierarchy and is where the user data will be stored. It contains a fixed size array of the user data, and similarly to the Bucket it also contains two fixed sized status arrays. 

Another important part of this DS is the help array which is an idea presented in [this paper](https://arxiv.org/pdf/1911.01676.pdf). It is used by a worker thread that is currently performing an operation on a bucket to do the work of another thread who was assigned to perform an operation on the same bucket.

To perform an operation we first announce it by assigning it to a thread slot in the help array. After that the thread which inesrted the operation tries to perform it.
We will describe the insert operation, the delete is similiar, and the lookup is trivial because of the way the algorithem works.
When a thread tries to insert a new item to a BState after it was announced it first finds the correct Bucket to insert it according to the key of the item inserted.
In the case where the BState isn't full it will copy the last BState, will add the item to the local copy, and will use atomic CAS to try and update the Bucket and it will announce in the help array that it has finished the operation so that another thread won't try to execute it as well.

In the case where the BState is full the thread will try to split the Bucket as long as we need so we will have space for the new item , if the Bucket cannot be splitted it means the the DState is too small, and we will then begin the resizing operation of the table. A thread will create a local copy of a DState that is double the size of the last one it saw, and will copy the entire directory into the new DState, and will then try to atomic CAS the old DState with the new one.

For a more detailed explaniation please read the the paper linked above.

The following picture is taken from the paper, and demonstrate the case of resizing:

![resize_example](https://github.com/Duckilicious/wait_free_hash_table/blob/master/images/resize_example.PNG)

#### Differnces from the paper
There are two main differnces in our implementation than what the paper describes the first is in the end of the operations insert/delete we don't return the value of the status in the results array in BState, but instead we check if the seqnum in the relevant result array slot is the same as the one in OperationSeqnum. 
We found that without this we get false positives on failed operation.

The second differnce that in the paper it is implied the insert can fail, we found that if insert fails (and from our expierements it does fail occasionally) there is a race condition. Let us demonstrate: Let T1 and T2 be two independent threads performing operations on the DS. T1 preforms insert and fails, in the meanwhile T2 is executing on the same bucket that T1 has failed on so it sees the operation T1 has failed to perform, T2 then executes the failed operation by sending it to ExecOnBucket in applied ApplyWFOp. During the execution of that operations a new operation to insert another element, but this time to a different bucket, is inserted into the help array by T1, this will cause T2 to insert an item to the wrong Bucket thus harming the algorithm correctness.

So in our implementation we don't allow a thread to fail an operation, it doesn't mean that it must be the same thread that has announced it that should perform it, but we don't allow it to announce another operation until that one he already announced as done.

#### Dependecies

We used shared_ptr and it's array functionality in C++ 17 and also [XXhash](https://github.com/Cyan4973/xxHash) is used as the hash function and the C++ implementation is done in C++ 17. Meaning C++ 17 is a must to use this project.

#### Compiling

to compile this project including our tests simply run in the src/ folder:

```sh 
$ g++ -pthread -std=c++17 tests.cpp
```
To use this in your own project simply add to your headers

```sh
#include "hashmap.h"
```

#### Example

Let us demonstrate a small example of initating the WFEXT performing an insert , a lookup and a remove.

```sh
#include "hashmap.h"
#include <iostream>

int main()  {
    hashmap<int, int> ht{};
    if(ht.insert(312, 0, 0)) //Key = 312, data = 0, thread_id = 0
        std::cout << "Item was inserted successfully";
    std::pair<bool, int> p = ht.lookup(312);
    if(p.first)
        std::cout << "Found data for key 312";
    ht.remove(312, 0); //Key = 312, thread_id = 0
    p = ht.lookup(312);
    if(!p.first)
        std::cout << "Item was removed successfully";
}
'''
#### Benchmarks

Coming soon.

