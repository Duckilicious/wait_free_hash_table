#include <iostream>
#include "hashmap.h"
#include <pthread.h>

using namespace std;
#define NUM_THREADS 1

struct thread_data {
    int thread_id;
    int sleep_time;
    hashmap<int, int> *m;
    int number_of_items_to_enter;
};

void *test04_thread(void *threadarg) {
    struct thread_data *params;
    params = (struct thread_data *) threadarg;
    hashmap<int, int> &m = *(params->m); // reference assignment (no constructor)

    cout << "Thread ID : " << params->thread_id;
    // sleep(params->sleep_time);

    for (int i = 0; i < params->number_of_items_to_enter; ++i) {
        m.insert(i,i, params->thread_id); // TODO: insert should not include the thread id
    }
    for (int i = 0; i < params->number_of_items_to_enter; ++i) {
        hashmap<int,int>::Tuple t = m.lookup(i);
        assert(t.status && t.value == i);
    }

    pthread_exit(nullptr);
    return nullptr;
}


void test04() {
    hashmap<int, int> m{};

    pthread_t threads[NUM_THREADS];
    struct thread_data td[NUM_THREADS];
    int rc;
    int i;

    for (i = 0; i < NUM_THREADS; i++) {
        cout << "test1() : creating thread, " << i << endl;
        td[i] = {i, rand() % 10, &m, rand() % 100};
        rc = pthread_create(&threads[i], nullptr, test04_thread, (void *) &td[i]);

        if (rc) {
            cout << "Error: unable to create thread," << rc << endl;
            exit(-1);
        }
    }
    pthread_exit(nullptr);
}

void test03() {
    hashmap<int, int> m{};
    int test_len = 3;
    for (int i = 0; i < test_len; ++i) {
        bool st = m.insert(i,i, 0);
        assert(st);
    }
//    m.DebugPrintDir();
    for (int i = 0; i < test_len; ++i) {
        hashmap<int,int>::Tuple t = m.lookup(i);
        assert(t.status && t.value == i);
        m.remove(i,0);
        t = m.lookup(i);
        assert(!t.status);
    }
//    m.DebugPrintDir(); // todo: remove op enlarges the dir.. should it happen?
    cout << "Test #03 Passed!" << endl;
}

void test02() {
    hashmap<int, int> m{};
    int test_len = 27; // todo: 27 is okay, 28 and higher makes problems
    for (int i = 0; i < test_len; ++i) {
        bool st = m.insert(i,i, 0);
        assert(st);
    }
//    m.DebugPrintDir();
    for (int i = 0; i < test_len; ++i) {
        hashmap<int,int>::Tuple t = m.lookup(i);
        assert(t.status && t.value == i);
    }
//    m.DebugPrintDir();
    cout << "Test #02 Passed!" << endl;
}

void test01() {
    hashmap<int, int> m{};
    int test_len = BUCKET_SIZE;
        for (int i = 0; i < test_len; ++i) {
        bool st = m.insert(i,i, 0); // TODO: insert should not include the thread id
        assert(st);
    }
//    m.DebugPrintDir();
    for (int i = 0; i < test_len; ++i) {
        hashmap<int,int>::Tuple t = m.lookup(i);
        assert(t.status && t.value == i);
        m.remove(i,0);
        t = m.lookup(i);
        assert(!t.status);
    }
    for (int i = 0; i < test_len; ++i) {
        hashmap<int,int>::Tuple t = m.lookup(i);
        assert(!t.status);
        m.remove(i,0);
        assert(!t.status);
    }
    cout << "Test #01 Passed!" << endl;
}

int main() {
    cout << "Hello Efficient Wait-Free Resizable HashMap!" << endl;
    test01(); // test without threads and without resize
    test02(); // test without threads and with resize
    test03(); // test without threads and with resize and remove
//    test04(); // test with threads
    return 0;
}

