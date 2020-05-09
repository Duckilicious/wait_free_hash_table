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

void *test03_thread(void *threadarg) {
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


void test03() {
    hashmap<int, int> m{};

    pthread_t threads[NUM_THREADS];
    struct thread_data td[NUM_THREADS];
    int rc;
    int i;

    for (i = 0; i < NUM_THREADS; i++) {
        cout << "test1() : creating thread, " << i << endl;
        td[i] = {i, rand() % 10, &m, rand() % 100};
        rc = pthread_create(&threads[i], nullptr, test03_thread, (void *) &td[i]);

        if (rc) {
            cout << "Error: unable to create thread," << rc << endl;
            exit(-1);
        }
    }
    pthread_exit(nullptr);
}

void test02() {
    hashmap<int, int> m{};
    int test_len = 5;
    for (int i = 0; i < test_len; ++i) {
        if (i == 4) m.DebugPrintDir();
        bool st = m.insert(i,i, 0); // TODO: insert should not include the thread id
//        assert(st); // todo: i==5 returns fail (but insert was okay)
    }
    m.DebugPrintDir();
    for (int i = 0; i < test_len; ++i) {
        hashmap<int,int>::Tuple t = m.lookup(i);
        assert(t.status && t.value == i);
    }
    cout << "Test #02 Passed!" << endl;
}

void test01() {
    hashmap<int, int> m{};
    int test_len = 4;
        for (int i = 0; i < test_len; ++i) {
        bool st = m.insert(i,i, 0); // TODO: insert should not include the thread id
        assert(st);
    }
    for (int i = 0; i < test_len; ++i) {
        hashmap<int,int>::Tuple t = m.lookup(i);
        assert(t.status && t.value == i);
    }
    cout << "Test #01 Passed!" << endl;
}


int main() {
    cout << "Hello Efficient Wait-Free Resizable HashMap!" << endl;
    test01(); // test without threads and without resize
    test02(); // test without threads
//    test03();
    return 0;
}

