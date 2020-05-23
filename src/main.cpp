#include <iostream>
#include "hashmap.h"
#include <pthread.h>
#include <unistd.h> // for sleep

#define KEY(id, k) (id * 100 + k)

using namespace std;

struct thread_data {
    int thread_id;
    hashmap<int, int> *m;
    int number_to_insert;
    int number_to_remove;
};


void *test04_thread(void *threadarg) {
    struct thread_data *params;
    params = (struct thread_data *) threadarg;
    hashmap<int, int> &m = *(params->m); // reference assignment (no constructor)
    int id = params->thread_id;

    for (int i = 0; i < params->number_to_insert; ++i) {
        bool st = m.insert(KEY(id, i),KEY(id, i), id);
        assert(st);
    }
    for (int i = 0; i < params->number_to_insert; ++i) {
        hashmap<int,int>::Tuple t = m.lookup(KEY(id, i));
        assert(t.status && t.value == KEY(id, i));
    }
    for (int i = 0; i < params->number_to_remove; ++i) { // remove number_to_remove
        bool st = m.remove(KEY(id, i),id);
        hashmap<int,int>::Tuple t = m.lookup(KEY(id, i));
        assert(st && !t.status);
    }
    pthread_exit(nullptr);
    return nullptr;
}

void test07() {
    static const int num_threads = 2; // when test passes okay increase to 8 todo
    hashmap<int, int> m{};
    pthread_t threads[num_threads];
    struct thread_data td[num_threads];

    for (int id = 0; id < num_threads; ++id) {
//        td[id] = {id, &m, 20 + rand() % 70, rand() % 10}; // upgrade this test using this line todo
        td[id] = {id, &m, 5, 2};
        int rc = pthread_create(&threads[id], nullptr, test04_thread, (void *) &td[id]);
        assert(rc == 0); // Error: unable to create thread
    }
    for (unsigned long thread : threads) {
        int ret = pthread_join(thread, nullptr);
        assert(ret == 0);
    }
    for (int id = 0; id < num_threads; ++id) {
        for (int j = 0; j < td[id].number_to_remove; ++j) {
            hashmap<int,int>::Tuple t = m.lookup(KEY(id, j));
            assert(!t.status); // check removed ok
        }
        for (int j = td[id].number_to_remove; j < td[id].number_to_insert; ++j) {
            hashmap<int,int>::Tuple t = m.lookup(KEY(id, j));
            assert(t.status && t.value == KEY(id, j)); // check stayed okay
        }
    }
    cout << "Test #07 Passed!" << endl;
}

void test06() {
    static const int num_threads = 8; // when test passes okay increase to 8 todo
    hashmap<int, int> m{};
    pthread_t threads[num_threads];
    struct thread_data td[num_threads];

    for (int id = 0; id < num_threads; ++id) {
        td[id] = {id, &m, 30, 10};
        int rc = pthread_create(&threads[id], nullptr, test04_thread, (void *) &td[id]);
        assert(rc == 0); // Error: unable to create thread
        int ret = pthread_join(threads[id], nullptr);
        assert(ret == 0);
    }
    for (int id = 0; id < num_threads; ++id) {
        for (int j = 0; j < td[id].number_to_remove; ++j) {
            hashmap<int,int>::Tuple t = m.lookup(KEY(id, j));
            assert(!t.status); // check removed ok
        }
        for (int j = td[id].number_to_remove; j < td[id].number_to_insert; ++j) {
            hashmap<int,int>::Tuple t = m.lookup(KEY(id, j));
            assert(t.status && t.value == KEY(id, j)); // check stayed okay
        }
    }
    cout << "Test #06 Passed!" << endl;
}


void test05() {
    static const int num_threads = 2; // when test passes okay increase to 8 todo
    hashmap<int, int> m{};

    pthread_t threads[num_threads];
    struct thread_data td[num_threads];

    for (int id = 0; id < num_threads; ++id) {
//        td[id] = {id, &m, 30 + rand() % 60, -1}; // upgrade this test using this line todo
        td[id] = {id, &m, 80, -1};
        int rc = pthread_create(&threads[id], nullptr, test04_thread, (void *) &td[id]);
        assert(rc == 0); // Error: unable to create thread
    }
    for (unsigned long thread : threads) {
        int ret = pthread_join(thread, nullptr);
        assert(ret == 0);
    }
    for (int id = 0; id < num_threads; ++id) {
        for (int j = 0; j < td[id].number_to_insert; ++j) {
            hashmap<int,int>::Tuple t = m.lookup(KEY(id, j));
            assert(t.status && t.value == KEY(id, j));
        }
    }
    cout << "Test #05 Passed!" << endl;
}

void test04() {
    static const int num_threads = 8;
    hashmap<int, int> m{};

    pthread_t threads[num_threads];
    struct thread_data td[num_threads];

    for (int id = 0; id < num_threads; ++id) {
        td[id] = {id, &m, 15, -1};
        int rc = pthread_create(&threads[id], nullptr, test04_thread, (void *) &td[id]);
        assert(rc == 0); // Error: unable to create thread
        int ret = pthread_join(threads[id], nullptr);
        assert(ret == 0);
    }
    for (int id = 0; id < num_threads; ++id) {
        for (int j = 0; j < td[id].number_to_insert; ++j) {
            hashmap<int,int>::Tuple t = m.lookup(KEY(id, j));
            assert(t.status && t.value == KEY(id, j));
        }
    }
    cout << "Test #04 Passed!" << endl;
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
        bool st = m.remove(i,0);
        t = m.lookup(i);
        assert(st && !t.status);
    }
//    m.DebugPrintDir(); // todo: remove op enlarges the dir.. should it happen?
    cout << "Test #03 Passed!" << endl;
}

void test02() {
    hashmap<int, int> m{};
    int test_len = 143;
    for (int i = 0; i < test_len; ++i) {
        bool st = m.insert(i,i, 0);
        assert(st);
    }
    for (int i = 0; i < test_len; ++i) {
        hashmap<int,int>::Tuple t = m.lookup(i);
        assert(t.status && t.value == i);
    }
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
    test03(); // test without threads and with resize and remove // todo check if dir should increase when remove
    test04(); // test insert with threads running separately
    test05(); // test insert with threads running in parallel // todo code fails when parallel
//    test06(); // test remove with threads running separately
//    test07(); // test remove with threads running in parallel
    return 0;
}

