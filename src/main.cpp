#include <iostream>
#include "hashmap.h"
#include <pthread.h>
#include <unistd.h> // for sleep

#define MAX_ELEMENTS_PER_THREAD_FOR_COMFORT_TEST (10000)
#define KEY(id, k) (id * MAX_ELEMENTS_PER_THREAD_FOR_COMFORT_TEST + k)

using namespace std;

struct thread_data {
    int thread_id;
    hashmap<int, int> *m;
    int number_to_insert;
    int number_to_remove;
};

bool start_the_threads_global_flag;

void *thead_function(void *threadarg) {
    struct thread_data *params;
    params = (struct thread_data *) threadarg;
    hashmap<int, int> &m = *(params->m); // reference assignment (no constructor)
    int id = params->thread_id;

    assert(params->number_to_insert < MAX_ELEMENTS_PER_THREAD_FOR_COMFORT_TEST);
    while (!start_the_threads_global_flag);
    for (int i = 0; i < params->number_to_insert; ++i) {
        bool st = m.insert(KEY(id, i),KEY(id, i), id);
        assert(st);
    }
    for (int i = 0; i < params->number_to_insert; ++i) {
        std::pair<bool, int> t = m.lookup(KEY(id, i));
        if (!t.first) cout << "## ERROR FINDING " << KEY(id, i) << "! ##\n"; // for debugging
        assert(t.first && t.second == KEY(id, i));
    }
    for (int i = 0; i < params->number_to_remove; ++i) { // remove number_to_remove
        bool st = m.remove(KEY(id, i),id);
        assert(st);
    }
    pthread_exit(nullptr);
    return nullptr;
}

void test07() {
    start_the_threads_global_flag = false;
    static const int num_threads = 8;
    hashmap<int, int> m{};
    pthread_t threads[num_threads];
    struct thread_data td[num_threads];

    for (int id = 0; id < num_threads; ++id) {
        td[id] = {id, &m, 700 + rand() % 200, 500 + rand() % 150};
        int rc = pthread_create(&threads[id], nullptr, thead_function, (void *) &td[id]);
        assert(rc == 0); // Error: unable to create thread
    }
    start_the_threads_global_flag = true;
    for (unsigned long thread : threads) {
        int ret = pthread_join(thread, nullptr);
        assert(ret == 0);
    }
    for (int id = 0; id < num_threads; ++id) {
        for (int j = 0; j < td[id].number_to_remove; ++j) {
            std::pair<bool, int> t = m.lookup(KEY(id, j));
            assert(!t.first); // check removed ok
        }
        for (int j = td[id].number_to_remove; j < td[id].number_to_insert; ++j) {
            std::pair<bool, int> t = m.lookup(KEY(id, j));
            assert(t.first && t.second == KEY(id, j)); // check stayed okay
        }
    }
    cout << "Test #07 Finished!" << endl;
}

void test06() {
    start_the_threads_global_flag = true;
    static const int num_threads = 8;
    hashmap<int, int> m{};
    pthread_t threads[num_threads];
    struct thread_data td[num_threads];

    for (int id = 0; id < num_threads; ++id) {
        td[id] = {id, &m, 300, 200};
        int rc = pthread_create(&threads[id], nullptr, thead_function, (void *) &td[id]);
        assert(rc == 0); // Error: unable to create thread
        int ret = pthread_join(threads[id], nullptr);
        assert(ret == 0);
    }
    for (int id = 0; id < num_threads; ++id) {
        for (int j = 0; j < td[id].number_to_remove; ++j) {
            std::pair<bool, int> t = m.lookup(KEY(id, j));
            assert(!t.first); // check removed ok
        }
        for (int j = td[id].number_to_remove; j < td[id].number_to_insert; ++j) {
            std::pair<bool, int> t = m.lookup(KEY(id, j));
            assert(t.first && t.second == KEY(id, j)); // check stayed okay
        }
    }
    cout << "Test #06 Finished!" << endl;
}

void test05() {
    start_the_threads_global_flag = false;
    static const int num_threads = 16;
    hashmap<int, int> m{};

    pthread_t threads[num_threads];
    struct thread_data td[num_threads];

    for (int id = 0; id < num_threads; ++id) {
//        td[id] = {id, &m, (rand() % 800) + 70, -1};
        td[id] = {id, &m, 9000, -1};
        int rc = pthread_create(&threads[id], nullptr, thead_function, (void *) &td[id]);
        assert(rc == 0); // Error: unable to create thread
    }
    start_the_threads_global_flag = true;
    for (unsigned long thread : threads) {
        int ret = pthread_join(thread, nullptr);
        assert(ret == 0);
    }
    for (int id = 0; id < num_threads; ++id) {
        for (int j = 0; j < td[id].number_to_insert; ++j) {
            std::pair<bool, int> t = m.lookup(KEY(id, j));
            if (!t.first) {
                cout << "## ERROR FINDING " << KEY(id, j) << "! ##\n"; // for debugging
                /*int key = KEY(id, j);
                const void* kptr = &key;
                xxh::hash_t<32> hashed_key(xxh::xxhash<32>(kptr, sizeof(int)));
                cout << hashed_key << endl;
                m.DebugPrintDir();
                return;*/
            }
            assert(t.first && t.second == KEY(id, j));
        }
    }
    cout << "Test #05 Finished!" << endl;
}

void test04() {
    start_the_threads_global_flag = true;
    static const int num_threads = 8;
    hashmap<int, int> m{};

    pthread_t threads[num_threads];
    struct thread_data td[num_threads];

    for (int id = 0; id < num_threads; ++id) {
        td[id] = {id, &m, 300, -1};
        int rc = pthread_create(&threads[id], nullptr, thead_function, (void *) &td[id]);
        assert(rc == 0); // Error: unable to create thread
        int ret = pthread_join(threads[id], nullptr);
        assert(ret == 0);
    }
    for (int id = 0; id < num_threads; ++id) {
        for (int j = 0; j < td[id].number_to_insert; ++j) {
            std::pair<bool, int> t = m.lookup(KEY(id, j));
            assert(t.first && t.second == KEY(id, j));
        }
    }
    cout << "Test #04 Finished!" << endl;
}

void test03() {
    hashmap<int, int> m{};
    int test_len = 315;
    for (int i = 0; i < test_len; ++i) {
        bool st = m.insert(i,i, 0);
        assert(st);
    }
    for (int i = 0; i < test_len; ++i) {
        std::pair<bool, int> t = m.lookup(i);
        assert(t.first && t.second == i);
        bool st = m.remove(i,0);
        t = m.lookup(i);
        assert(st && !t.first);
    }
    cout << "Test #03 Finished!" << endl;
}

void test02() {
    hashmap<int, int> m{};
    int test_len = 143;
    for (int i = 0; i < test_len; ++i) {
        bool st = m.insert(i, i, 0);
        assert(st);
    }
    for (int i = 0; i < test_len; ++i) {
        std::pair<bool, int> t = m.lookup(i);
        assert(t.first && t.second == i);
    }
    cout << "Test #02 Finished!" << endl;
}

void test01() {
    hashmap<int, int> m{};
    const int test_len = BUCKET_SIZE;
    for (int i = 0; i < test_len; ++i) {
        bool st = m.insert(i,i, 0);
        assert(st);
    }
    for (int i = 0; i < test_len; ++i) {
        std::pair<bool, int> t = m.lookup(i);
        assert(t.first && t.second == i);
        m.remove(i,0);
        t = m.lookup(i);
        assert(!t.first);
    }
    for (int i = 0; i < test_len; ++i) {
        std::pair<bool, int> t = m.lookup(i);
        assert(!t.first);
        m.remove(i,0);
        assert(!t.first);
    }
    cout << "Test #01 Finished!" << endl;
}

int main() {
    cout << "Hello Efficient Wait-Free Resizable Hash Table!" << endl;
    start_the_threads_global_flag = true;
    test01(); // test without threads and without resize
    test02(); // test without threads and with resize
    test03(); // test without threads and with resize and remove
    test04(); // test insert with threads running separately
    test05(); // test insert with threads running in parallel
    test06(); // test remove with threads running separately
    test07(); // test remove with threads running in parallel

    return 0;
}

