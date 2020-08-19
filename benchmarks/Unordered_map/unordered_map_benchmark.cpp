#include <iostream>
#include <pthread.h>
#include <unistd.h> // for sleep
#include <ctime>
#include <cstdint>
#include <fstream>
#include <unordered_map>
#include <atomic>
#include <cassert>

#define MAX_ELEMENTS_PER_THREAD_FOR_COMFORT_TEST (1000000)
#define KEY(id, k) (id * MAX_ELEMENTS_PER_THREAD_FOR_COMFORT_TEST + k)

using namespace std;

std::atomic_flag lock = ATOMIC_FLAG_INIT;

struct thread_data {
    int thread_id;
    unordered_map<int, int> *m;
    int number_to_insert;
    uint64_t number_of_insert_ops;
    uint64_t number_of_lookup_ops;
    int num_of_threads;
};


bool start_the_threads_global_flag;

void *thread_function(void *threadarg) {
    struct thread_data *params;
    params = (struct thread_data *) threadarg;
    unordered_map<int, int> &m = *(params->m); // reference assignment (no constructor)
    int id = params->thread_id;
    std::clock_t start;
    double duration;
    uint64_t insert_id = 0;
    uint64_t lookup_id = 0; 
    while (!start_the_threads_global_flag);
    start = std::clock();
    while((std::clock() - start) / ((double) CLOCKS_PER_SEC) <= (30 * params->num_of_threads)){
        while (lock.test_and_set(std::memory_order_acquire));  // acquire lock
        for (int i = 0; i < 1; ++i) {
            m.insert(make_pair(KEY(insert_id, i),KEY(insert_id++, i)));
            params->number_of_insert_ops++;
        }
        for (int i = 0; i < 9; ++i) {
            m.find(KEY(lookup_id++,i));
            params->number_of_lookup_ops++;
        }
        lock.clear(std::memory_order_release); // release lock
    }
    pthread_exit(nullptr);
    return nullptr;
}

void benchmark_unordered_map(int n_threads) {
    int num_threads = n_threads;
    start_the_threads_global_flag = false;
    unordered_map<int, int> m;

    pthread_t threads[num_threads];
    struct thread_data td[num_threads];

    for (int id = 0; id < num_threads; ++id) {
        td[id] = {id, &m, 1000000, 0, 0, num_threads};
        int rc = pthread_create(&threads[id], nullptr, thread_function, (void *) &td[id]);
        assert(rc == 0); // Error: unable to create thread
    }
    start_the_threads_global_flag = true;

    for (int id = 0; id < num_threads; ++id) {
        int ret = pthread_join(threads[id], nullptr);
    }

    //Write to file the results here
    std::ofstream outfile("test_for_unordered_map_num_of_threads_" + std::to_string(num_threads));
    outfile << "Thread ID, Number of insert_ops, Number of lookup ops" << std::endl;
    for(int id = 0; id < num_threads; ++id) {
        outfile << id << ", " << td[id].number_of_insert_ops << ", " << td[id].number_of_lookup_ops << std::endl;
    }
}

int main() {
    int n_threads;

    std::cout << "Please enter the amount of HW thread this machine supports:" << std::endl;
    std::cin >> n_threads;
    for(int i = 1; i <= n_threads; i++ ) {
        std::cout << "Starting Test: " << i << std::endl;
        benchmark_unordered_map(i);
        std::cout << "Done test number: " << i << std::endl;
    }
    std::cout << "Done all tests" << std::endl;
    return 0;
}