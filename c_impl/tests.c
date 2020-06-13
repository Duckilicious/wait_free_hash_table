//
// Created by Tal on 13/06/2020.
//

#include <pthread.h>
#include "WFEXTH.h"

#define KEY(id, k) (id * 1000 + k)


struct thread_data {
    int thread_id;
    int number_to_insert;
    int number_to_remove;
};


void *test04_thread(void *threadarg) {
    struct thread_data *params;
    params = (struct thread_data *) threadarg;
    int id = params->thread_id;

    for (uint64_t i = 0; i < params->number_to_insert; ++i) {
        bool st = insert((uint64_t) KEY(id, i), KEY(id, i), id);
        assert(st);
        /*
        if(Lookup((uint64_t) KEY(id, i)).value != KEY(id, i)) {
            printf("failed to find key  %d\n", KEY(id, i));
        }*/

    }
    //for (int i = 0; i < params->number_to_insert; ++i) {
        /*
        if(Lookup((uint64_t) KEY(id, i)).value != KEY(id, i)) {
            printf("failed to find key  %d\n", KEY(id, i));
        }
         */
  //  }
    pthread_exit(NULL);
    return NULL;
}


void test05() {
    static const int num_threads = 8; // when test passes okay increase to 8 todo

    pthread_t threads[num_threads];
    struct thread_data td[num_threads];

    for (int id = 0; id < num_threads; ++id) {
//        td[id] = {id, &m, 30 + rand() % 60, -1}; // upgrade this test using this line todo
        td[id].thread_id = id;
        td[id].number_to_insert = 500;
        td[id].number_to_remove = 0;
        int rc = pthread_create(&threads[id], NULL, test04_thread, (void *) &td[id]);
        assert(rc == 0); // Error: unable to create thread
    }
    for (int threadid = 0; threadid < num_threads; threadid++) {
        int ret = pthread_join(threads[threadid], NULL);
        assert(ret == 0);
    }
    for (int id = 0; id < num_threads; ++id) {
        for (int j = 0; j < td[id].number_to_insert; ++j) {
            if(Lookup((uint64_t) KEY(id,j)).value != KEY(id, j)) {
                printf("failed to find key  %d\n", KEY(id, j));
            }
        }
    }
    printf("Test #05 Passed!");
}

int test1(){
    for(uint64_t i = 0; i < 1000; i++) {
        insert(i, i, 0);
        assert(Lookup(i).value == i);
    }
    for(uint64_t i = 0; i < 1000; i++) {
        assert(Lookup(i).value == i);
    }
    printf("test1 passed\n");
    return 0;
}


int main() {
    initHashTable();
    //test1();
    test05();
    return 0;
}

