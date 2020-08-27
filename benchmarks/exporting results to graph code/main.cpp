#include <iostream>
#include <string>
#include <iostream>
#include <vector>
#include <fstream>
#include <cassert>
//#include <filesystem>
#include "dirent.h"

//namespace fs = std::experimental::filesystem;
using namespace std;

vector<int> insert_res;
vector<int> lookup_res;

void open_file(std::string path) {
    // attach an input stream to the wanted file
    ifstream input_stream(path);
    if (!input_stream) cerr << "Can't open input file: " << path << endl;
    string line;
    getline(input_stream, line); // title row
    long long insert = 0, lookup = 0, counter = 0;

    // extract all the text from the input file
    while (getline(input_stream, line)) {
        // use line
        int k = line.find(' ');
        line = line.erase(0,line.find(' ') + 1);
        string insert_string = line.substr(0,line.find(','));
        string lookup_string = line.substr(line.find(' '));

        insert += stoll(insert_string);
        lookup += stoll(lookup_string);
        ++counter;
    }
    insert_res[counter] = insert;
    lookup_res[counter] = lookup;
}

void open_folder(std::string path) {
    const char *cpath = path.c_str();
//    cout << path;
    DIR *dir;
    struct dirent *ent;
    insert_res = vector<int>(129, 0);
    lookup_res = vector<int>(129, 0);
    if ((dir = opendir(cpath)) != NULL) {
        /* print all the files and directories within directory */
        assert(dir);
        while ((ent = readdir(dir)) != NULL) {
//            printf ("%s\n", ent->d_name);
            assert(ent);
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            open_file(path + ent->d_name);

        }
        closedir(dir);

        cout << path.substr(path.find("benchmarks")) << ",insert,";
        for (int i = 1; i < 129; ++i) {
            cout << insert_res[i]/30 << ',';
        }
        cout << endl;

        cout << path.substr(path.find("benchmarks")) << ",lookup,";
        for (int i = 1; i < 129; ++i) {
            cout << lookup_res[i]/30 << ',';
        }
        cout << endl;

        cout << path.substr(path.find("benchmarks")) << ",tot,";
        for (int i = 1; i < 129; ++i) {
            cout << insert_res[i]/30 + lookup_res[i]/30 << ',';
        }
        cout << endl;
    } else {
        /* could not open directory */
        perror("");
    }
}


int main() {
    std::cout << "Hello, World!" << std::endl;
    string base = R"(C:\Users\alonl\project-benchmarks\)";

    cout << "kind,op,";
    for (int i = 1; i < 129; ++i) {
        cout << i << ',';
    }
    cout << ',' << endl;

    open_folder(base + R"(libcukoo\benchmark_ratio_1_1\)");
    open_folder(base + R"(Unordered_map\benchmark_ratio_1_1\)");
    open_folder(base + R"(WFEXT\benchmark_ratio_1_to_1\)");
    open_folder(base + R"(libcukoo\benchmark_ratio_1_9\)");
    open_folder(base + R"(Unordered_map\benchmark_ratio_9_1\)");
    open_folder(base + R"(WFEXT\benchmark_ratio_9_to_1\)");

    return 0;
}
