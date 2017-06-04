#include <cstdio>
#include <string>
#include <iostream>
#include <vector>
#include <string>
#include <memory>

#include "rocksdb/db.h"
#include "db/db_impl.h"
#include "util/string_util.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "util/mutexlock.h"
#include "move_benchmark.h"

#define TIER_SSD 0
#define TIER_HDD 1

using namespace rocksdb;
using namespace std;

// Below are fake paths. Actual paths could be something like:  /dev/SSD1, /dev/HDD4 etc.
std::string SSD_DB_path = "/tmp/SSD_on_rocks";
std::string HDD_DB_path = "/tmp/HDD_on_rocks";

vector<Slice> getKeyVector(int num_keys /* Total keys to be created */,
                           int size_key /* Size of each key */) {
    std::vector<Slice> keys;
    auto iter = keys.begin();

    // Create and populate the keys vector
    for (int i = 1; i < num_keys; ++i) {
        char* key = "key";
        char* data_ = (char*) calloc(num_keys, size_key * sizeof(char));

        memset(data_, 0, sizeof(char));
        memmove(data_, key, sizeof(char) * 3);

        char* ptr = data_ + (3 * sizeof(char));
        data_ = strcat(data_, std::to_string(i).c_str());
        data_ = strcat(data_, "\0");

        keys.emplace_back(Slice(data_));
    }
}

void printDB(DB* db, string message) {
    cout << message << endl;
    rocksdb::Iterator* it;
    it = db->NewIterator(rocksdb::ReadOptions());

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout << it->key().ToString()
             << " : "
             << it->value().ToString()
             << endl;
    }
}

int main(int argc, char** argv) {
    // Options
    Options options;
    options.create_if_missing = true;
    Status s;
    vector<Status> status_vec;
    vector<ColumnFamilyHandle*> cfh;
    int num_keys = 10000;
    int key_size = 1; // in bytes

    if(argc < 3) {
        std::cout << "\n *** Incorrect arguments *** "
                  << __FILE__ << " " << __FUNCTION__ << " " << __LINE__
                  << std::endl;
        return -1;
    } else {
        num_keys = atoi(argv[1]);
        key_size = atoi(argv[2]);
    }

    /****************************************************************************************************************/
    // Holds normal KV pairs in ssd
    DB* db_ssd;

    // Holds normal KV pairs in hdd
    DB* db_hdd;
    /****************************************************************************************************************/
    std::vector<ColumnFamilyDescriptor> column_families_hdd;

    // Default column family : must
    column_families_hdd.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));

    // Handles
    std::vector<ColumnFamilyHandle*> handles_hdd;

    // OPEN - HDD DB
    s = DB::Open(options, HDD_DB_path, column_families_hdd, &handles_hdd, &db_hdd);
    if (!s.ok()) {
        cerr << s.ToString() << endl;
        delete db_hdd;
    }
    /****************************************************************************************************************/
    std::vector<ColumnFamilyDescriptor> column_families_ssd;

    // Default column family : must
    column_families_ssd.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));

    // Handles
    std::vector<ColumnFamilyHandle*> handles_ssd;

    // OPEN - SSD DB
    s = DB::Open(options, SSD_DB_path, column_families_ssd, &handles_ssd, &db_ssd);
    if (!s.ok()) {
        cerr << s.ToString() << endl;
        delete db_ssd;
    }
    /****************************************************************************************************************/
    // PUT - HDD
    s = db_hdd->Put(WriteOptions(), handles_hdd[0], Slice("key1"), Slice("val1"));

    // PUTS - SSD (atomic)
    WriteBatch batch;
    batch.Put(handles_ssd[0], Slice("key2"), Slice("val2"));
    batch.Put(handles_ssd[0], Slice("key3"), Slice("val3"));
    batch.Put(handles_ssd[0], Slice("key4"), Slice("val4"));
    s = db_ssd->Write(WriteOptions(), &batch);
    assert(s.ok());
    /****************************************************************************************************************/

    std::vector<Slice> keys;
    std::vector<string> values;
    auto iter = keys.begin();

    /*
     *  Benchmark
     */
    keys = getKeyVector(num_keys);

    printDB(db_hdd, "\nBefore :: HDD");
    cout << endl;
    printDB(db_ssd, "\nBefore :: SSD");
    cout << endl;

    cout << "*****" << endl;

    // Time start
    auto start = std::chrono::high_resolution_clock::now();

    // MOVE
    // move(src_db, dest_db, dest_db_cf_handle, ReadOptions, WriteOptions, keys_vector)
    s = db_ssd->move(db_ssd, db_hdd, handles_hdd[0], ReadOptions(), WriteOptions(), keys);

    // Time end
    auto end = std::chrono::high_resolution_clock::now() - start;
    long long duration = std::chrono::duration_cast<std::chrono::microseconds> (end).count();
    cout << duration << endl;

    printDB(db_hdd, "\nAfter :: HDD");
    cout << endl;
    printDB(db_ssd, "\nAfter :: SSD");
    cout << endl;

    /****************************************************************************************************************/
    for (auto handle : handles_hdd) {
        delete handle;
    }

    for (auto handle : handles_ssd) {
        delete handle;
    }
    /****************************************************************************************************************/

    delete db_hdd;
    delete db_ssd;


    return 0;
}
