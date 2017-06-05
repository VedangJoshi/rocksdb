
#include <cstdio>
#include <string>
#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <ctime>
#include <chrono>

#include "rocksdb/db.h"
#include "db/db_impl.h"
#include "util/string_util.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "util/mutexlock.h"

using namespace rocksdb;
using namespace std;

// Below are fake paths. Actual paths could be something like:  /dev/SSD1, /dev/HDD4 etc.
std::string SSD_DB_path = "/tmp/SSD_on_rocks";
std::string HDD_DB_path = "/tmp/HDD_on_rocks";

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
    int num_keys = 1;
    int key_size = 1; // in bytes
    int val_size = 1; // in bytes

    if(argc < 3) {
        std::cout << "\n *** Incorrect arguments *** "
                  << __FILE__ << " " << __FUNCTION__ << " " << __LINE__
                  << std::endl;
        return -1;
    } else {
        num_keys = atoi(argv[1]);
        key_size = atoi(argv[2]);
        val_size = atoi(argv[3]);
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

    std::vector<Slice> keys;

    // PUT - SSD
    // Create and populate the keys vector
    for (int i = 1; i < num_keys; ++i) {
        char* key = "key";
        char* key_data_ = (char*) calloc(num_keys, key_size * sizeof(char));
        memset(key_data_, 0, sizeof(char));
        memmove(key_data_, key, sizeof(char) * 3);
        key_data_ = strcat(key_data_, std::to_string(i).c_str());
        key_data_ = strcat(key_data_, "\0");

        // Also add to the key vector, later used for moving keys around
        keys.emplace_back(Slice(key_data_));

        char* val = "val";
        char* value_data_ = (char*) calloc(num_keys, val_size * sizeof(char));
        memset(value_data_, 0, sizeof(char));
        memmove(value_data_, val, sizeof(char) * 3);
        value_data_ = strcat(value_data_, std::to_string(i).c_str());
        value_data_ = strcat(value_data_, "\0");

        //cout << key_data_ << " : " << value_data_ << endl;
        s = db_ssd->Put(WriteOptions(), handles_hdd[0], Slice(key_data_), Slice(value_data_));
        assert(s.ok());
    }

    /****************************************************************************************************************/

    std::vector<string> values;

    /*
     *  Benchmark
     */

    //printDB(db_hdd, "\nBefore :: HDD");
    //cout << endl;
    //printDB(db_ssd, "\nBefore :: SSD");
    //cout << endl;

    //cout << "*****" << endl;

    // Time start
    auto start = std::chrono::steady_clock::now();
    //cout << "start: " << start.time_since_epoch();

    // MOVE
    // move(src_db, dest_db, dest_db_cf_handle, ReadOptions, WriteOptions, keys_vector)
    s = db_ssd->move(db_ssd, db_hdd, handles_hdd[0], ReadOptions(), WriteOptions(), keys);

    // Time end
    auto end = std::chrono::steady_clock::now();

    cout << "Time: " << std::chrono::duration_cast<chrono::microseconds> (end - start).count() << endl;

    // printDB(db_hdd, "\nAfter :: HDD");
    // cout << endl;
    // printDB(db_ssd, "\nAfter :: SSD");
    // cout << endl;

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
