#include <cstdio>
#include <string>
#include <iostream>
#include <vector>

#include "rocksdb/db.h"
#include "db/db_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "util/mutexlock.h"

#define TIER_SSD 0
#define TIER_HDD 1

using namespace rocksdb;
using namespace std;

// Below are fake paths. Actual paths could be something like:
//                  /dev/SSD1, /dev/HDD4 etc.
std::string SSD_DB_path = "/tmp/SSD_on_rocks";
std::string HDD_DB_path = "/tmp/HDD_on_rocks";

int main() {
    // Options
    Options options;
    options.create_if_missing = true;
    Status s;
    vector<Status> status_vec;
    vector<ColumnFamilyHandle*> cfh;
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

    vector<Slice> keys;
    vector<string> values;
    keys.push_back("key2");
    keys.push_back("key3");
    keys.push_back("key4");

    cout << "Before :: HDD" << endl;
    rocksdb::Iterator* it;
    it = db_hdd->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout << it->key().ToString() << ": " << it->value().ToString() << endl;
    }

    cout << "Before :: SSD" << endl;
    it = db_ssd->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout << it->key().ToString() << ": " << it->value().ToString() << endl;
    }

    cout << endl << "*****" << endl;

    assert(it->status().ok()); // Check for any errors found during the scan

    // MOVE
    s = db_ssd->move(db_ssd, db_hdd, handles_hdd[0], ReadOptions(), WriteOptions(), keys);
    assert(s.ok());

    cout << "After :: HDD" << endl;
    it = db_hdd->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout << it->key().ToString() << ": " << it->value().ToString() << endl;
    }
    cout << endl;

    cout << "After :: SSD" << endl;
    it = db_ssd->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout << it->key().ToString() << ": " << it->value().ToString() << endl;
    }
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
