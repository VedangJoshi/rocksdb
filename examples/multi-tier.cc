#include <cstdio>
#include <string>
#include <iostream>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"

#define TIER_SSD 0
#define TIER_HDD 1

using namespace rocksdb;
using namespace std;

// Below are fake paths. Actual paths could be something like:
//                  /dev/SSD1, /dev/HDD4 etc.
std::string SSD_DB_path = "/tmp/rocksdb_virtual_SSD";
std::string HDD_DB_path = "/tmp/rocksdb_virtual_HDD";

/**
 *  Move()
 */
std::vector<Status> move(DB* src_db, DB* dest_db, ColumnFamilyHandle* dest_db_cfh,
                         const ReadOptions& read_options, const WriteOptions& write_options,
                         const vector<Slice>& keys)
{
    vector<string> values;
    vector<Status> status_vec;
    WriteBatch batch;

    // Get 'values' for all 'keys'
    status_vec = src_db->MultiGet(read_options, keys, &values);

    for (auto k = 0; k < values.size(); k++) {
        batch.Put(dest_db_cfh, keys.at(k), Slice(values.at(k)));
    }

    Status s = dest_db->Write(write_options, &batch);
    assert(s.ok());

    return status_vec;
}

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

    rocksdb::Iterator* it;
    it = db_hdd->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout << it->key().ToString() << ": " << it->value().ToString() << endl;
    }
    cout << endl;

    assert(it->status().ok()); // Check for any errors found during the scan

    // MOVE
    vector<Status> stats = move(db_ssd, db_hdd, handles_hdd[0], ReadOptions(), WriteOptions(), keys);

    for(auto s : stats) {
        assert(s.ok());
    }

    it = db_hdd->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout << it->key().ToString() << ": " << it->value().ToString() << endl;
    }
    assert(it->status().ok()); // Check for any errors found during the scan

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
