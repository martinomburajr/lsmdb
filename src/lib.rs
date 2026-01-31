
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path};
use crate::entry::{Entry};
use crate::memtable::{MemTable};
use crate::types::{Encode, Decode, DBError};
use crate::sstable::{SSTableMeta, SSTableReader};

mod memtable;
mod entry;
mod types;
mod sstable;
mod wal;
mod manifest;

const DEFAULT_SS_TABLE_DIR: &'static str = ".lsm/sstables";
const DEFAULT_WAL_DIR: &'static str = ".lsm/wal";
const DEFAULT_SS_L0_COMPACT_THRESHOLD: u32 = 100;

/// The DBOpts used to configure the LSMDB
/// TODO: We can use the builder pattern here
pub struct DBOpts {
    // The max size in terms of capacity ie number of objects the MemTable will hold prior to SSTable write
    // `u32` is tentative
    pub memtable_max_size: Option<u32>,
    pub ss_table_path: Option<String>,
    pub wal_path: Option<String>,
    pub ss_l0_compact_threshold: u32,
}

impl Default for DBOpts {
    fn default() -> Self {
        let ss_table_path = Path::new(DEFAULT_SS_TABLE_DIR);
        let wal_path = Path::new(DEFAULT_WAL_DIR);
        let ss_num_files_before_compaction = DEFAULT_SS_L0_COMPACT_THRESHOLD;

        Self {
            memtable_max_size: Some(100),
            ss_table_path: Some(DEFAULT_SS_TABLE_DIR.to_string()),
            wal_path: Some(DEFAULT_WAL_DIR.to_string()),
            ss_l0_compact_threshold: ss_num_files_before_compaction,
        }
    }
}

/// DB represents the actual LSM-Tree. In it we have the following core components
/// 1. `mt`: The MemTable representing an in-memory cache for the inserted data
/// 2. `opts`: The options subpplied to the DBOpts
/// 3. `manifest`: The main configuration file holding the state of the LSM-Tree.
/// 4. ``
pub struct DB {
    mt: MemTable,
    ss_meta: Vec<SSTableMeta>,
    ss_reader: Option<SSTableReader>,
    // manifest: Option<Manifest>,
    // wal: Option<WAL>,
    opts: DBOpts,
    next_seq_no: u64,
}

impl DB {
    pub fn new(opts: Option<DBOpts>) -> Self {
        let opt = if let Some(opt) = opts {
            opt
        } else {
            DBOpts::default()
        };

        Self {
            mt: BTreeMap::new(),
            ss_meta: vec![],
            ss_reader: None,
            // manifest: None,
            // wal: None,
            opts: opt,
            next_seq_no: 0,
        }
    }

    /// Put will attempt to add the new K, V pair. In the event a key match takes place, if the new value
    /// contains a `seq_no` older than the existing one, we ignore it. Otherwise we perform the insert/update
    ///
    /// Put will always make use of the `next_seq_no` in `Self` first, and only increment after,
    /// so callers need to ensure that any operation that prepares the
    /// LSM-Tree for receiving new data, take this into account
    pub fn put<K: Encode, V: Encode>(&mut self, key: &K, val: &V) -> Result<(), DBError> {
        let encoded_key = key.encode();
        let encoded_val = val.encode();

        memtable::put(&mut self.mt, encoded_key, encoded_val, self.next_seq_no)?;

        self.next_seq_no += 1;

        Ok(())
    }

    // delete removes the object from the MemTable by setting its Entry to a `Entry::Tombstone` with the most up to data
    // `next_seq_no`.
    //
    // You may ask why not remove it entirely? As more objects are inserted, the MemTable will get to a point where it will
    // have to flush data to a more durable layer an SSTable. In a scenario where a <key,value> pair had been flushed to the
    // SSTable, if we then get a delete, we would have to find that entry in the SSTable and mark it as deleted (or just delete it)
    // this causes two main problems
    // 1. There could be multiple entries across multiple SSTables that still contain the value, we would either need to delete all of them
    // which is extremely costly.
    // 2. Compaction that works solely on SSTables will not know a key is delete and therefore still maintain it.
    //
    // To completely delete a key, we set a Tombstone, to let compaction know it should not be compacted again.
    pub fn delete<K: Encode>(&mut self, key: &K) -> Result<(), DBError> {
        let encoded_key = key.encode();

        if encoded_key.len() == 0 {
            return Err(
                DBError::Codec {
                    context: String::from("key cannot be empty"),
                    source: None
                }
            )
        }

        self.mt.entry(encoded_key)
            .or_insert(Entry::Tombstone { seq_no: self.next_seq_no });

        self.next_seq_no += 1;

        Ok(())
    }

    pub fn get_typed<K: Encode, V: Decode>(&self, key: &K) -> Result<Option<V>, DBError> {
        todo!()
    }

    pub fn get_raw<K: Encode, V: Decode>(&self, key: &K) -> Result<Option<Vec<u8>>, DBError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestEncoder = String;
    impl Encode for TestEncoder {
        fn encode(&self) -> Vec<u8> {
            self.as_bytes().to_owned()
        }
    }

    impl Decode for TestEncoder {
        fn decode(bytes: &[u8]) -> Result<Self, DBError> {
            match String::from_utf8(bytes.to_owned()) {
                Ok(data) => Ok(data),
                Err(e) => Err(DBError::Codec {
                    context: String::from("failed to convert bytes"),
                    source: Some(Box::new(e)),
                }),
            }
        }
    }

    #[test]
    fn insert_and_get() {
        let mut db = DB::new(None);

        let key: TestEncoder = "key-1".to_string();
        let val: TestEncoder = "some-val".to_string();

        assert_eq!(db.next_seq_no, 0);

        db.put(&key, &val);

        assert_eq!(db.mt.len(), 1);
        assert_eq!(db.next_seq_no, 1);

        let kbytes = key.encode();
        let vbytes = val.encode();

        assert_eq!(
            db.mt.get(kbytes.as_slice()),
            Some(&Entry::Value {
                seq_no: 0,
                val: vbytes
            })
        );

        // Add Next Key
        {
            let key_2: TestEncoder = "key-2".to_string();
            let val_2: TestEncoder = "some-val_2".to_string();

            assert_eq!(db.next_seq_no, 1);

            db.put(&key_2, &val_2);

            assert_eq!(db.mt.len(), 2);
            assert_eq!(db.next_seq_no, 2);

            let kbytes = key_2.encode();
            let vbytes = val_2.encode();

            assert_eq!(
                db.mt.get(kbytes.as_slice()),
                Some(&Entry::Value {
                    seq_no: 1,
                    val: vbytes
                })
            )
        }
    }

    #[test]
    fn insert_empty_key() {
        let mut db = DB::new(None);

        let key: TestEncoder = "".to_string();
        let val: TestEncoder = "s1".to_string();

        let res = db.put(&key, &val);

        matches!(res.err(), Some(DBError::Codec { context: _, source: _  }));
    }

    #[test]
    fn insert_duplicate_and_get() {
        let mut db = DB::new(None);

        let key: TestEncoder = "k1".to_string();
        let val: TestEncoder = "s1".to_string();

        db.put(&key, &val);

        let kbytes = key.encode();
        let vbytes = val.encode();

        assert_eq!(
            db.mt.get(kbytes.as_slice()),
            Some(&Entry::Value {
                seq_no: 0,
                val: vbytes
            })
        );

        //WHEN: Add duplicate key
        {
            let dup_key: TestEncoder = "k1".to_string();
            let val_2: TestEncoder = "s2".to_string();

            db.put(&dup_key, &val_2);

            assert_eq!(db.next_seq_no, 2);
            assert_eq!(db.mt.len(), 1);

            let dup_key_bytes = dup_key.encode();
            let val_bytes = val_2.encode();

            assert_eq!(
                db.mt.get(&dup_key_bytes),
                Some(&Entry::Value {
                    seq_no: 1,
                    val: val_bytes.clone()
                })
            );
            assert_eq!(
                db.mt.get(&kbytes),
                Some(&Entry::Value {
                    seq_no: 1,
                    val: val_bytes
                })
            )
        }
    }

    #[test]
    fn delete_empty() {
        // This should error - prevent any change that db.seq_no increases
        assert!(false)
    }

    #[test]
    fn delete_empty_key() {
        // 1. This should error - empty key violation
        assert!(false)

    }

    #[test]
    fn delete_on_key_that_doesnt_exist() {
        // 1. This should error - no precedent to return anything - if some this means i always need to return something on delete, this isn't stable and
        // hard to reason about with both memtable & sstable
        assert!(false)
    }

    #[test]
    fn delete_ok() {
        assert!(false)
    }

    #[test]
    fn insert_delete_insert_ok() {
        assert!(false)

    }

}
