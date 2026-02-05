use crate::entry::Entry;
use crate::memtable::MemTable;
use crate::sstable::{SSTableMeta, SSTableReader};
use crate::types::{DBError, Decode, Encode};
use crate::wal::{Op, SyncPolicy, WAL, WALRecord};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

mod entry;
mod manifest;
mod memtable;
mod sstable;
mod types;
mod wal;

const DEFAULT_SS_TABLE_DIR: &'static str = ".lsm/sstables";
const DEFAULT_WAL_DIR: &'static str = ".lsm/wal";
const DEFAULT_SS_L0_COMPACT_THRESHOLD: u32 = 100;
const DEFAULT_MAX_RECORD_LEN: u32 = 1024 * 1000; // 1MiB

/// The DBOpts used to configure the LSMDB
/// TODO: We can use the builder pattern here
pub struct DBConfig {
    // The max size in terms of capacity ie number of objects the MemTable will hold prior to SSTable write
    // `u32` is tentative
    pub memtable_max_size: Option<u32>,
    pub ss_table_dir: PathBuf,
    pub wal_file: PathBuf,
    pub wal_sync_policy: SyncPolicy,
    pub max_record_len: u32,
    pub ss_l0_compact_threshold: u32,
    disable_wal_memtable_replay_on_load: bool,
}

impl Default for DBConfig {
    fn default() -> Self {
        let mut ss_table_path = PathBuf::new();
        ss_table_path.push(DEFAULT_SS_TABLE_DIR);

        let mut wal_path = PathBuf::new();
        wal_path.push(DEFAULT_WAL_DIR);

        Self {
            memtable_max_size: Some(100),
            ss_table_dir: ss_table_path,
            wal_file: wal_path,
            ss_l0_compact_threshold: DEFAULT_SS_L0_COMPACT_THRESHOLD,
            wal_sync_policy: SyncPolicy::Always,
            max_record_len: DEFAULT_MAX_RECORD_LEN,
            disable_wal_memtable_replay_on_load: false,
        }
    }
}

/// DB represents the actual LSM-Tree. In it we have the following core components
/// 1. `mt`: The MemTable representing an in-memory cache for the inserted data
/// 2. `opts`: The options subpplied to the DBOpts
/// 3. `manifest`: The main configuration file holding the state of the LSM-Tree.
/// 4. ``
pub struct DB {
    mem_table: MemTable,
    ss_meta: Vec<SSTableMeta>,
    ss_reader: Option<SSTableReader>,
    // manifest: Option<Manifest>,
    wal: wal::WAL,
    opts: DBConfig,
    next_seq_no: u64,
}

impl DB {
    pub fn new(opts: Option<DBConfig>) -> Result<Self, DBError> {
        let opt = if let Some(opt) = opts {
            opt
        } else {
            DBConfig::default()
        };

        let mut mem_table = BTreeMap::new();

        let wal_file = File::open(opt.wal_file.clone()).map_err(|e| DBError::Io {
            op: "failed to open wal_file",
            path: opt.wal_file.clone(),
            source: e,
        })?;

        let mut wal = WAL::new(
            opt.wal_file.clone(),
            opt.wal_sync_policy,
            opt.max_record_len,
        )?;

        if !opt.disable_wal_memtable_replay_on_load {
            wal.replay_into(wal_file, &mut mem_table)?;
        } else {
            // log this
        }

        Ok(Self {
            mem_table,
            ss_meta: vec![],
            ss_reader: None,
            // manifest: None,
            wal: wal,
            opts: opt,
            next_seq_no: 0,
        })
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

        // Insert into WAL
        // TODO: see if we can prevent multiple clones
        let wal_record = WALRecord::new(
            Op::Put,
            self.next_seq_no,
            encoded_key.clone(),
            encoded_val.clone(),
        );
        self.wal.append(&wal_record)?;

        // Insert into MemTable
        memtable::put(
            &mut self.mem_table,
            encoded_key,
            encoded_val,
            self.next_seq_no,
        )?;

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
            return Err(DBError::Codec {
                context: String::from("key cannot be empty"),
                source: None,
            });
        }

        self.mem_table
            .entry(encoded_key)
            .or_insert(Entry::Tombstone {
                seq_no: self.next_seq_no,
            });

        self.next_seq_no += 1;

        Ok(())
    }

    pub fn get_typed<K: Encode, V: Decode>(&self, key: &K) -> Result<Option<V>, DBError> {
        match self.get_raw(key)? {
            Some(data) => Ok(Some(V::decode(data.as_ref())?)),
            None => Ok(None)
        }
    }

    pub fn get_raw<K: Encode>(&self, key: &K) -> Result<Option<Vec<u8>>, DBError> {
        let encoded_key = key.encode();

        let val = match self.mem_table.get(&encoded_key) {
            Some(entry) => match entry {
                Entry::Value { seq_no, val } => Some(val.clone()),
                Entry::Tombstone { seq_no } => None,
            },
            None => None,
        };
        
        // TODO: Implement SSTables if cache miss
        
        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    const TEST_DATA_DIR: &'static str = "test_data";
    const SS_TABLE_DIR: &'static str = "sstb";
    const WAL_DIR: &'static str = "wal";

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

    fn test_default_config(wal_file_name: &str, preserve_wal: bool) -> DBConfig {
        let mut ss_table_path = PathBuf::new();
        ss_table_path.push(TEST_DATA_DIR);
        ss_table_path.push(SS_TABLE_DIR);

        let mut wal_path = PathBuf::new();
        wal_path.push(TEST_DATA_DIR);
        wal_path.push(WAL_DIR);
        wal_path.push(format!("{}_wal.wl", wal_file_name));

        // println!("wal_path: {:?}", wal_path);
        
        let mut wal_file_opts = OpenOptions::new();
        wal_file_opts
            .create(true)
            .write(true)
            .read(true);
        
        if !preserve_wal {
            wal_file_opts
                .truncate(true);
        }

        wal_file_opts.open(wal_path.clone())
        .unwrap();
        
        DBConfig {
            memtable_max_size: Some(1000),
            ss_table_dir: ss_table_path,
            wal_file: wal_path,
            wal_sync_policy: SyncPolicy::Always,
            ss_l0_compact_threshold: 1000,
            max_record_len: DEFAULT_MAX_RECORD_LEN,
            disable_wal_memtable_replay_on_load: false,
        }
    }

    #[test]
    fn insert_and_get() {
        let mut db = DB::new(Some(test_default_config("insert_and_get", false))).unwrap();

        let key: TestEncoder = "key-1".to_string();
        let val: TestEncoder = "some-val".to_string();

        assert_eq!(db.next_seq_no, 0);

        db.put(&key, &val);

        assert_eq!(db.mem_table.len(), 1);
        assert_eq!(db.next_seq_no, 1);

        let kbytes = key.encode();
        let vbytes = val.encode();

        assert_eq!(
            db.mem_table.get(kbytes.as_slice()),
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

            assert_eq!(db.mem_table.len(), 2);
            assert_eq!(db.next_seq_no, 2);

            let kbytes = key_2.encode();
            let vbytes = val_2.encode();

            assert_eq!(
                db.mem_table.get(kbytes.as_slice()),
                Some(&Entry::Value {
                    seq_no: 1,
                    val: vbytes
                })
            )
        }
    }

    #[test]
    fn insert_empty_key() {
        let mut db = DB::new(Some(test_default_config("insert_empty_key", false))).unwrap();

        let key: TestEncoder = "".to_string();
        let val: TestEncoder = "s1".to_string();

        let res = db.put(&key, &val);

        matches!(
            res.err(),
            Some(DBError::Codec {
                context: _,
                source: _
            })
        );
    }

    #[test]
    fn insert_duplicate_and_get() {
        let mut db = DB::new(Some(test_default_config("insert_duplicate_and_get", false))).unwrap();

        let key: TestEncoder = "k1".to_string();
        let val: TestEncoder = "s1".to_string();

        db.put(&key, &val);

        let kbytes = key.encode();
        let vbytes = val.encode();

        assert_eq!(
            db.mem_table.get(kbytes.as_slice()),
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
            assert_eq!(db.mem_table.len(), 1);

            let dup_key_bytes = dup_key.encode();
            let val_bytes = val_2.encode();

            assert_eq!(
                db.mem_table.get(&dup_key_bytes),
                Some(&Entry::Value {
                    seq_no: 1,
                    val: val_bytes.clone()
                })
            );
            assert_eq!(
                db.mem_table.get(&kbytes),
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

    #[test]
    fn simulate_replay() {
        let mut db = DB::new(Some(test_default_config("simulate_replay", true))).unwrap();

        let key: TestEncoder = String::from("k1");
        let val: TestEncoder = String::from("v1");
        let key2: TestEncoder = String::from("k2");
        let val2: TestEncoder = String::from("v2");
        db.put(&key, &val).unwrap();
        db.put(&key2, &val2).unwrap();

        // Drop the db
        drop(db);

        let mut new_db = DB::new(Some(test_default_config("simulate_replay",true))).unwrap();

        assert!(
            matches!(
                new_db.get_typed::<TestEncoder, TestEncoder>(&key).unwrap(), 
                Some(val)
            )
        );
        
        assert!(
            matches!(
                new_db.get_typed::<TestEncoder, TestEncoder>(&key2).unwrap(), 
                Some(val2)
            )
        )
        
    }
}
