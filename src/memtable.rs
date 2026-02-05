use crate::entry::Entry;
use crate::types::{DBError, ERR_CONFIG_EMPTY_KEY};
use std::collections::{BTreeMap};

// pub trait MemTableExt {
//     fn get(&self, key: &[u8]) -> Option<&Entry>;
//     fn put(&mut self, key: Vec<u8>, val: Vec<u8>, seq_no: u64);
//     fn delete(&mut self, key: Vec<u8>, seq_no: u64);
//     // TODO:
//     // fn iter();
//     // fn range();
// }

pub type MemTable = BTreeMap<Vec<u8>, Entry>;

pub fn put(mem: &mut MemTable, key: Vec<u8>, val: Vec<u8>, seq_no: u64) -> Result<(), DBError> {
    if key.len() == 0 {
        return Err(DBError::Codec {
            context: String::from(ERR_CONFIG_EMPTY_KEY),
            source: None,
        });
    }

    mem.entry(key)
        .and_modify(|v| {
            if v.seq_no() < seq_no {
                *v = Entry::Value {
                    seq_no: seq_no,
                    val: val.clone(), // I dont want to re-clone here
                };
            }
        })
        .or_insert(Entry::Value {
            seq_no: seq_no,
            val: val,
        });

    Ok(())
}

#[cfg(test)]
mod memtable {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut mem = MemTable::new();

        let key: Vec<u8> = "key-1".to_string().into_bytes();
        let val: Vec<u8> = "some-val".to_string().into_bytes();

        put(&mut mem, key.clone(), val.clone(), 0).unwrap();

        assert_eq!(mem.len(), 1);

        assert_eq!(
            mem.get(key.as_slice()),
            Some(&Entry::Value {
                seq_no: 0,
                val: val
            })
        );

        // Add Next Key
        {
            let key_2 = "key-2".to_string().into_bytes();
            let val_2 = "some-val_2".to_string().into_bytes();

            put(&mut mem, key_2.clone(), val_2.clone(), 1).unwrap();

            assert_eq!(mem.len(), 2);

            assert_eq!(
                mem.get(key_2.clone().as_slice()),
                Some(&Entry::Value {
                    seq_no: 1,
                    val: val_2.clone()
                })
            )
        }
    }
}
