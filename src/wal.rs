use std::fs::File;
use std::io::{BufWriter}

use crate::types::DBError;

enum SyncPolicy {
    Always,
    Never
}


/// The WAL (Write-Ahead-Log) acts as a persistent store for incoming changes for the MemTable. It acts as a durability layer that accepts append-only writes
/// that go to file, and only then are then added to the MemTable
struct WAL {
    wal_file: BufWriter<File>,
    sync: Option<SyncPolicy>
}

impl WAL {
    pub fn append(&self, rec: &WALRecord) -> Result<(), DBError> {

    }
}


#[derive(Debug, Clone)]
struct WALRecord {
    // len: u32,
    op: u8,
    seq_no: u64,
    // key_size: u32,
    // val_size: u32,
    key: Vec<u8>,
    val: Vec<u8>,
    // crc32: u32
}

#[derive(Debug)]
pub enum WalDecodeError {
    CleanEOF,
    Corruption(&'static str),
}

/// Attempts to encode a byte vec from the WAL record with some extra information e.g. key and val lengths.
/// Below is a map of the encoding:
///
/// [op u8][seq u64][key_len u32][val_len u32][key bytes][val bytes]
pub fn encode_record(rec: &WALRecord) -> Vec<u8> {
    let key_len_u32: u32 = rec.key.len().try_into().expect("key is too large");
    let val_len_u32: u32 = rec.val.len().try_into().expect("val too large");

    let mut body = Vec::with_capacity(
        1+8+4+4+rec.key.len() + rec.val.len()
    );

    body.push(rec.op);
    body.extend_from_slice(&rec.seq_no.to_le_bytes());
    body.extend_from_slice(&key_len_u32.to_le_bytes());
    body.extend_from_slice(&val_len_u32.to_le_bytes());
    body.extend_from_slice(&rec.key);
    body.extend_from_slice(&rec.val);

    let crc: u32 = crc32fast::hash(&body);

    // len = [body]+[crc]
    let len: u32 = (body.len() + 4).try_into().expect("record too larget");

    // Ensure [len][body][crc]
    let mut out = Vec::with_capacity(4 + body.len() + 4);
    out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&body);
            out.extend_from_slice(&crc.to_le_bytes());

            out

}

impl WALRecord {
    pub fn encode(&self) -> Vec<u8> {

    }

    pub fn decode(&mut self, obj: &[u8]) -> Result<(), DBError> {

    }
}
