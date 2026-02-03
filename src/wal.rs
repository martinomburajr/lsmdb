use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::types::DBError;

#[derive(Debug, Clone, Copy)]
pub enum SyncPolicy {
    Always,
    Never,
}

/// The WAL (Write-Ahead-Log) acts as a persistent store for incoming changes for the MemTable. It acts as a durability layer that accepts append-only writes
/// that go to file, and only then are then added to the MemTable
pub struct WAL {
    buf: BufWriter<File>,
    pathBuf: PathBuf,
    sync: SyncPolicy,
}

impl WAL {
    pub fn new(file_path: PathBuf, sync: SyncPolicy) -> Result<Self, DBError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(file_path.clone())
            .map_err(|e| DBError::Io {
                op: "wal: failed to open file from path_buf",
                path: file_path.clone(),
                source: e,
            })?;

        Ok(Self {
            buf: BufWriter::new(file),
            pathBuf: file_path,
            sync: sync,
        })
    }

    pub fn append(&mut self, rec: &WALRecord) -> Result<(), DBError> {
        let encode = encode_record(rec);

        self.buf
            .write_all(encode.as_ref())
            .map_err(|e| DBError::Io {
                op: "wal: failed to write wal buf",
                path: self.pathBuf.clone(),
                source: e,
            })?;

        match self.sync {
            SyncPolicy::Always => {
                // `flush()` only moves the writes from the buffer to the kernel
                // i.e user_space -> kernel_space.
                self.buf.flush().map_err(|e| DBError::Io {
                    op: "wal: failed to flush wal buf",
                    path: self.pathBuf.clone(),
                    source: e,
                })?;

                // If something happens to the kernel e.g. power outage, the writes might not have been
                // synced to the file system, so we need to call `sync_all` for that.
                self.buf.get_ref().sync_all().map_err(|e| DBError::Io {
                    op: "wal: failed to sync_all",
                    path: self.pathBuf.clone(),
                    source: e,
                })?;
            }
            SyncPolicy::Never => {}
        };

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WALRecord {
    op: Op,
    seq_no: u64,
    key: Vec<u8>,
    val: Vec<u8>,
}

impl WALRecord {
    pub fn new(op: Op, seq_no: u64, key: Vec<u8>, val: Vec<u8>) -> Self {
        Self {
            op,
            seq_no,
            key,
            val,
        }
    }
}

#[derive(Debug)]
pub enum WalDecodeError {
    CleanEOF,
    Corruption(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    Put = 1,
    Delete = 2,
}

impl TryFrom<u8> for Op {
    type Error = WalDecodeError;
    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            0x1 => Ok(Op::Put),
            0x2 => Ok(Self::Delete),
            _ => Err(WalDecodeError::Corruption("invalid op code found")),
        }
    }
}

/// Attempts to encode to a `Vec<u8>` from the WAL record with some extra information e.g. key and val lengths.
/// Below is a map of the encoding:
///
/// [op u8][seq u64][key_len u32][val_len u32][key bytes][val bytes]
///
/// The above structure is maintained regardless of whether the `op` i.e operation is a `DEL` or
pub fn encode_record(rec: &WALRecord) -> Vec<u8> {
    let key_len_u32: u32 = rec.key.len().try_into().expect("key is too large");
    let val_len_u32: u32 = rec.val.len().try_into().expect("val too large");

    let mut body = Vec::with_capacity(1 + 8 + 4 + 4 + rec.key.len() + rec.val.len());

    body.push(rec.op.clone() as u8);
    body.extend_from_slice(&rec.seq_no.to_le_bytes());
    body.extend_from_slice(&key_len_u32.to_le_bytes());
    body.extend_from_slice(&val_len_u32.to_le_bytes());
    body.extend_from_slice(&rec.key);
    body.extend_from_slice(&rec.val);

    let crc: u32 = crc32fast::hash(&body);

    // len = [body]+[crc]
    let len: u32 = (body.len() + 4).try_into().expect("record too large");

    // Ensure [len][body][crc]
    let mut out = Vec::with_capacity(4 + body.len() + 4);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(&crc.to_le_bytes());

    out
}

///
pub fn decode_record(
    buf: &[u8],
    offset: usize,
    max_record_len: u32,
) -> Result<(WALRecord, usize), WalDecodeError> {
    //  start by obtaining the len
    if buf.len().saturating_sub(offset) < 4 {
        return Err(WalDecodeError::CleanEOF);
    }

    // [len u32][op u8][seq u64][key_len u32][val_len u32][key bytes][val bytes]
    let len = read_u32_le(&buf[offset..]).ok_or(WalDecodeError::CleanEOF)?;
    if len == 0 || len > max_record_len {
        return Err(WalDecodeError::Corruption("invalid len"));
    }

    let total = (4 as usize) + len as usize;
    if buf.len().saturating_sub(offset) < total {
        // the tail has likely been truncated
        return Err(WalDecodeError::CleanEOF);
    }

    // We already extracted len
    // {len u32}[op u8][seq u64][key_len u32][val_len u32][key bytes][val bytes]
    let start = offset + 4; //
    let end = offset + total;
    let rest_of_buf = &buf[start..end];

    // If the start->end is less than at least the `rest_of_body+crc`, our record is too short;
    if rest_of_buf.len() < 4 {
        return Err(WalDecodeError::Corruption("record too short"));
    }

    let body_len = rest_of_buf.len() - 4;
    let body = &rest_of_buf[..body_len];
    let crc_expecteed =
        read_u32_le(&rest_of_buf[body_len..]).ok_or(WalDecodeError::Corruption("missing crc"))?;
    let crc_actual = crc32fast::hash(body);
    if crc_actual != crc_expecteed {
        return Err(WalDecodeError::Corruption("crc mismatch"));
    }

    //  compute the crchash first to see bits are still valid
    //
    if body.len() < 1 + 8 + 4 + 4 {
        return Err(WalDecodeError::Corruption("body too short"));
    }

    let op = body[0];
    let seq_no = read_u64_le(&body[1..]).ok_or(WalDecodeError::Corruption("bad seq"))?;
    let key_len =
        read_u32_le(&body[1 + 8..]).ok_or(WalDecodeError::Corruption("bad seq"))? as usize;
    let val_len =
        read_u32_le(&body[1 + 8 + 4..]).ok_or(WalDecodeError::Corruption("bad seq"))? as usize;

    if key_len == 0 {
        return Err(WalDecodeError::Corruption("key_len is 0"));
    }

    // Now we grab the [key:?][body:?]
    let payload_offset = 1 + 8 + 4 + 4;
    let expected_body_size = payload_offset + key_len + val_len;

    if expected_body_size != body.len() {
        return Err(WalDecodeError::Corruption(
            "length mismatch - body len doesnt match what is described in payload metadata",
        ));
    }

    // half-open i.e body[0..n)
    let key_start = payload_offset;
    let key_end = key_start + key_len;
    let val_start = key_end;
    let val_end = val_start + val_len;

    let key = body[key_start..key_end].to_vec();
    let val = body[val_start..val_end].to_vec();

    let rec = WALRecord {
        op: Op::try_from(op)?,
        seq_no,
        key,
        val,
    };

    Ok((rec, end))
}

fn read_u32_le(input: &[u8]) -> Option<u32> {
    let bitfield: [u8; 4] = input.get(0..4)?.try_into().ok()?;
    Some(u32::from_le_bytes(bitfield))
}

fn read_u64_le(input: &[u8]) -> Option<u64> {
    let bitfield: [u8; 8] = input.get(0..8)?.try_into().ok()?;
    Some(u64::from_le_bytes(bitfield))
}

#[cfg(test)]
mod test {
    use crate::wal::{Op, WALRecord, encode_record, decode_record};

    #[test]
    fn test_enc_dec() {
        let record = WALRecord {
            op: Op::Put,
            seq_no: 42,
            key: vec![0, 1],
            val: vec![0, 1, 2, 3, 4, 5],
        };

        let enc = encode_record(&record);
        let (dec, next) = decode_record(&enc, 0, 1024 * 1024).unwrap();

        assert_eq!(dec.op, record.op);
        assert_eq!(dec.seq_no, record.seq_no);
        assert_eq!(dec.key, record.key);
        assert_eq!(dec.val, record.val);
        assert_eq!(next, enc.len());
    }
}
