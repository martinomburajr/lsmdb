#[derive(Debug, PartialEq, Eq)]
pub enum Entry {
    Value { seq_no: u64, val: Vec<u8> },
    Tombstone { seq_no: u64 },
}

impl Entry {
    pub fn seq_no(&self) -> u64 {
        match self {
            Entry::Value { seq_no, .. } => *seq_no,
            Entry::Tombstone { seq_no } => *seq_no,
        }
    }
}