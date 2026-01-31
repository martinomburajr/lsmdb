use std::fs::File;

pub struct SSTableMeta {
    file_no: u64,
    level: u32,
    path: String,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
}

pub struct SSTableReader {
    file: File,
}