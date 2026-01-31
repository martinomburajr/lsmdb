/// The Manifest maintains a record of all the SSTables and provides necessary configuration data to bring back the LSM Tree
struct Manifest {
    wal_path: String,
    ss_table_path: String,
}