# lsmdb

This is a log-structured merge (tree) database that I'm building as a means to understand performance in write-heavy key-value store databases. LSM Tree's are common data structures that help maximize write throughput and data durability by establishing an in-memory write buffer [`MemTable`] that holds onto writes Key-Value style writes that eventually get drained in an ordered-fashion
into an underlying set of files called sorted-string tables (`[SSTables]`).

LSM Trees are used in production on systems like [RocksDB](https://github.com/facebook/rocksdb) made by Facebook who based their initial design on the [LevelDB](https://github.com/google/leveldb) produced at Google.

### Key Concepts & Components
1. `MemTable`
2. `WAL`
3. `SSTable`


#### The MemTable
The `MemTable` is also backed by a write-ahead-log known as a  `WAL` that at best effort stores the incoming writes in an append-only fashion into a `WAL` file. In the event of the `MemTable` failing (power outage, OOM), the `WAL` can be used to replay the contents of the `MemTable`.

#### The Write-Ahead Log (WAL)


#### The SSTables SSTables]

### Future Pipeline
As the project advances, it would be great to incorporate the following. These are just ideas now, and a more formal roadmap is expected if/when the project gains more traction.

- [ ] Concurrency
- [ ] 
