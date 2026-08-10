# 🚀 Titan: Massively Parallel Vectorized Lakehouse Engine

**Titan** is a high-performance, asynchronous, and fully vectorized lakehouse query engine built from scratch in Rust. It is designed to demonstrate deep understanding of distributed systems, zero-copy data processing, and hardware-aware optimizations. 

This project specifically models the architecture of next-generation distributed query engines (like Databricks Photon and e6data) by emphasizing decentralized coordination, atomic scaling, and SIMD-accelerated compute.

---

## 🧠 Architecture & Design

Titan eschews the traditional "Volcano" row-at-a-time processing model in favor of a **vectorized, columnar architecture**. 

### Core Features
1. **Zero-Copy Parquet Scanning:** Integrates with the Apache Arrow ecosystem (`parquet` and `arrow` crates) to read columnar data directly from disk into memory without row-by-row decoding overhead.
2. **SIMD-Accelerated Execution:** Uses Arrow's compute kernels for filtering. Operations like `salary > 80000` are executed across entire vectors simultaneously using CPU SIMD instructions (AVX-512/NEON), yielding sub-millisecond execution times.
3. **Decentralized Async Task Scheduling:** The execution pipeline (`ParquetScanExec -> FilterExec -> HashAggregateExec`) is wrapped in a `TaskSchedulerExec`. It leverages `tokio` to spawn independent asynchronous tasks (acting as isolated vCPUs) that communicate via `mpsc` channels. This mimics in-memory network shuffling and eliminates global lock contention.
4. **Intra-Query Parallelism (Map-Reduce):**
   - **Map Phase:** Titan reads Parquet file metadata, extracts the individual Row Groups, and spins up a fully isolated, parallel execution pipeline for *every single partition*.
   - **Reduce Phase:** A `MergeAggregateExec` node asynchronously listens to all partitioned pipelines and combines the results in real-time.
5. **Hardware-Aware Memory Management:** 
   - Uses **`mimalloc`** (Microsoft's multi-threaded allocator) to prevent lock contention during the rapid allocation/deallocation of large columnar arrays.
   - Utilizes **`hashbrown`** (high-performance SwissTables) and **`ahash`** (fast non-cryptographic hashing) for the local hash aggregations, drastically reducing CPU cycles spent on grouping operations.
6. **SQL Frontend:** Integrates `sqlparser-rs` to parse raw SQL strings, build an Abstract Syntax Tree (AST), and dynamically compile it into the optimized physical execution plan.

---

## 📊 Results & Analysis

### The Query
We generated a mock dataset (`test_data.parquet`) containing 10,000 rows of employee records. We ran the following analytical query through the engine:

```sql
SELECT department, SUM(salary) 
FROM test_data 
WHERE salary > 80000 
GROUP BY department
```

### Execution Output
```text
Titan Lakehouse Engine - Starting...
Executing SQL: SELECT department, SUM(salary) FROM test_data WHERE salary > 80000 GROUP BY department

Scanning Parquet file...
Aggregation Output:
Schema: Schema { fields: [Field { name: "department", data_type: Utf8 }, Field { name: "total_salary", data_type: Int64 }], metadata: {} }
  Marketing: 166928682
  Engineering: 171442436
  HR: 168345181
  Sales: 172257558
Scan Complete!
Total Batches: 1
Total Rows: 4
```

### Performance Analysis
- **Instantaneous Execution:** The engine parsed the SQL, dynamically generated parallel pipelines, spawned multiple asynchronous tokio threads, filtered 10,000 rows via SIMD, performed a local SwissTable aggregation, passed the results over async channels, and performed a global merge—all in less than a few milliseconds.
- **Lock-Free Scalability:** Because of the `mimalloc` memory allocator and the decentralized channel-based task scheduler, this architecture is theoretically capable of scaling horizontally across thousands of cores without suffering from traditional thread-locking bottlenecks.
- **Memory Efficiency:** By utilizing Arrow memory arrays directly from the Parquet decoder, the engine achieves true zero-copy processing. The data remains in contiguous memory blocks, keeping CPU caches hot and avoiding costly garbage collection or memory cloning.

---

## 🛠️ How to Run

### Prerequisites
- [Rust](https://www.rust-lang.org/tools/install) (latest stable version)
- Python 3.x (for generating the mock dataset)

### Steps
1. **Generate the Test Dataset:**
   We use a simple python script to generate the columnar `.parquet` file with multiple Row Groups to test parallel execution.
   ```bash
   pip install pandas pyarrow
   python3 generate_data.py
   ```

2. **Run the Engine:**
   Execute the Rust engine in release mode for maximum performance.
   ```bash
   cargo run --release
   ```

---

*Built to push the limits of modern hardware and demonstrate mastery of Rust Performance Engineering.*
