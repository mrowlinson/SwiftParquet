# SwiftParquet

Pure Swift implementation of Apache Parquet file format. Port of Go implementation
from github.com/apache/arrow-go/parquet.

## Build & Test
swift build
swift test

## Architecture
- Sources/SwiftParquet/ — single target, no external dependencies
- Thrift/ — hand-rolled TCompactProtocol (not generated code)
- Format/ — Swift structs matching parquet.thrift definitions
- Encoding/ — column value encoding (Plain, RLE, Dictionary, Delta*)
- Compress/ — compression codecs (Snappy, Gzip, Zstd)
- File/ — file/row-group/column/page readers and writers
- Schema/ — Parquet schema tree and column descriptors

## Key Design Decisions
- Zero dependencies. Pure Swift.
- Generics with ParquetValue protocol instead of Go's code generation
- Data/[UInt8] with ARC instead of Go's manual memory.Buffer refcounting
- Swift concurrency for parallel column reading (Phase 2+)
- Thrift compact protocol implemented by hand (~500 lines), not generated

## Reference Source
Primary: github.com/apache/arrow-go/parquet (Apache 2.0)
Spec: github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
Test files: github.com/apache/parquet-testing

## Validation
All output must be readable by:
- pyarrow: python3 -c "import pyarrow.parquet as pq; print(pq.read_table('test.parquet'))"
- DuckDB: duckdb -c "SELECT * FROM 'test.parquet'"
- parquet-cli: parquet-tools cat test.parquet

## Phase 1 Scope (MVP)
Write flat-column Parquet files with Plain encoding, no compression.
Read not required yet. Dictionary encoding not required yet.
Must pass pyarrow and DuckDB validation.

## Phase 2 Scope
Add file reader, column reader, Snappy + Gzip compression, dictionary encoding,
column statistics. Must pass roundtrip tests (write→read) and cross-tool interop
(write with SwiftParquet → read with pyarrow, and vice versa).

## ⛔ STOP AFTER PHASE 2
Do NOT proceed to Phase 3. Report status and wait for user confirmation.
Phase 3 (nested types, encryption, bloom filters) will use a different model.
