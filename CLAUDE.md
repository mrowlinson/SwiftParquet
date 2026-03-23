# SwiftParquet

Pure Swift implementation of Apache Parquet file format. Port of Go implementation
from github.com/apache/arrow-go/parquet.

## Build & Test
swift build
swift test

## Architecture
- Sources/SwiftParquet/ — single target, swift-crypto conditional dep (Linux only)
- Thrift/ — hand-rolled TCompactProtocol writer + reader
- Format/ — Swift structs matching parquet.thrift definitions (with ThriftReadable/ThriftWritable)
- Encoding/ — column value encoding/decoding (Plain, RLE, Dictionary, DeltaBinaryPacked, DeltaByteArray, BloomFilter)
- Compress/ — compression codecs (Snappy pure Swift, Gzip via Compression/CZlib, Zstd pure Swift, CRC32)
- File/ — file/row-group/column/page readers and writers, StreamingFileWriter
- Schema/ — Parquet schema tree, column descriptors, Dremel shredder/assembler
- Convenience/ — ParquetFileWriter, ParquetFileReader, ParquetStreamWriter, SchemaBuilder, ParquetTable
- Encrypt/ — AES-GCM column/footer encryption via CryptoKit/swift-crypto
- Sources/CZlib/ — system library module for zlib (Linux)

## Key Design Decisions
- Zero external dependencies on macOS. swift-crypto only on Linux (mirrors CryptoKit API).
- Generics with ParquetValue protocol instead of Go's code generation
- ParquetRecord recursive enum for nested types (List, Map, Struct)
- Dremel algorithm for shredding/assembling nested records to/from flat columns
- Data/[UInt8] with ARC instead of Go's manual memory.Buffer refcounting
- Thrift compact protocol implemented by hand (~500 lines), not generated
- Pure Swift Snappy and Zstd — no C imports
- CryptoKit/swift-crypto for AES-GCM — no OpenSSL
- Swift concurrency (async/await) for parallel column reading

## Reference Source
Primary: github.com/apache/arrow-go/parquet (Apache 2.0)
Spec: github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
Test files: github.com/apache/parquet-testing

## Validation
All output must be readable by:
- pyarrow: python3 -c "import pyarrow.parquet as pq; print(pq.read_table('test.parquet'))"
- DuckDB: duckdb -c "SELECT * FROM 'test.parquet'"

## Completed Phases
- Phase 1: Flat-column writer (Plain encoding, no compression)
- Phase 2: File reader, Snappy/Gzip compression, dictionary encoding, statistics, roundtrip tests
- Phase 3: AES-GCM encryption, Bloom filters, Zstd compression, delta encodings
- Phase 4: Linux support, Data Page V2, nested types (Dremel), streaming writer, parallel async reader
