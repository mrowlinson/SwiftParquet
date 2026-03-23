# SwiftParquet

Pure Swift implementation of the [Apache Parquet](https://parquet.apache.org/) file format. Port of [github.com/apache/arrow-go/parquet](https://github.com/apache/arrow-go/tree/main/parquet).

**Status:** Phase 1 complete — write flat-column Parquet files readable by pyarrow, DuckDB, and any standard Parquet reader.

## Features

- **Zero dependencies.** Pure Swift. No C, no bridging headers, no system libraries.
- **macOS 13+, iOS 16+** (Linux coming in Phase 2).
- **Write flat Parquet files** with any mix of physical types.
- **Plain encoding, no compression** (Phase 1). Dictionary encoding + Snappy/Gzip in Phase 2.
- **pyarrow and DuckDB compatible** — validated on every test run.

## Quick Start

```swift
import SwiftParquet

// 1. Define schema
var schema = SchemaBuilder()
schema.addColumn(name: "title",    type: .byteArray)
schema.addColumn(name: "views",    type: .int64)
schema.addColumn(name: "score",    type: .double)

// 2. Write
var writer = ParquetFileWriter(path: "output.parquet", schema: schema.build())
try writer.writeRowGroup(columns: [
    ("title",  .strings(["Article 1", "Article 2", "Article 3"])),
    ("views",  .int64s([1200, 450, 8900])),
    ("score",  .doubles([4.5, 3.1, 4.9])),
])
try writer.close()
```

Validate with pyarrow:
```sh
python3 -c "import pyarrow.parquet as pq; print(pq.read_table('output.parquet').to_pandas())"
```
```
      title  views  score
0  Article 1   1200    4.5
1  Article 2    450    3.1
2  Article 3   8900    4.9
```

Or DuckDB:
```sh
duckdb -c "SELECT * FROM 'output.parquet'"
```

## Installation

Add to `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/mrowlinson/SwiftParquet", from: "0.1.0"),
],
targets: [
    .target(name: "MyTarget", dependencies: ["SwiftParquet"]),
]
```

## Supported Types

| Swift type | Parquet physical type | SchemaBuilder argument |
|---|---|---|
| `String` | `BYTE_ARRAY` (UTF-8) | `.strings([...])` |
| `Int32` | `INT32` | `.int32s([...])` |
| `Int64` | `INT64` | `.int64s([...])` |
| `Float` | `FLOAT` | `.floats([...])` |
| `Double` | `DOUBLE` | `.doubles([...])` |
| `Bool` | `BOOLEAN` | `.booleans([...])` |
| `ByteArray` | `BYTE_ARRAY` | `.byteArrays([...])` |

## Architecture

```
Sources/SwiftParquet/
├── Types.swift              # PhysicalType, Encoding, Repetition, ParquetError, Int96
├── Thrift/
│   └── ThriftCompact.swift  # Hand-rolled TCompactProtocol (not generated)
├── Format/
│   └── Generated.swift      # FileMetaData, SchemaElement, PageHeader, etc.
├── Schema/
│   ├── SchemaNode.swift     # GroupNode, PrimitiveNode, ParquetSchema
│   └── Column.swift         # ColumnDescriptor
├── Encoding/
│   ├── PlainEncoding.swift  # ParquetValue protocol + all physical types
│   └── RLE.swift            # RLE/bit-packing hybrid (for def/rep levels)
├── File/
│   ├── PageWriter.swift     # Data page V1 serialization
│   ├── ColumnWriter.swift   # Per-column value accumulation and flushing
│   ├── RowGroupWriter.swift # Row group assembly
│   └── FileWriter.swift     # PAR1 magic + footer + FileMetaData
└── Convenience/
    └── SimpleWriter.swift   # ParquetFileWriter, SchemaBuilder, ColumnValues
```

### Key design decisions

- **Thrift is hand-rolled.** The Go source uses generated Thrift code via the Apache Thrift library. SwiftParquet implements `TCompactProtocol` directly (~300 lines) using a `ThriftCompactWriter` struct with a field-ID stack for nested struct tracking.
- **Generics replace code generation.** The Go library uses `go:generate` to produce typed column readers/writers. SwiftParquet uses `ColumnWriter<T: ParquetValue>` generics instead.
- **ARC replaces manual reference counting.** Go's `memory.Buffer` with `Retain()`/`Release()` is replaced with `Data` and `[UInt8]` managed by ARC.

## Development

```sh
swift build
swift test
```

Reference source is at `/tmp/arrow-go/` (clone with `git clone --depth 1 https://github.com/apache/arrow-go.git /tmp/arrow-go`).

## Roadmap

### Phase 1 (complete)
- [x] Flat-column Parquet writer (Plain encoding, no compression)
- [x] All primitive physical types
- [x] pyarrow + DuckDB interop validation

### Phase 2 (next)
- [ ] File reader + column reader
- [ ] Snappy and Gzip compression
- [ ] Dictionary encoding (major compression win for string columns)
- [ ] Column statistics (min/max/null\_count)
- [ ] Roundtrip tests (write→read, pyarrow→SwiftParquet, SwiftParquet→pyarrow)

### Phase 3 (requires user confirmation before starting)
- [ ] Nested types: List, Map, Struct (Dremel algorithm)
- [ ] AES-GCM encryption
- [ ] Split block Bloom filters
- [ ] Zstd compression
- [ ] Delta encodings (DeltaBinaryPacked, DeltaByteArray)

## License

Apache License 2.0 — matching the upstream Apache Arrow Go implementation.
