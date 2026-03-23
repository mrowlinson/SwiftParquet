# SwiftParquet

A pure Swift implementation of the [Apache Parquet](https://parquet.apache.org/) file format for macOS, iOS, and Linux.

## Features

| Category | Details |
|---|---|
| **Compression** | Snappy, Gzip, Zstd (all pure Swift on macOS/iOS; system zlib on Linux) |
| **Encodings** | Plain, Dictionary (RLE), Delta Binary Packed, Delta Byte Array |
| **Nested types** | List, Map, Struct via the Dremel algorithm |
| **Page formats** | Data page V1 and V2 |
| **Encryption** | AES-GCM-128/192/256 column and footer encryption |
| **Bloom filters** | Split-block with xxHash64 |
| **Statistics** | Per-column min/max/null_count |
| **Streaming** | Write large files with constant memory via `ParquetStreamWriter` |
| **Async** | Parallel column reading with Swift concurrency |
| **Platforms** | macOS 13+, iOS 16+, Linux (Swift 6.x) |
| **Dependencies** | None on Apple platforms. [swift-crypto](https://github.com/apple/swift-crypto) on Linux only. |

Validated against **pyarrow** and **DuckDB** on every test run (29 tests).

## Installation

```swift
// Package.swift
dependencies: [
    .package(url: "https://github.com/mrowlinson/SwiftParquet", from: "0.4.0"),
]
```

On Linux, install zlib: `apt install zlib1g-dev`

## Usage

### Write flat columns

```swift
import SwiftParquet

var schema = SchemaBuilder()
schema.addColumn(name: "city",  type: .byteArray)
schema.addColumn(name: "pop",   type: .int64)
schema.addColumn(name: "score", type: .double)

var writer = ParquetFileWriter(path: "cities.parquet", schema: schema.build(), options: .snappy)
try writer.writeRowGroup(columns: [
    ("city",  .strings(["Tokyo", "Delhi", "Shanghai"])),
    ("pop",   .int64s([37_400_068, 32_941_000, 29_210_808])),
    ("score", .doubles([9.1, 8.4, 8.8])),
])
try writer.close()
```

### Read

```swift
let table = try ParquetFileReader.read(path: "cities.parquet")
print(table.columnNames)  // ["city", "pop", "score"]
print(table.numRows)      // 3

if case .int64s(let pops) = table.column("pop") {
    print(pops)  // [37400068, 32941000, 29210808]
}
```

### Async parallel read

```swift
let table = try await ParquetFileReader.readAsync(path: "cities.parquet")
```

### Nested types (List, Map, Struct)

```swift
var schema = SchemaBuilder()
schema.addColumn(name: "name", type: .byteArray)
schema.addList(name: "tags", elementType: .byteArray)
schema.addMap(name: "scores", keyType: .byteArray, valueType: .int32)

var writer = ParquetFileWriter(path: "nested.parquet", schema: schema.build())
try writer.writeRows([
    .struct([
        ("name", .string("Alice")),
        ("tags", .list([.string("swift"), .string("parquet")])),
        ("scores", .map([
            (key: .string("math"), value: .int32(95)),
            (key: .string("eng"),  value: .int32(88)),
        ])),
    ]),
    .struct([
        ("name", .string("Bob")),
        ("tags", .list([.string("data")])),
        ("scores", .map([
            (key: .string("math"), value: .int32(72)),
        ])),
    ]),
])
try writer.close()
```

### Streaming writer (constant memory)

```swift
var schema = SchemaBuilder()
schema.addColumn(name: "id", type: .int64)
schema.addColumn(name: "value", type: .double)

var writer = try ParquetStreamWriter(path: "large.parquet", schema: schema.build(), options: .zstd)
for batch in batches {
    try writer.writeBatch(columns: [
        ("id",    .int64s(batch.ids)),
        ("value", .doubles(batch.values)),
    ])
}
try writer.close()
```

### Encryption

```swift
import CryptoKit  // or Crypto on Linux

let key = SymmetricKey(size: .bits128)
var writer = ParquetFileWriter(
    path: "encrypted.parquet",
    schema: schema.build(),
    encryption: .uniformEncryption(key: key)
)
// ... write and close as normal
```

### Write options

```swift
ParquetWriteOptions()           // no compression
ParquetWriteOptions.snappy      // snappy + dictionary
ParquetWriteOptions.gzip        // gzip + dictionary
ParquetWriteOptions.zstd        // zstd + dictionary

// Custom
ParquetWriteOptions(
    compression: .snappy,
    useDictionary: true,
    enableStatistics: true,
    dataPageVersion: .v2
)
```

## Type mapping

| Swift | Parquet | ColumnValues |
|---|---|---|
| `String` | `BYTE_ARRAY` (UTF-8) | `.strings([...])` |
| `Int32` | `INT32` | `.int32s([...])` |
| `Int64` | `INT64` | `.int64s([...])` |
| `Float` | `FLOAT` | `.floats([...])` |
| `Double` | `DOUBLE` | `.doubles([...])` |
| `Bool` | `BOOLEAN` | `.booleans([...])` |
| `Data` | `BYTE_ARRAY` | `.byteArrays([...])` |

For nested data, use `ParquetRecord`:

| ParquetRecord case | Parquet logical type |
|---|---|
| `.list([...])` | `LIST` |
| `.map([(key:, value:)])` | `MAP` |
| `.struct([(String, ...)])` | Group |
| `.null` | null at any level |

## Architecture

```
Sources/SwiftParquet/
├── Types.swift                    # Core types, ParquetRecord enum
├── Thrift/ThriftCompact.swift     # TCompactProtocol reader + writer
├── Format/Generated.swift         # All Parquet metadata structs
├── Schema/
│   ├── SchemaNode.swift           # GroupNode, PrimitiveNode, ParquetSchema
│   ├── Column.swift               # ColumnDescriptor
│   └── Dremel.swift               # Shredder + assembler for nested types
├── Encoding/
│   ├── PlainEncoding.swift        # ParquetValue protocol + Plain codec
│   ├── RLE.swift                  # RLE/bit-packing hybrid
│   ├── DictionaryEncoding.swift   # Dictionary encoder/decoder
│   ├── DeltaEncoding.swift        # Delta Binary Packed, Delta Byte Array
│   └── BloomFilter.swift          # Split-block Bloom filter + xxHash64
├── Compress/
│   ├── Compression.swift          # Codec protocol + registry
│   ├── Snappy.swift               # Pure Swift
│   ├── Gzip.swift                 # Compression framework (Apple) / zlib (Linux)
│   ├── Zstd.swift                 # Pure Swift
│   └── CRC32.swift                # For gzip trailer
├── File/
│   ├── PageWriter.swift           # V1 + V2 page serialization
│   ├── PageReader.swift           # V1 + V2 page deserialization
│   ├── ColumnWriter.swift         # Per-column write with levels/compression/dict
│   ├── ColumnReader.swift         # Per-column read with all encodings
│   ├── RowGroupWriter.swift       # Row group assembly
│   ├── FileWriter.swift           # PAR1 framing + footer
│   ├── FileReader.swift           # File parsing + async parallel read
│   └── StreamingFileWriter.swift  # FileHandle-based streaming output
├── Convenience/
│   ├── SimpleWriter.swift         # ParquetFileWriter, SchemaBuilder
│   ├── SimpleReader.swift         # ParquetFileReader, ParquetTable
│   └── StreamingWriter.swift      # ParquetStreamWriter
└── Encrypt/
    └── AESEncryption.swift        # AES-GCM via CryptoKit / swift-crypto
```

## Developing

```sh
swift build
swift test   # 29 tests — roundtrips, compression, nested types, pyarrow interop
```

## License

Apache License 2.0
