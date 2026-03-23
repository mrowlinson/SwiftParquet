import Testing
import Foundation
@testable import SwiftParquet

// MARK: - Roundtrip Tests (write → read)

@Test func roundtripStrings() throws {
    let path = NSTemporaryDirectory() + "roundtrip-strings-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "name", type: .byteArray)
    builder.addColumn(name: "city", type: .byteArray)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema)
    try writer.writeRowGroup(columns: [
        ("name", .strings(["Alice", "Bob", "Charlie"])),
        ("city", .strings(["NYC", "LA", "Chicago"])),
    ])
    try writer.close()

    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 3)
    #expect(table.columnNames == ["name", "city"])
    if case .strings(let names) = table.column("name") { #expect(names == ["Alice", "Bob", "Charlie"]) }
    if case .strings(let cities) = table.column("city") { #expect(cities == ["NYC", "LA", "Chicago"]) }
}

@Test func roundtripMixedTypes() throws {
    let path = NSTemporaryDirectory() + "roundtrip-mixed-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "name", type: .byteArray)
    builder.addColumn(name: "age", type: .int32)
    builder.addColumn(name: "score", type: .double)
    builder.addColumn(name: "active", type: .boolean)
    builder.addColumn(name: "count", type: .int64)
    builder.addColumn(name: "rating", type: .float)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema)
    try writer.writeRowGroup(columns: [
        ("name",   .strings(["Alice", "Bob", "Charlie"])),
        ("age",    .int32s([30, 25, 40])),
        ("score",  .doubles([95.5, 87.0, 92.3])),
        ("active", .booleans([true, false, true])),
        ("count",  .int64s([1000, 2000, 3000])),
        ("rating", .floats([4.5, 3.2, 4.9])),
    ])
    try writer.close()

    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 3)
    if case .strings(let v) = table.column("name") { #expect(v == ["Alice", "Bob", "Charlie"]) }
    if case .int32s(let v) = table.column("age") { #expect(v == [30, 25, 40]) }
    if case .doubles(let v) = table.column("score") { #expect(v == [95.5, 87.0, 92.3]) }
    if case .booleans(let v) = table.column("active") { #expect(v == [true, false, true]) }
    if case .int64s(let v) = table.column("count") { #expect(v == [1000, 2000, 3000]) }
    if case .floats(let v) = table.column("rating") { #expect(v == [4.5, 3.2, 4.9]) }
}

// MARK: - Snappy Compression

@Test func snappyCompressDecompress() throws {
    let original = Data("Hello, World! This is a test of Snappy compression. Repeated text repeated text.".utf8)
    let codec = SnappyCodec()
    let compressed = try codec.compress(original)
    let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
    #expect(decompressed == original)
}

@Test func writeAndReadSnappy() throws {
    let path = NSTemporaryDirectory() + "snappy-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "name", type: .byteArray)
    builder.addColumn(name: "value", type: .int64)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema, options: .init(compression: .snappy))
    try writer.writeRowGroup(columns: [
        ("name",  .strings(["alpha", "beta", "gamma", "delta"])),
        ("value", .int64s([100, 200, 300, 400])),
    ])
    try writer.close()

    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 4)
    if case .strings(let v) = table.column("name") { #expect(v == ["alpha", "beta", "gamma", "delta"]) }
    if case .int64s(let v) = table.column("value") { #expect(v == [100, 200, 300, 400]) }
    validateWithPyarrow(path: path, expectedRows: 4, expectedColumns: ["name", "value"])
}

// MARK: - Gzip Compression

@Test func gzipCompressDecompress() throws {
    let original = Data("Gzip test data with some content to compress. Repeated text repeated text.".utf8)
    let codec = GzipCodec()
    let compressed = try codec.compress(original)
    let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
    #expect(decompressed == original)
}

@Test func writeAndReadGzip() throws {
    let path = NSTemporaryDirectory() + "gzip-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "city", type: .byteArray)
    builder.addColumn(name: "pop", type: .int64)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema, options: .init(compression: .gzip))
    try writer.writeRowGroup(columns: [
        ("city", .strings(["NYC", "LA", "Chicago"])),
        ("pop",  .int64s([8_336_817, 3_979_576, 2_693_976])),
    ])
    try writer.close()

    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 3)
    if case .strings(let v) = table.column("city") { #expect(v == ["NYC", "LA", "Chicago"]) }
    if case .int64s(let v) = table.column("pop") { #expect(v == [8_336_817, 3_979_576, 2_693_976]) }
    validateWithPyarrow(path: path, expectedRows: 3, expectedColumns: ["city", "pop"])
}

// MARK: - Dictionary Encoding

@Test func writeAndReadDictionary() throws {
    let path = NSTemporaryDirectory() + "dict-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "category", type: .byteArray)
    builder.addColumn(name: "value", type: .int32)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema, options: .init(useDictionary: true))
    try writer.writeRowGroup(columns: [
        ("category", .strings(["A", "B", "A", "C", "B", "A", "C", "A"])),
        ("value",    .int32s([1, 2, 3, 4, 5, 6, 7, 8])),
    ])
    try writer.close()

    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 8)
    if case .strings(let v) = table.column("category") { #expect(v == ["A", "B", "A", "C", "B", "A", "C", "A"]) }
    if case .int32s(let v) = table.column("value") { #expect(v == [1, 2, 3, 4, 5, 6, 7, 8]) }
    validateWithPyarrow(path: path, expectedRows: 8, expectedColumns: ["category", "value"])
}

// MARK: - Statistics

@Test func columnStatistics() throws {
    let path = NSTemporaryDirectory() + "stats-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "value", type: .int64)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema, options: .init(enableStatistics: true))
    try writer.writeRowGroup(columns: [
        ("value", .int64s([10, 50, 30, 20, 40])),
    ])
    try writer.close()

    let reader = try ParquetFileReaderCore(path: path)
    let stats = reader.metadata.rowGroups[0].columns[0].metaData.statistics
    #expect(stats != nil)
    if let s = stats {
        #expect(s.nullCount == 0)
        if let minVal = s.minValue {
            let v = PlainDecoder.decodeInt64s(minVal, count: 1)
            #expect(v.first == 10)
        }
        if let maxVal = s.maxValue {
            let v = PlainDecoder.decodeInt64s(maxVal, count: 1)
            #expect(v.first == 50)
        }
    }
}

// MARK: - Cross-tool: pyarrow writes → SwiftParquet reads

@Test func readPyarrowWritten() throws {
    guard let python = findPython3() else { return }
    let path = NSTemporaryDirectory() + "pyarrow-written-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let proc = Process()
    proc.executableURL = URL(fileURLWithPath: python)
    proc.arguments = ["-c", """
    import pyarrow as pa, pyarrow.parquet as pq
    t = pa.table({'name': ['Alice', 'Bob', 'Charlie'], 'age': [30, 25, 40], 'score': [95.5, 87.0, 92.3]})
    pq.write_table(t, '\(path)')
    print('OK')
    """]
    let pipe = Pipe()
    proc.standardOutput = pipe
    proc.standardError = Pipe()
    try proc.run()
    proc.waitUntilExit()

    let output = String(data: pipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
    guard output.trimmingCharacters(in: .whitespacesAndNewlines) == "OK" else {
        Issue.record("pyarrow write failed: \(output)"); return
    }

    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 3)
    #expect(table.columnNames == ["name", "age", "score"])
    if case .strings(let v) = table.column("name") { #expect(v == ["Alice", "Bob", "Charlie"]) }
    if case .int64s(let v) = table.column("age") { #expect(v == [30, 25, 40]) }
    if case .doubles(let v) = table.column("score") { #expect(v == [95.5, 87.0, 92.3]) }
}

@Test func readPyarrowSnappy() throws {
    guard let python = findPython3() else { return }
    let path = NSTemporaryDirectory() + "pyarrow-snappy-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let proc = Process()
    proc.executableURL = URL(fileURLWithPath: python)
    proc.arguments = ["-c", """
    import pyarrow as pa, pyarrow.parquet as pq
    t = pa.table({'x': [1, 2, 3, 4, 5], 'y': ['a', 'b', 'c', 'd', 'e']})
    pq.write_table(t, '\(path)', compression='snappy')
    print('OK')
    """]
    let pipe = Pipe()
    proc.standardOutput = pipe
    proc.standardError = Pipe()
    try proc.run()
    proc.waitUntilExit()

    let output = String(data: pipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
    guard output.trimmingCharacters(in: .whitespacesAndNewlines) == "OK" else {
        Issue.record("pyarrow snappy write failed"); return
    }

    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 5)
    if case .int64s(let v) = table.column("x") { #expect(v == [1, 2, 3, 4, 5]) }
    if case .strings(let v) = table.column("y") { #expect(v == ["a", "b", "c", "d", "e"]) }
}

// MARK: - Bloom Filter

@Test func bloomFilterInsertAndQuery() throws {
    var filter = BloomFilter(numDistinct: 100)
    let hash1 = BloomFilter.hashString("hello")
    let hash2 = BloomFilter.hashString("world")

    filter.insert(hash: hash1)
    filter.insert(hash: hash2)

    #expect(filter.mightContain(hash: hash1) == true)
    #expect(filter.mightContain(hash: hash2) == true)
}

// MARK: - RLE Roundtrip

@Test func rleRoundtrip() throws {
    let values: [Int32] = [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1]
    let encoder = RLEEncoder(bitWidth: 1)
    let encoded = encoder.encode(values)
    let decoder = RLEDecoder(bitWidth: 1)
    let decoded = decoder.decode(encoded, expectedCount: values.count)
    #expect(decoded == values)
}

// MARK: - Delta Binary Packed Roundtrip

@Test func deltaBinaryPackedRoundtrip() throws {
    let values: [Int64] = [100, 102, 105, 110, 115, 120, 130, 140, 150, 200]
    let encoded = DeltaBinaryPackedEncoder.encode(values)
    let decoded = try DeltaBinaryPackedDecoder.decode(encoded)
    #expect(decoded == values)
}
