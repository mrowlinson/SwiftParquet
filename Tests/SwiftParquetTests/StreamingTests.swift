import Testing
import Foundation
@testable import SwiftParquet

// MARK: - Streaming Writer

@Test func streamingWriteAndRead() throws {
    let path = NSTemporaryDirectory() + "streaming-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "id", type: .int64)
    builder.addColumn(name: "name", type: .byteArray)
    let schema = builder.build()

    // Write 3 row groups (batches) via streaming
    var writer = try ParquetStreamWriter(path: path, schema: schema, options: .init(compression: .snappy))
    try writer.writeBatch(columns: [
        ("id",   .int64s([1, 2, 3])),
        ("name", .strings(["Alice", "Bob", "Charlie"])),
    ])
    try writer.writeBatch(columns: [
        ("id",   .int64s([4, 5])),
        ("name", .strings(["Diana", "Eve"])),
    ])
    try writer.writeBatch(columns: [
        ("id",   .int64s([6])),
        ("name", .strings(["Frank"])),
    ])
    try writer.close()

    // Read back and verify all data
    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 6)
    if case .int64s(let v) = table.column("id") { #expect(v == [1, 2, 3, 4, 5, 6]) }
    if case .strings(let v) = table.column("name") { #expect(v == ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"]) }

    // Verify file has 3 row groups
    let reader = try ParquetFileReaderCore(path: path)
    #expect(reader.numRowGroups == 3)
    #expect(reader.metadata.rowGroups[0].numRows == 3)
    #expect(reader.metadata.rowGroups[1].numRows == 2)
    #expect(reader.metadata.rowGroups[2].numRows == 1)

    validateWithPyarrow(path: path, expectedRows: 6, expectedColumns: ["id", "name"])
}

@Test func streamingLargeFile() throws {
    let path = NSTemporaryDirectory() + "streaming-large-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "value", type: .int64)
    let schema = builder.build()

    var writer = try ParquetStreamWriter(path: path, schema: schema)

    // Write 100 batches of 100 rows = 10,000 total
    let batchSize = 100
    for batch in 0..<100 {
        let start = Int64(batch * batchSize)
        let values = (0..<batchSize).map { start + Int64($0) }
        try writer.writeBatch(columns: [
            ("value", .int64s(values)),
        ])
    }
    try writer.close()

    // Verify
    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 10_000)
    if case .int64s(let v) = table.column("value") {
        #expect(v.count == 10_000)
        #expect(v.first == 0)
        #expect(v.last == 9999)
    }
}
