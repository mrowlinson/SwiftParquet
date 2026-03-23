import Testing
import Foundation
@testable import SwiftParquet

// MARK: - Async Parallel Reading

@Test func asyncReadMatchesSync() async throws {
    let path = NSTemporaryDirectory() + "async-test-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "name", type: .byteArray)
    builder.addColumn(name: "age", type: .int32)
    builder.addColumn(name: "score", type: .double)
    builder.addColumn(name: "active", type: .boolean)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema)
    try writer.writeRowGroup(columns: [
        ("name",   .strings(["Alice", "Bob", "Charlie"])),
        ("age",    .int32s([30, 25, 40])),
        ("score",  .doubles([95.5, 87.0, 92.3])),
        ("active", .booleans([true, false, true])),
    ])
    try writer.close()

    // Read synchronously
    let syncTable = try ParquetFileReader.read(path: path)

    // Read asynchronously (parallel column decoding)
    let asyncTable = try await ParquetFileReader.readAsync(path: path)

    // Verify they match
    #expect(syncTable.numRows == asyncTable.numRows)
    #expect(syncTable.columnNames == asyncTable.columnNames)

    for name in syncTable.columnNames {
        let syncCol = syncTable.column(name)
        let asyncCol = asyncTable.column(name)
        switch (syncCol, asyncCol) {
        case (.strings(let a), .strings(let b)): #expect(a == b)
        case (.int32s(let a), .int32s(let b)): #expect(a == b)
        case (.doubles(let a), .doubles(let b)): #expect(a == b)
        case (.booleans(let a), .booleans(let b)): #expect(a == b)
        default: break
        }
    }
}

@Test func asyncReadMultipleRowGroups() async throws {
    let path = NSTemporaryDirectory() + "async-multi-rg-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "value", type: .int64)
    let schema = builder.build()

    // Write 3 row groups via streaming
    var writer = try ParquetStreamWriter(path: path, schema: schema)
    try writer.writeBatch(columns: [("value", .int64s([1, 2, 3]))])
    try writer.writeBatch(columns: [("value", .int64s([4, 5]))])
    try writer.writeBatch(columns: [("value", .int64s([6, 7, 8, 9]))])
    try writer.close()

    let table = try await ParquetFileReader.readAsync(path: path)
    #expect(table.numRows == 9)
    if case .int64s(let v) = table.column("value") {
        #expect(v == [1, 2, 3, 4, 5, 6, 7, 8, 9])
    }
}
