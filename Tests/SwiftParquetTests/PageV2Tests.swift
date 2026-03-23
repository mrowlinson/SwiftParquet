import Testing
import Foundation
@testable import SwiftParquet

// MARK: - Data Page V2 Roundtrip

@Test func writeAndReadV2() throws {
    let path = NSTemporaryDirectory() + "v2-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "name", type: .byteArray)
    builder.addColumn(name: "age", type: .int32)
    builder.addColumn(name: "score", type: .double)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema,
                                    options: .init(dataPageVersion: .v2))
    try writer.writeRowGroup(columns: [
        ("name",  .strings(["Alice", "Bob", "Charlie"])),
        ("age",   .int32s([30, 25, 40])),
        ("score", .doubles([95.5, 87.0, 92.3])),
    ])
    try writer.close()

    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 3)
    if case .strings(let v) = table.column("name") { #expect(v == ["Alice", "Bob", "Charlie"]) }
    if case .int32s(let v) = table.column("age") { #expect(v == [30, 25, 40]) }
    if case .doubles(let v) = table.column("score") { #expect(v == [95.5, 87.0, 92.3]) }
}

@Test func writeV2WithSnappy() throws {
    let path = NSTemporaryDirectory() + "v2-snappy-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "category", type: .byteArray)
    builder.addColumn(name: "count", type: .int64)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema,
                                    options: .init(compression: .snappy, dataPageVersion: .v2))
    try writer.writeRowGroup(columns: [
        ("category", .strings(["A", "B", "C", "D", "E"])),
        ("count",    .int64s([100, 200, 300, 400, 500])),
    ])
    try writer.close()

    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 5)
    if case .strings(let v) = table.column("category") { #expect(v == ["A", "B", "C", "D", "E"]) }
    if case .int64s(let v) = table.column("count") { #expect(v == [100, 200, 300, 400, 500]) }

    validateWithPyarrow(path: path, expectedRows: 5, expectedColumns: ["category", "count"])
}

// MARK: - Read pyarrow-written V2 files

@Test func readPyarrowV2() throws {
    guard let python = findPython3() else { return }
    let path = NSTemporaryDirectory() + "pyarrow-v2-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let proc = Process()
    proc.executableURL = URL(fileURLWithPath: python)
    proc.arguments = ["-c", """
    import pyarrow as pa, pyarrow.parquet as pq
    t = pa.table({'x': [1, 2, 3, 4, 5], 'y': ['a', 'b', 'c', 'd', 'e']})
    pq.write_table(t, '\(path)', data_page_version='2.0')
    print('OK')
    """]
    let pipe = Pipe()
    proc.standardOutput = pipe
    proc.standardError = Pipe()
    try proc.run()
    proc.waitUntilExit()

    let output = String(data: pipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
    guard output.trimmingCharacters(in: .whitespacesAndNewlines) == "OK" else {
        Issue.record("pyarrow V2 write failed"); return
    }

    let table = try ParquetFileReader.read(path: path)
    #expect(table.numRows == 5)
    if case .int64s(let v) = table.column("x") { #expect(v == [1, 2, 3, 4, 5]) }
    if case .strings(let v) = table.column("y") { #expect(v == ["a", "b", "c", "d", "e"]) }
}
