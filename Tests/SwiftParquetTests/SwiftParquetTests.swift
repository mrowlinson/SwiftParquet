import Testing
import Foundation
@testable import SwiftParquet

// MARK: - Phase 1 Validation Test

@Test func writeThreeStringColumns() throws {
    let path = NSTemporaryDirectory() + "test-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "title", type: .byteArray)
    builder.addColumn(name: "author", type: .byteArray)
    builder.addColumn(name: "category", type: .byteArray)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema)
    try writer.writeRowGroup(columns: [
        ("title",    .strings(["Article 1", "Article 2", "Article 3", "Article 4", "Article 5"])),
        ("author",   .strings(["Alice", "Bob", "Charlie", "Diana", "Eve"])),
        ("category", .strings(["Tech", "Science", "Tech", "Art", "Science"])),
    ])
    try writer.close()

    let fileSize = try FileManager.default.attributesOfItem(atPath: path)[.size] as! Int
    #expect(fileSize > 0, "Output file should not be empty")

    guard let result = runPyarrow("""
        import pyarrow.parquet as pq, json, sys
        try:
            t = pq.read_table('\(path)')
            result = {"columns": t.column_names, "rows": t.num_rows, "ok": True}
            print(json.dumps(result))
        except Exception as e:
            print(json.dumps({"ok": False, "error": str(e)}))
        """) else { return }

    #expect(result["ok"] as? Bool == true, "pyarrow should read the file without errors")
    #expect(result["rows"] as? Int == 5, "Expected 5 rows")
    let columns = result["columns"] as? [String] ?? []
    #expect(columns == ["title", "author", "category"], "Columns should match schema order")
}

@Test func writeMixedTypes() throws {
    let path = NSTemporaryDirectory() + "test-mixed-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "name", type: .byteArray)
    builder.addColumn(name: "age", type: .int32)
    builder.addColumn(name: "score", type: .double)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema)
    try writer.writeRowGroup(columns: [
        ("name",  .strings(["Alice", "Bob", "Charlie"])),
        ("age",   .int32s([30, 25, 40])),
        ("score", .doubles([95.5, 87.0, 92.3])),
    ])
    try writer.close()

    guard let result = runPyarrow("""
        import pyarrow.parquet as pq, json
        try:
            t = pq.read_table('\(path)')
            result = {"columns": t.column_names, "rows": t.num_rows, "ok": True}
            print(json.dumps(result))
        except Exception as e:
            print(json.dumps({"ok": False, "error": str(e)}))
        """) else { return }

    #expect(result["ok"] as? Bool == true, "pyarrow should read the mixed-type file")
    #expect(result["rows"] as? Int == 3)
}
