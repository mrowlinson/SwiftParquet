import Testing
import Foundation
@testable import SwiftParquet

// MARK: - Phase 1 Validation Test

@Test func writeThreeStringColumns() throws {
    let path = NSTemporaryDirectory() + "test-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    // Build schema with 3 string columns
    var builder = SchemaBuilder()
    builder.addColumn(name: "title", type: .byteArray)
    builder.addColumn(name: "author", type: .byteArray)
    builder.addColumn(name: "category", type: .byteArray)
    let schema = builder.build()

    // Write 5 rows
    var writer = ParquetFileWriter(path: path, schema: schema)
    try writer.writeRowGroup(columns: [
        ("title",    .strings(["Article 1", "Article 2", "Article 3", "Article 4", "Article 5"])),
        ("author",   .strings(["Alice", "Bob", "Charlie", "Diana", "Eve"])),
        ("category", .strings(["Tech", "Science", "Tech", "Art", "Science"])),
    ])
    try writer.close()

    // Validate the file exists and has content
    let fileSize = try FileManager.default.attributesOfItem(atPath: path)[.size] as! Int
    #expect(fileSize > 0, "Output file should not be empty")

    // Validate with pyarrow
    let proc = Process()
    proc.executableURL = URL(fileURLWithPath: "/usr/local/bin/python3")
    proc.arguments = ["-c", """
import pyarrow.parquet as pq, json, sys
try:
    t = pq.read_table('\(path)')
    result = {"columns": t.column_names, "rows": t.num_rows, "ok": True}
    print(json.dumps(result))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
"""]
    let stdoutPipe = Pipe()
    let stderrPipe = Pipe()
    proc.standardOutput = stdoutPipe
    proc.standardError = stderrPipe
    try proc.run()
    proc.waitUntilExit()

    let output = String(data: stdoutPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
    let errOutput = String(data: stderrPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""

    guard !output.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
        Issue.record("pyarrow produced no output. stderr: \(errOutput)")
        return
    }

    let result = try JSONSerialization.jsonObject(with: Data(output.utf8)) as! [String: Any]

    #expect(result["ok"] as? Bool == true, "pyarrow should read the file without errors. Error: \(result["error"] ?? "unknown"). stderr: \(errOutput)")
    #expect(result["rows"] as? Int == 5, "Expected 5 rows, got \(result["rows"] ?? "nil")")

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

    // Validate with pyarrow
    let proc = Process()
    proc.executableURL = URL(fileURLWithPath: "/usr/local/bin/python3")
    proc.arguments = ["-c", """
import pyarrow.parquet as pq, json
try:
    t = pq.read_table('\(path)')
    result = {"columns": t.column_names, "rows": t.num_rows, "ok": True}
    print(json.dumps(result))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
"""]
    let stdoutPipe2 = Pipe()
    let stderrPipe2 = Pipe()
    proc.standardOutput = stdoutPipe2
    proc.standardError = stderrPipe2
    try proc.run()
    proc.waitUntilExit()

    let output = String(data: stdoutPipe2.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
    let errOutput = String(data: stderrPipe2.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""

    guard !output.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
        Issue.record("pyarrow produced no output for mixed-type file. stderr: \(errOutput)")
        return
    }

    let result = try JSONSerialization.jsonObject(with: Data(output.utf8)) as! [String: Any]

    #expect(result["ok"] as? Bool == true, "pyarrow should read the mixed-type file. Error: \(result["error"] ?? "unknown")")
    #expect(result["rows"] as? Int == 3)
}
