import Testing
import Foundation
@testable import SwiftParquet

/// Writes test.parquet to /tmp and validates with pyarrow + DuckDB.
/// This test writes to a persistent path so you can also validate manually:
///   python3 -c "import pyarrow.parquet as pq; print(pq.read_table('/tmp/swiftparquet_test.parquet').to_pandas())"
@Test func writeAndValidateManually() throws {
    let path = "/tmp/swiftparquet_test.parquet"

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

    // pyarrow validation
    let proc = Process()
    proc.executableURL = URL(fileURLWithPath: "/usr/local/bin/python3")
    proc.arguments = ["-c", """
import pyarrow.parquet as pq, json
try:
    t = pq.read_table('\(path)')
    df = t.to_pandas()
    result = {
        "ok": True,
        "columns": t.column_names,
        "rows": t.num_rows,
        "titles": df["title"].tolist(),
        "authors": df["author"].tolist(),
    }
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

    #expect(result["ok"] as? Bool == true,
            "pyarrow error: \(result["error"] ?? "unknown")\nstderr: \(errOutput)")
    #expect(result["rows"] as? Int == 5)

    let columns = result["columns"] as? [String] ?? []
    #expect(columns == ["title", "author", "category"])

    let titles = result["titles"] as? [String] ?? []
    #expect(titles == ["Article 1", "Article 2", "Article 3", "Article 4", "Article 5"])

    let authors = result["authors"] as? [String] ?? []
    #expect(authors == ["Alice", "Bob", "Charlie", "Diana", "Eve"])
}
