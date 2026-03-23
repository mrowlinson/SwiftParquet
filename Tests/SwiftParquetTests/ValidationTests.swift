import Testing
import Foundation
@testable import SwiftParquet

/// Writes test.parquet to /tmp and validates with pyarrow.
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

    guard let result = runPyarrow("""
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
        """) else { return }

    #expect(result["ok"] as? Bool == true, "pyarrow error: \(result["error"] ?? "unknown")")
    #expect(result["rows"] as? Int == 5)

    let columns = result["columns"] as? [String] ?? []
    #expect(columns == ["title", "author", "category"])

    let titles = result["titles"] as? [String] ?? []
    #expect(titles == ["Article 1", "Article 2", "Article 3", "Article 4", "Article 5"])

    let authors = result["authors"] as? [String] ?? []
    #expect(authors == ["Alice", "Bob", "Charlie", "Diana", "Eve"])
}
