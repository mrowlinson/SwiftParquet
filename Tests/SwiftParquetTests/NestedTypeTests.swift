import Testing
import Foundation
@testable import SwiftParquet

// MARK: - Dremel Shredder Unit Tests

@Test func shredFlatStruct() throws {
    // A simple flat struct should shred to flat columns with trivial levels
    var builder = SchemaBuilder()
    builder.addColumn(name: "name", type: .byteArray)
    builder.addColumn(name: "age", type: .int32)
    let schema = builder.build()

    let rows: [ParquetRecord] = [
        .struct([("name", .string("Alice")), ("age", .int32(30))]),
        .struct([("name", .string("Bob")),   ("age", .int32(25))]),
    ]

    let shredded = DremelShredder.shred(rows: rows, schema: schema.root)
    #expect(shredded.count == 2)

    // name column
    #expect(shredded[0].values.count == 2)
    #expect(shredded[0].defLevels == [0, 0])
    #expect(shredded[0].repLevels == [0, 0])

    // age column
    #expect(shredded[1].values.count == 2)
}

@Test func shredListColumn() throws {
    var builder = SchemaBuilder()
    builder.addList(name: "tags", elementType: .byteArray)
    let schema = builder.build()

    let rows: [ParquetRecord] = [
        .struct([("tags", .list([.string("a"), .string("b"), .string("c")]))]),
        .struct([("tags", .list([.string("d")]))]),
        .struct([("tags", .list([]))]),  // empty list
    ]

    let shredded = DremelShredder.shred(rows: rows, schema: schema.root)
    #expect(shredded.count == 1)  // one leaf column: tags.list.element

    let col = shredded[0]
    // Row 0: 3 elements, Row 1: 1 element, Row 2: empty list
    #expect(col.values.count == 4)  // a, b, c, d

    // Rep levels: 0 for first element, maxRep for subsequent in same list
    // Row 0: [0, maxRep, maxRep], Row 1: [0], Row 2: [0]
    #expect(col.repLevels[0] == 0)
    #expect(col.repLevels[3] == 0) // first element of row 1

    // Def levels should distinguish null vs empty vs present
    // Row 2 (empty list) should have lower def level than present elements
}

// MARK: - List Roundtrip (write → read with pyarrow validation)

@Test func writeListAndValidateWithPyarrow() throws {
    let path = NSTemporaryDirectory() + "list-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "id", type: .int32)
    builder.addList(name: "tags", elementType: .byteArray)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema)
    try writer.writeRows([
        .struct([("id", .int32(1)), ("tags", .list([.string("a"), .string("b")]))]),
        .struct([("id", .int32(2)), ("tags", .list([.string("c")]))]),
        .struct([("id", .int32(3)), ("tags", .list([]))]),
    ])
    try writer.close()

    // Validate with pyarrow
    guard let result = runPyarrow("""
        import pyarrow.parquet as pq, json
        try:
            t = pq.read_table('\(path)')
            ids = t.column('id').to_pylist()
            tags = t.column('tags').to_pylist()
            print(json.dumps({"ok": True, "rows": t.num_rows, "ids": ids, "tags": tags}))
        except Exception as e:
            print(json.dumps({"ok": False, "error": str(e)}))
        """) else { return }

    #expect(result["ok"] as? Bool == true, "pyarrow error: \(result["error"] ?? "unknown")")
    #expect(result["rows"] as? Int == 3)
    if let ids = result["ids"] as? [Int] {
        #expect(ids == [1, 2, 3])
    }
    if let tags = result["tags"] as? [[String]] {
        #expect(tags == [["a", "b"], ["c"], []])
    }
}

// MARK: - Map Column

@Test func writeMapAndValidateWithPyarrow() throws {
    let path = NSTemporaryDirectory() + "map-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var builder = SchemaBuilder()
    builder.addColumn(name: "id", type: .int32)
    builder.addMap(name: "attrs", keyType: .byteArray, valueType: .int32)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema)
    try writer.writeRows([
        .struct([("id", .int32(1)), ("attrs", .map([
            (key: .string("x"), value: .int32(10)),
            (key: .string("y"), value: .int32(20)),
        ]))]),
        .struct([("id", .int32(2)), ("attrs", .map([
            (key: .string("z"), value: .int32(30)),
        ]))]),
    ])
    try writer.close()

    guard let result = runPyarrow("""
        import pyarrow.parquet as pq, json
        try:
            t = pq.read_table('\(path)')
            print(json.dumps({"ok": True, "rows": t.num_rows, "columns": t.column_names}))
        except Exception as e:
            print(json.dumps({"ok": False, "error": str(e)}))
        """) else { return }

    #expect(result["ok"] as? Bool == true, "pyarrow error: \(result["error"] ?? "unknown")")
    #expect(result["rows"] as? Int == 2)
}

// MARK: - Read pyarrow-written List files

@Test func readPyarrowList() throws {
    guard let python = findPython3() else { return }
    let path = NSTemporaryDirectory() + "pyarrow-list-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let proc = Process()
    proc.executableURL = URL(fileURLWithPath: python)
    proc.arguments = ["-c", """
    import pyarrow as pa, pyarrow.parquet as pq
    t = pa.table({
        'id': [1, 2, 3],
        'tags': [['a', 'b'], ['c'], []],
    })
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
        Issue.record("pyarrow list write failed"); return
    }

    // Read with SwiftParquet — at minimum, verify it doesn't crash and reads the flat columns
    let reader = try ParquetFileReaderCore(path: path)
    #expect(reader.numRows == 3)
    #expect(reader.numColumns >= 1) // At least the id column + list's leaf column
}

// MARK: - Dremel assembler unit test

@Test func dremelShredAndAssembleRoundtrip() throws {
    var builder = SchemaBuilder()
    builder.addColumn(name: "name", type: .byteArray)
    builder.addColumn(name: "score", type: .int32)
    let schema = builder.build()

    let rows: [ParquetRecord] = [
        .struct([("name", .string("Alice")), ("score", .int32(100))]),
        .struct([("name", .string("Bob")),   ("score", .int32(200))]),
        .struct([("name", .string("Charlie")), ("score", .int32(300))]),
    ]

    let shredded = DremelShredder.shred(rows: rows, schema: schema.root)
    let assembled = DremelAssembler.assemble(columns: shredded, schema: schema.root, numRows: 3)

    #expect(assembled.count == 3)
    #expect(assembled[0] == .struct([("name", .string("Alice")), ("score", .int32(100))]))
    #expect(assembled[1] == .struct([("name", .string("Bob")), ("score", .int32(200))]))
    #expect(assembled[2] == .struct([("name", .string("Charlie")), ("score", .int32(300))]))
}
