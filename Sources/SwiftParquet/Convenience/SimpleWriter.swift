// SimpleWriter.swift — High-level API for writing Parquet files
// Hides the schema/column/page/row-group details behind a simple typed API.

import Foundation

// MARK: - Column Value Box

/// Type-erased column values. Used by ParquetFileWriter.writeRowGroup.
public enum ColumnValues {
    case strings([String])
    case int32s([Int32])
    case int64s([Int64])
    case floats([Float])
    case doubles([Double])
    case booleans([Bool])
    case byteArrays([ByteArray])

    var count: Int {
        switch self {
        case .strings(let v):    return v.count
        case .int32s(let v):     return v.count
        case .int64s(let v):     return v.count
        case .floats(let v):     return v.count
        case .doubles(let v):    return v.count
        case .booleans(let v):   return v.count
        case .byteArrays(let v): return v.count
        }
    }
}

// MARK: - Schema Builder

/// Build a flat (non-nested) Parquet schema from column name+type pairs.
public struct SchemaBuilder {
    private var fields: [any SchemaNode] = []

    public init() {}

    @discardableResult
    public mutating func addColumn(name: String, type: PhysicalType, repetition: Repetition = .required) -> SchemaBuilder {
        let convertedType: ConvertedType? = (type == .byteArray) ? .utf8 : nil
        let logicalType: LogicalTypeThrift? = (type == .byteArray) ? LogicalTypeThrift(kind: .string) : nil
        let node = PrimitiveNode(
            name: name,
            repetition: repetition,
            physicalType: type,
            convertedType: convertedType,
            logicalType: logicalType
        )
        fields.append(node)
        return self
    }

    public func build() -> ParquetSchema {
        ParquetSchema(fields: fields)
    }
}

// MARK: - ParquetFileWriter

/// High-level Parquet file writer.
///
/// Example:
/// ```swift
/// var writer = try ParquetFileWriter(path: "output.parquet")
/// writer.schema.addColumn(name: "name", type: .byteArray)
/// writer.schema.addColumn(name: "age", type: .int32)
/// try writer.writeRowGroup(columns: [
///     ("name", .strings(["Alice", "Bob"])),
///     ("age",  .int32s([30, 25])),
/// ])
/// try writer.close()
/// ```
public struct ParquetFileWriter {
    private let path: String
    private let schema: ParquetSchema
    private var fileWriter: FileWriter

    public init(path: String, schema: ParquetSchema) {
        self.path = path
        self.schema = schema
        self.fileWriter = FileWriter(schema: schema)
    }

    /// Write a row group. Columns are matched by position to the schema.
    ///
    /// - Parameter columns: Array of (name, values) pairs. Count must equal schema column count.
    ///   Each column's value count must match across all columns.
    public mutating func writeRowGroup(columns: [(String, ColumnValues)]) throws {
        guard columns.count == schema.numColumns else {
            throw ParquetError.invalidSchema(
                "Expected \(schema.numColumns) columns, got \(columns.count)"
            )
        }

        // Validate all columns have the same row count
        let counts = columns.map { $0.1.count }
        guard let numRows = counts.first else { return }
        guard counts.allSatisfy({ $0 == numRows }) else {
            throw ParquetError.invalidSchema("All columns must have the same number of rows")
        }

        let schemaCols = schema.columns
        var colWriters: [AnyColumnWriter] = []

        for (i, (_, values)) in columns.enumerated() {
            let desc = ColumnDescriptor(node: schemaCols[i])
            switch values {
            case .strings(let vs):
                var w = ByteArrayColumnWriter(descriptor: desc)
                w.write(values: vs.map { ByteArray($0) })
                colWriters.append(w)
            case .int32s(let vs):
                var w = ColumnWriter<Int32>(descriptor: desc)
                w.write(values: vs)
                colWriters.append(w)
            case .int64s(let vs):
                var w = ColumnWriter<Int64>(descriptor: desc)
                w.write(values: vs)
                colWriters.append(w)
            case .floats(let vs):
                var w = ColumnWriter<Float>(descriptor: desc)
                w.write(values: vs)
                colWriters.append(w)
            case .doubles(let vs):
                var w = ColumnWriter<Double>(descriptor: desc)
                w.write(values: vs)
                colWriters.append(w)
            case .booleans(let vs):
                var w = ColumnWriter<Bool>(descriptor: desc)
                w.write(values: vs)
                colWriters.append(w)
            case .byteArrays(let vs):
                var w = ByteArrayColumnWriter(descriptor: desc)
                w.write(values: vs)
                colWriters.append(w)
            }
        }

        var rowGroupWriter = RowGroupWriter(
            schema: schema,
            numRows: Int64(numRows),
            columnWriters: colWriters
        )
        fileWriter.addRowGroup(&rowGroupWriter)
    }

    /// Finalize and write the file to disk.
    public mutating func close() throws {
        try fileWriter.write(to: path)
    }
}
