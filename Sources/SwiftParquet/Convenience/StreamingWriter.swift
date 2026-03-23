// StreamingWriter.swift — High-level streaming Parquet writer
// Writes row groups directly to disk for low memory usage with large files.

import Foundation

/// High-level streaming Parquet writer.
/// Each call to `writeBatch` writes one row group directly to disk.
///
/// Example:
/// ```swift
/// var writer = try ParquetStreamWriter(path: "large.parquet", schema: schema, options: .snappy)
/// for batch in dataBatches {
///     try writer.writeBatch(columns: batch)
/// }
/// try writer.close()
/// ```
public struct ParquetStreamWriter {
    private let schema: ParquetSchema
    private let writeOptions: ParquetWriteOptions
    private var streamWriter: StreamingFileWriter

    public init(path: String, schema: ParquetSchema, options: ParquetWriteOptions = ParquetWriteOptions()) throws {
        self.schema = schema
        self.writeOptions = options
        self.streamWriter = try StreamingFileWriter(path: path, schema: schema)
    }

    /// Write a batch of columns as a single row group. Flushed to disk immediately.
    public mutating func writeBatch(columns: [(String, ColumnValues)]) throws {
        guard columns.count == schema.numColumns else {
            throw ParquetError.invalidSchema(
                "Expected \(schema.numColumns) columns, got \(columns.count)")
        }

        let counts = columns.map { $0.1.count }
        guard let numRows = counts.first else { return }
        guard counts.allSatisfy({ $0 == numRows }) else {
            throw ParquetError.invalidSchema("All columns must have the same number of rows")
        }

        let schemaCols = schema.columns
        let colOpts = ColumnWriteOptions(
            compression: writeOptions.compression,
            useDictionary: writeOptions.useDictionary,
            enableStatistics: writeOptions.enableStatistics,
            dataPageVersion: writeOptions.dataPageVersion
        )

        var colWriters: [AnyColumnWriter] = []
        for (i, (_, values)) in columns.enumerated() {
            let desc = ColumnDescriptor(node: schemaCols[i])
            switch values {
            case .strings(let vs):
                var w = ByteArrayColumnWriter(descriptor: desc, options: colOpts)
                w.write(values: vs.map { ByteArray($0) })
                colWriters.append(w)
            case .int32s(let vs):
                var w = ColumnWriter<Int32>(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            case .int64s(let vs):
                var w = ColumnWriter<Int64>(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            case .floats(let vs):
                var w = ColumnWriter<Float>(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            case .doubles(let vs):
                var w = ColumnWriter<Double>(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            case .booleans(let vs):
                var w = ColumnWriter<Bool>(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            case .byteArrays(let vs):
                var w = ByteArrayColumnWriter(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            }
        }

        var rowGroupWriter = RowGroupWriter(
            schema: schema, numRows: Int64(numRows), columnWriters: colWriters
        )
        try streamWriter.writeRowGroup(&rowGroupWriter)
    }

    /// Finalize and close the file.
    public mutating func close() throws {
        try streamWriter.close()
    }
}
