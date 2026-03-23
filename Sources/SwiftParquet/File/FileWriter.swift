// FileWriter.swift — Write a complete Parquet file
// Port of github.com/apache/arrow-go/parquet/file/file_writer.go
//
// Parquet file layout:
//   [4 bytes] Magic: "PAR1"
//   [N bytes] Row groups (column chunks → pages)
//   [M bytes] FileMetaData (Thrift compact)
//   [4 bytes] Footer length (M as little-endian i32)
//   [4 bytes] Magic: "PAR1"

import Foundation

private let parquetMagic = Data([0x50, 0x41, 0x52, 0x31])  // "PAR1"

// MARK: - FileWriter

/// Low-level Parquet file writer.
/// Buffers all data in memory. For large files, a streaming version would be needed.
struct FileWriter {
    private let schema: ParquetSchema
    private var rowGroupResults: [RowGroupWriteResult] = []
    private var fileBody = Data()

    init(schema: ParquetSchema) {
        self.schema = schema
        fileBody.append(contentsOf: parquetMagic)
    }

    /// Add a row group. Each call to addRowGroup writes one row group.
    mutating func addRowGroup(_ writer: inout RowGroupWriter) {
        let fileOffset = Int64(fileBody.count)
        let result = writer.close(fileOffset: fileOffset)
        fileBody.append(contentsOf: result.bytes)
        rowGroupResults.append(result)
    }

    /// Finalize and return the complete Parquet file as Data.
    mutating func finalize() -> Data {
        let rowGroups = rowGroupResults.map { $0.rowGroup }

        let schemaElements = schema.toSchemaElements()

        let fileMeta = FileMetaData(
            version: 2,  // Parquet format version 2
            schema: schemaElements,
            numRows: rowGroups.reduce(0) { $0 + $1.numRows },
            rowGroups: rowGroups,
            createdBy: "SwiftParquet 0.1"
        )

        let footerBytes = ThriftCompactWriter.serialize(fileMeta)

        var file = fileBody
        file.append(contentsOf: footerBytes)

        let footerLength = UInt32(footerBytes.count)
        withUnsafeBytes(of: footerLength.littleEndian) { file.append(contentsOf: $0) }

        file.append(contentsOf: parquetMagic)

        return file
    }

    /// Write the file to disk.
    mutating func write(to url: URL) throws {
        let data = finalize()
        try data.write(to: url)
    }

    /// Write the file to a path string.
    mutating func write(to path: String) throws {
        try write(to: URL(fileURLWithPath: path))
    }
}
