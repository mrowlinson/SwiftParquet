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
private let parquetEncryptedMagic = Data([0x50, 0x41, 0x52, 0x45])  // "PARE"

/// Closure that encrypts footer bytes. Captures the footer key.
typealias FooterEncryptor = (Data) throws -> Data

// MARK: - FileWriter

struct FileWriter {
    private let schema: ParquetSchema
    private var rowGroupResults: [RowGroupWriteResult] = []
    private var fileBody = Data()
    var footerEncryptor: FooterEncryptor? = nil

    init(schema: ParquetSchema) {
        self.schema = schema
        fileBody.append(contentsOf: parquetMagic)
    }

    mutating func addRowGroup(_ writer: inout RowGroupWriter) {
        let fileOffset = Int64(fileBody.count)
        let result = writer.close(fileOffset: fileOffset)
        fileBody.append(contentsOf: result.bytes)
        rowGroupResults.append(result)
    }

    mutating func finalize() throws -> Data {
        let rowGroups = rowGroupResults.map { $0.rowGroup }
        let schemaElements = schema.toSchemaElements()

        let fileMeta = FileMetaData(
            version: 2,
            schema: schemaElements,
            numRows: rowGroups.reduce(0) { $0 + $1.numRows },
            rowGroups: rowGroups,
            createdBy: "SwiftParquet 0.4"
        )

        let footerBytes = ThriftCompactWriter.serialize(fileMeta)
        var file = fileBody

        if let encrypt = footerEncryptor {
            let encrypted = try encrypt(footerBytes)
            file.replaceSubrange(file.startIndex..<(file.startIndex + 4), with: parquetEncryptedMagic)
            file.append(contentsOf: encrypted)
            let footerLength = UInt32(encrypted.count)
            withUnsafeBytes(of: footerLength.littleEndian) { file.append(contentsOf: $0) }
            file.append(contentsOf: parquetEncryptedMagic)
        } else {
            file.append(contentsOf: footerBytes)
            let footerLength = UInt32(footerBytes.count)
            withUnsafeBytes(of: footerLength.littleEndian) { file.append(contentsOf: $0) }
            file.append(contentsOf: parquetMagic)
        }

        return file
    }

    mutating func write(to url: URL) throws {
        let data = try finalize()
        try data.write(to: url)
    }

    mutating func write(to path: String) throws {
        try write(to: URL(fileURLWithPath: path))
    }
}
