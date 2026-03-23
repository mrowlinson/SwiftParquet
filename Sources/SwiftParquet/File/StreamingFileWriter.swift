// StreamingFileWriter.swift — Write Parquet files using FileHandle for low memory usage
// Unlike FileWriter which buffers the entire file in memory, this writes row groups
// directly to disk as they complete. Only metadata is kept in memory for the footer.

import Foundation

private let parquetMagic = Data([0x50, 0x41, 0x52, 0x31])  // "PAR1"

/// Streams Parquet data to a file handle, flushing each row group to disk immediately.
/// Memory usage is proportional to one row group + metadata, not the entire file.
final class StreamingFileWriter {
    private let schema: ParquetSchema
    private let fileHandle: FileHandle
    private let filePath: String
    private var currentOffset: Int64 = 4  // after PAR1 magic
    private var rowGroupMetadata: [RowGroup] = []
    private var totalRows: Int64 = 0
    private var closed = false

    init(path: String, schema: ParquetSchema) throws {
        self.schema = schema
        self.filePath = path

        // Create the file and write magic bytes
        FileManager.default.createFile(atPath: path, contents: nil)
        guard let fh = FileHandle(forWritingAtPath: path) else {
            throw ParquetError.ioError("cannot open \(path) for writing")
        }
        self.fileHandle = fh
        fileHandle.write(parquetMagic)
    }

    deinit {
        if !closed {
            try? fileHandle.close()
        }
    }

    /// Write a row group. Data is flushed to disk immediately.
    func writeRowGroup(_ writer: inout RowGroupWriter) throws {
        guard !closed else {
            throw ParquetError.ioError("writer is already closed")
        }

        let result = writer.close(fileOffset: currentOffset)
        fileHandle.write(result.bytes)
        currentOffset += Int64(result.bytes.count)
        rowGroupMetadata.append(result.rowGroup)
        totalRows += result.rowGroup.numRows
    }

    /// Finalize: write footer and close.
    func close() throws {
        guard !closed else { return }
        closed = true

        let schemaElements = schema.toSchemaElements()
        let fileMeta = FileMetaData(
            version: 2,
            schema: schemaElements,
            numRows: totalRows,
            rowGroups: rowGroupMetadata,
            createdBy: "SwiftParquet 0.3 (streaming)"
        )

        let footerBytes = ThriftCompactWriter.serialize(fileMeta)
        fileHandle.write(footerBytes)

        let footerLength = UInt32(footerBytes.count)
        var footerLenBytes = Data(count: 4)
        withUnsafeBytes(of: footerLength.littleEndian) { footerLenBytes = Data($0) }
        fileHandle.write(footerLenBytes)
        fileHandle.write(parquetMagic)

        try fileHandle.close()
    }
}
