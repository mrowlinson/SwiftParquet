// RowGroupWriter.swift — Orchestrate writing all columns in a row group
// Port of github.com/apache/arrow-go/parquet/file/row_group_writer.go

import Foundation

// MARK: - RowGroupWriteResult

struct RowGroupWriteResult {
    let bytes: Data
    let rowGroup: RowGroup
    let startOffset: Int64
}

// MARK: - RowGroupWriter

struct RowGroupWriter {
    let schema: ParquetSchema
    private var columnWriters: [AnyColumnWriter]
    let numRows: Int64

    init(schema: ParquetSchema, numRows: Int64, columnWriters: [AnyColumnWriter]) {
        self.schema = schema
        self.numRows = numRows
        self.columnWriters = columnWriters
    }

    /// Close all columns and assemble the row group bytes + metadata.
    /// - Parameter fileOffset: Byte offset in the file where this row group starts.
    mutating func close(fileOffset: Int64) -> RowGroupWriteResult {
        var groupBytes = Data()
        var columnChunks: [ColumnChunk] = []
        var totalByteSize: Int64 = 0

        for i in 0..<columnWriters.count {
            let colStartOffset = fileOffset + Int64(groupBytes.count)
            let result = columnWriters[i].closeColumn(startOffset: colStartOffset)

            for page in result.pages {
                groupBytes.append(contentsOf: page.bytes)
            }

            let chunk = ColumnChunk(
                fileOffset: colStartOffset,
                metaData: result.columnMetaData
            )
            columnChunks.append(chunk)
            totalByteSize += result.columnMetaData.totalCompressedSize
        }

        let rowGroup = RowGroup(
            columns: columnChunks,
            totalByteSize: totalByteSize,
            numRows: numRows
        )

        return RowGroupWriteResult(
            bytes: groupBytes,
            rowGroup: rowGroup,
            startOffset: fileOffset
        )
    }
}
