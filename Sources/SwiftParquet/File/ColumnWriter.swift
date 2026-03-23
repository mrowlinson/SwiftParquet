// ColumnWriter.swift — Accumulate and flush column values to pages
// Port of github.com/apache/arrow-go/parquet/file/column_writer.go
//
// Phase 1: single-page columns, plain encoding, no compression, no dictionary.

import Foundation

// MARK: - Column Chunk Result

/// Everything produced after closing a column: the encoded pages + metadata.
struct ColumnChunkResult {
    let pages: [EncodedDataPage]
    let columnMetaData: ColumnMetaData
    let startOffset: Int64  // byte offset where this column's data starts
}

// MARK: - TypeErasedColumnWriter

/// Type-erased protocol so RowGroupWriter can hold heterogeneous column writers.
protocol AnyColumnWriter {
    var descriptor: ColumnDescriptor { get }
    mutating func closeColumn(startOffset: Int64) -> ColumnChunkResult
}

// MARK: - ColumnWriter<T>

/// Typed column writer for a specific ParquetValue type.
struct ColumnWriter<T: ParquetValue>: AnyColumnWriter {
    let descriptor: ColumnDescriptor
    private var values: [T] = []
    private var defLevels: [Int32]?

    init(descriptor: ColumnDescriptor) {
        self.descriptor = descriptor
        if descriptor.maxDefinitionLevel > 0 {
            defLevels = []
        }
    }

    /// Write non-null values (required columns or all-non-null optional columns).
    mutating func write(values newValues: [T]) {
        values.append(contentsOf: newValues)
        if descriptor.maxDefinitionLevel > 0 {
            // All values present → def level = maxDefLevel
            defLevels?.append(contentsOf: Array(repeating: Int32(descriptor.maxDefinitionLevel), count: newValues.count))
        }
    }

    mutating func closeColumn(startOffset: Int64) -> ColumnChunkResult {
        // Plain-encode all values
        var valueBytes = Data()
        if T.physicalType == .boolean {
            // Bool is bit-packed
            var encoder = PlainBoolEncoder()
            for v in values {
                if let b = v as? Bool {
                    encoder.encode(b)
                }
            }
            encoder.finalize()
            valueBytes = encoder.buffer
        } else {
            for v in values {
                v.encodePlain(to: &valueBytes)
            }
        }

        let numValues = Int32(values.count)

        let page = PageWriter.encodeDataPage(
            valueBytes: valueBytes,
            numValues: numValues,
            defLevels: defLevels,
            repLevels: nil,  // Phase 1: no repeated columns
            maxDefLevel: descriptor.maxDefinitionLevel,
            maxRepLevel: 0,
            encoding: .plain
        )

        let dataPageOffset = startOffset
        var totalUncompressed: Int64 = Int64(page.uncompressedSize)
        var totalCompressed: Int64 = Int64(page.compressedSize)

        // Add in the page header size contribution
        let headerSize = Int64(page.bytes.count) - Int64(page.compressedSize)
        totalUncompressed += headerSize
        totalCompressed += headerSize

        let meta = ColumnMetaData(
            type: descriptor.physicalType,
            encodings: [.plain],
            pathInSchema: descriptor.path,
            codec: .uncompressed,
            numValues: Int64(numValues),
            totalUncompressedSize: Int64(page.bytes.count),
            totalCompressedSize: Int64(page.bytes.count),
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: nil
        )

        return ColumnChunkResult(
            pages: [page],
            columnMetaData: meta,
            startOffset: startOffset
        )
    }
}

// MARK: - ByteArray ColumnWriter (strings)

/// Convenience column writer for ByteArray (string) columns.
struct ByteArrayColumnWriter: AnyColumnWriter {
    let descriptor: ColumnDescriptor
    private var values: [ByteArray] = []
    private var defLevels: [Int32]?

    init(descriptor: ColumnDescriptor) {
        self.descriptor = descriptor
        if descriptor.maxDefinitionLevel > 0 {
            defLevels = []
        }
    }

    mutating func write(values newValues: [ByteArray]) {
        values.append(contentsOf: newValues)
        if descriptor.maxDefinitionLevel > 0 {
            defLevels?.append(contentsOf: Array(repeating: Int32(descriptor.maxDefinitionLevel), count: newValues.count))
        }
    }

    mutating func closeColumn(startOffset: Int64) -> ColumnChunkResult {
        var valueBytes = Data()
        for v in values {
            v.encodePlain(to: &valueBytes)
        }

        let numValues = Int32(values.count)

        let page = PageWriter.encodeDataPage(
            valueBytes: valueBytes,
            numValues: numValues,
            defLevels: defLevels,
            repLevels: nil,
            maxDefLevel: descriptor.maxDefinitionLevel,
            maxRepLevel: 0,
            encoding: .plain
        )

        let meta = ColumnMetaData(
            type: descriptor.physicalType,
            encodings: [.plain],
            pathInSchema: descriptor.path,
            codec: .uncompressed,
            numValues: Int64(numValues),
            totalUncompressedSize: Int64(page.bytes.count),
            totalCompressedSize: Int64(page.bytes.count),
            dataPageOffset: startOffset,
            dictionaryPageOffset: nil
        )

        return ColumnChunkResult(
            pages: [page],
            columnMetaData: meta,
            startOffset: startOffset
        )
    }
}
