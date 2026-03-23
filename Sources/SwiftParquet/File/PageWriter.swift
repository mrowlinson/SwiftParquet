// PageWriter.swift — Serialize a data page to bytes
// Port of github.com/apache/arrow-go/parquet/file/page_writer.go
//
// Data Page V1 layout:
//   [Thrift PageHeader]
//   [Repetition levels RLE (4-byte length prefix)] — only if maxRepLevel > 0
//   [Definition levels RLE (4-byte length prefix)] — only if maxDefLevel > 0
//   [Plain-encoded values]

import Foundation

// MARK: - Encoded Page

/// The result of encoding a data page: serialized bytes + metadata for ColumnMetaData.
struct EncodedDataPage {
    let bytes: Data
    let numValues: Int32
    let uncompressedSize: Int32
    let compressedSize: Int32  // same as uncompressed for Phase 1 (no compression)
}

// MARK: - Page Writer

struct PageWriter {

    /// Encode a data page V1 from pre-encoded values.
    ///
    /// - Parameters:
    ///   - valueBytes:    Plain-encoded value bytes.
    ///   - numValues:     Number of values (not rows — for non-repeated, these are equal).
    ///   - defLevels:     Definition levels (nil if maxDefLevel == 0).
    ///   - repLevels:     Repetition levels (nil if maxRepLevel == 0).
    ///   - maxDefLevel:   Max definition level for the column.
    ///   - maxRepLevel:   Max repetition level for the column.
    ///   - encoding:      Value encoding (typically .plain for Phase 1).
    static func encodeDataPage(
        valueBytes: Data,
        numValues: Int32,
        defLevels: [Int32]?,
        repLevels: [Int32]?,
        maxDefLevel: Int16,
        maxRepLevel: Int16,
        encoding: Encoding = .plain
    ) -> EncodedDataPage {

        var pageBody = Data()

        // 1. Repetition levels (only if maxRepLevel > 0)
        if maxRepLevel > 0, let rep = repLevels {
            let bitWidth = bitWidthForMaxLevel(maxRepLevel)
            let encoder = RLEEncoder(bitWidth: bitWidth)
            pageBody.append(contentsOf: encoder.encodeWithLengthPrefix(rep))
        }

        // 2. Definition levels (only if maxDefLevel > 0)
        if maxDefLevel > 0, let def = defLevels {
            let bitWidth = bitWidthForMaxLevel(maxDefLevel)
            let encoder = RLEEncoder(bitWidth: bitWidth)
            pageBody.append(contentsOf: encoder.encodeWithLengthPrefix(def))
        }

        // 3. Plain-encoded values
        pageBody.append(contentsOf: valueBytes)

        let pageSize = Int32(pageBody.count)

        // 4. Thrift PageHeader
        let header = PageHeader(
            type: .dataPage,
            uncompressedPageSize: pageSize,
            compressedPageSize: pageSize,
            dataPageHeader: DataPageHeader(
                numValues: numValues,
                encoding: encoding,
                definitionLevelEncoding: .rle,
                repetitionLevelEncoding: .rle
            )
        )
        let headerBytes = ThriftCompactWriter.serialize(header)

        // Assemble: header + body
        var result = Data(capacity: headerBytes.count + pageBody.count)
        result.append(contentsOf: headerBytes)
        result.append(contentsOf: pageBody)

        return EncodedDataPage(
            bytes: result,
            numValues: numValues,
            uncompressedSize: pageSize,
            compressedSize: pageSize
        )
    }
}
