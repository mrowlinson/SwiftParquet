// PageReader.swift — Read and decode Parquet data pages
// Port of github.com/apache/arrow-go/parquet/file/page_reader.go
//
// Reads PageHeader (Thrift), decompresses page body, decodes levels + values.

import Foundation

// MARK: - Decoded Page

struct DecodedDataPage {
    let numValues: Int32
    let encoding: Encoding
    let defLevels: [Int32]?
    let repLevels: [Int32]?
    let valueData: Data
}

struct DecodedDictionaryPage {
    let numValues: Int32
    let encoding: Encoding
    let data: Data
}

/// Closure that decrypts page data. Parameters: (encryptedData, pageOrdinal) → plaintext.
typealias PageDecryptor = (Data, Int16) throws -> Data

// MARK: - Page Reader

struct PageReader {

    /// Read a PageHeader from data at the given offset.
    /// Returns the header and number of bytes consumed by the header.
    static func readPageHeader(from data: Data, at offset: Int) throws -> (header: PageHeader, headerSize: Int) {
        let slice = data[(data.startIndex + offset)...]
        var reader = ThriftCompactReader(data: Data(slice))
        let header = try PageHeader.read(from: &reader)
        return (header, reader.bytesRead)
    }

    /// Read and decode a data page.
    static func readDataPage(
        from data: Data,
        at offset: Int,
        header: PageHeader,
        headerSize: Int,
        codec: CompressionCodec,
        maxDefLevel: Int16,
        maxRepLevel: Int16,
        decryptor: PageDecryptor? = nil,
        pageOrdinal: Int16 = 0
    ) throws -> DecodedDataPage {
        guard let dpHeader = header.dataPageHeader else {
            throw ParquetError.invalidPageHeader("missing data page header")
        }

        let bodyStart = data.startIndex + offset + headerSize
        let bodyEnd = bodyStart + Int(header.compressedPageSize)
        guard bodyEnd <= data.endIndex else { throw ParquetError.unexpectedEOF }
        var pageBody = Data(data[bodyStart..<bodyEnd])

        // Decrypt before decompression (Parquet spec: encrypt after compress)
        if let decrypt = decryptor {
            pageBody = try decrypt(pageBody, pageOrdinal)
        }

        // Decompress if needed
        if codec != .uncompressed {
            let decompressor = try CompressionCodecs.codec(for: codec)
            pageBody = try decompressor.decompress(pageBody, uncompressedSize: Int(header.uncompressedPageSize))
        }

        var bodyOffset = 0

        // Read repetition levels
        var repLevels: [Int32]? = nil
        if maxRepLevel > 0 {
            let bw = bitWidthForMaxLevel(maxRepLevel)
            let decoder = RLEDecoder(bitWidth: bw)
            let (values, consumed) = decoder.decodeWithLengthPrefix(pageBody, at: bodyOffset, expectedCount: Int(dpHeader.numValues))
            repLevels = values
            bodyOffset += consumed
        }

        // Read definition levels
        var defLevels: [Int32]? = nil
        if maxDefLevel > 0 {
            let bw = bitWidthForMaxLevel(maxDefLevel)
            let decoder = RLEDecoder(bitWidth: bw)
            let (values, consumed) = decoder.decodeWithLengthPrefix(pageBody, at: bodyOffset, expectedCount: Int(dpHeader.numValues))
            defLevels = values
            bodyOffset += consumed
        }

        // Remaining bytes are the encoded values
        let valueData = Data(pageBody[(pageBody.startIndex + bodyOffset)...])

        return DecodedDataPage(
            numValues: dpHeader.numValues,
            encoding: dpHeader.encoding,
            defLevels: defLevels,
            repLevels: repLevels,
            valueData: valueData
        )
    }

    /// Read and decode a data page V2.
    /// V2 layout: [rep levels uncompressed][def levels uncompressed][compressed values]
    /// Level byte lengths are in the header, not 4-byte prefixed in the body.
    static func readDataPageV2(
        from data: Data,
        at offset: Int,
        header: PageHeader,
        headerSize: Int,
        codec: CompressionCodec,
        maxDefLevel: Int16,
        maxRepLevel: Int16,
        decryptor: PageDecryptor? = nil,
        pageOrdinal: Int16 = 0
    ) throws -> DecodedDataPage {
        guard let v2Header = header.dataPageHeaderV2 else {
            throw ParquetError.invalidPageHeader("missing data page header V2")
        }

        let bodyStart = data.startIndex + offset + headerSize
        let bodyEnd = bodyStart + Int(header.compressedPageSize)
        guard bodyEnd <= data.endIndex else { throw ParquetError.unexpectedEOF }

        var bodyOffset = bodyStart
        let repLevelsByteLen = Int(v2Header.repetitionLevelsByteLength)
        let defLevelsByteLen = Int(v2Header.definitionLevelsByteLength)

        // Rep levels (uncompressed, no 4-byte prefix)
        var repLevels: [Int32]? = nil
        if repLevelsByteLen > 0 && maxRepLevel > 0 {
            let repData = Data(data[bodyOffset..<(bodyOffset + repLevelsByteLen)])
            let bw = bitWidthForMaxLevel(maxRepLevel)
            repLevels = RLEDecoder(bitWidth: bw).decode(repData, expectedCount: Int(v2Header.numValues))
        }
        bodyOffset += repLevelsByteLen

        // Def levels (uncompressed, no 4-byte prefix)
        var defLevels: [Int32]? = nil
        if defLevelsByteLen > 0 && maxDefLevel > 0 {
            let defData = Data(data[bodyOffset..<(bodyOffset + defLevelsByteLen)])
            let bw = bitWidthForMaxLevel(maxDefLevel)
            defLevels = RLEDecoder(bitWidth: bw).decode(defData, expectedCount: Int(v2Header.numValues))
        }
        bodyOffset += defLevelsByteLen

        // Values (compressed if isCompressed is true)
        var valueData = Data(data[bodyOffset..<bodyEnd])
        if v2Header.isCompressed && codec != .uncompressed {
            let decompressor = try CompressionCodecs.codec(for: codec)
            let uncompressedValueSize = Int(header.uncompressedPageSize) - repLevelsByteLen - defLevelsByteLen
            valueData = try decompressor.decompress(valueData, uncompressedSize: uncompressedValueSize)
        }

        return DecodedDataPage(
            numValues: v2Header.numValues,
            encoding: v2Header.encoding,
            defLevels: defLevels,
            repLevels: repLevels,
            valueData: valueData
        )
    }

    /// Read and decode a dictionary page.
    static func readDictionaryPage(
        from data: Data,
        at offset: Int,
        header: PageHeader,
        headerSize: Int,
        codec: CompressionCodec,
        decryptor: PageDecryptor? = nil
    ) throws -> DecodedDictionaryPage {
        guard let dictHeader = header.dictionaryPageHeader else {
            throw ParquetError.invalidPageHeader("missing dictionary page header")
        }

        let bodyStart = data.startIndex + offset + headerSize
        let bodyEnd = bodyStart + Int(header.compressedPageSize)
        guard bodyEnd <= data.endIndex else { throw ParquetError.unexpectedEOF }
        var pageBody = Data(data[bodyStart..<bodyEnd])

        if let decrypt = decryptor {
            pageBody = try decrypt(pageBody, -1)
        }

        if codec != .uncompressed {
            let decompressor = try CompressionCodecs.codec(for: codec)
            pageBody = try decompressor.decompress(pageBody, uncompressedSize: Int(header.uncompressedPageSize))
        }

        return DecodedDictionaryPage(
            numValues: dictHeader.numValues,
            encoding: dictHeader.encoding,
            data: pageBody
        )
    }
}
