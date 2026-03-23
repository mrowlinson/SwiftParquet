// ColumnReader.swift — Read all pages for a column chunk and produce typed values
// Port of github.com/apache/arrow-go/parquet/file/column_reader.go

import Foundation

// MARK: - Column Reader

struct ColumnChunkReader {
    let columnMeta: ColumnMetaData
    let fileData: Data
    let maxDefLevel: Int16
    let maxRepLevel: Int16
    var decryptor: PageDecryptor? = nil

    /// Read all values from this column chunk as ColumnValues.
    func readAll() throws -> ColumnValues {
        var offset = Int(columnMeta.dataPageOffset)
        var dictionary: DecodedDictionaryPage? = nil

        // Read dictionary page first if present
        if let dictOffset = columnMeta.dictionaryPageOffset {
            offset = Int(dictOffset)
            let (header, headerSize) = try PageReader.readPageHeader(from: fileData, at: offset)
            if header.type == .dictionaryPage {
                dictionary = try PageReader.readDictionaryPage(
                    from: fileData, at: offset, header: header,
                    headerSize: headerSize, codec: columnMeta.codec,
                    decryptor: decryptor
                )
                offset += headerSize + Int(header.compressedPageSize)
            }
        }

        // If dictionary offset < data page offset, we need to start data pages from dataPageOffset
        if columnMeta.dictionaryPageOffset != nil {
            offset = Int(columnMeta.dataPageOffset)
        }

        // Read data pages — pre-allocate based on expected size
        let totalExpected = columnMeta.numValues
        var allValueData = Data()
        let estimatedSize: Int
        switch columnMeta.type {
        case .int32, .float: estimatedSize = Int(totalExpected) * 4
        case .int64, .double: estimatedSize = Int(totalExpected) * 8
        case .int96: estimatedSize = Int(totalExpected) * 12
        case .boolean: estimatedSize = (Int(totalExpected) + 7) / 8
        case .byteArray, .fixedLenByteArray: estimatedSize = 0
        }
        if estimatedSize > 0 { allValueData.reserveCapacity(estimatedSize) }
        var totalValues: Int32 = 0
        var encoding: Encoding = .plain
        var allDefLevels: [Int32]? = maxDefLevel > 0 ? [] : nil
        var allRepLevels: [Int32]? = maxRepLevel > 0 ? [] : nil
        var dataPageCount = 0
        while totalValues < totalExpected {
            guard offset < fileData.count else { break }

            let (header, headerSize) = try PageReader.readPageHeader(from: fileData, at: offset)

            guard header.type == .dataPage || header.type == .dataPageV2 else {
                // Skip non-data pages
                offset += headerSize + Int(header.compressedPageSize)
                continue
            }

            let page: DecodedDataPage
            let pageOrd = Int16(dataPageCount)
            if header.type == .dataPageV2 {
                page = try PageReader.readDataPageV2(
                    from: fileData, at: offset, header: header,
                    headerSize: headerSize, codec: columnMeta.codec,
                    maxDefLevel: maxDefLevel, maxRepLevel: maxRepLevel,
                    decryptor: decryptor, pageOrdinal: pageOrd
                )
            } else {
                page = try PageReader.readDataPage(
                    from: fileData, at: offset, header: header,
                    headerSize: headerSize, codec: columnMeta.codec,
                    maxDefLevel: maxDefLevel, maxRepLevel: maxRepLevel,
                    decryptor: decryptor, pageOrdinal: pageOrd
                )
            }
            dataPageCount += 1

            encoding = page.encoding
            allValueData.append(page.valueData)
            totalValues += page.numValues
            if let dl = page.defLevels { allDefLevels?.append(contentsOf: dl) }
            if let rl = page.repLevels { allRepLevels?.append(contentsOf: rl) }

            offset += headerSize + Int(header.compressedPageSize)
        }

        // Determine how many non-null values we have (counting loop, no allocation)
        let numNonNull: Int
        if let defLevels = allDefLevels {
            let target = Int32(maxDefLevel)
            var count = 0
            for level in defLevels { if level == target { count += 1 } }
            numNonNull = count
        } else {
            numNonNull = Int(totalValues)
        }

        // Decode values based on encoding and type
        let isDictEncoding = encoding == .rleDict || encoding == .plainDictionary
        if isDictEncoding, let dict = dictionary {
            return try decodeDictionaryColumn(dict: dict, indexData: allValueData,
                                               numValues: numNonNull)
        } else {
            return decodeColumn(valueData: allValueData, numValues: numNonNull)
        }
    }

    private func decodeDictionaryColumn(dict: DecodedDictionaryPage, indexData: Data, numValues: Int) throws -> ColumnValues {
        let decoder = DictionaryDecoder(
            dictionaryData: dict.data,
            physicalType: columnMeta.type,
            typeLength: nil
        )
        switch columnMeta.type {
        case .byteArray:
            let values = try decoder.decodeByteArrays(indexData: indexData, numValues: numValues)
            return .strings(values.map { String(decoding: $0.data, as: UTF8.self) })
        case .int32:
            return .int32s(try decoder.decodeInt32s(indexData: indexData, numValues: numValues))
        case .int64:
            return .int64s(try decoder.decodeInt64s(indexData: indexData, numValues: numValues))
        case .float:
            return .floats(try decoder.decodeFloats(indexData: indexData, numValues: numValues))
        case .double:
            return .doubles(try decoder.decodeDoubles(indexData: indexData, numValues: numValues))
        default:
            throw ParquetError.unsupportedEncoding(.rleDict)
        }
    }

    private func decodeColumn(valueData: Data, numValues: Int) -> ColumnValues {
        switch columnMeta.type {
        case .boolean:
            return .booleans(PlainDecoder.decodeBooleans(valueData, count: numValues))
        case .int32:
            return .int32s(PlainDecoder.decodeInt32s(valueData, count: numValues))
        case .int64:
            return .int64s(PlainDecoder.decodeInt64s(valueData, count: numValues))
        case .float:
            return .floats(PlainDecoder.decodeFloats(valueData, count: numValues))
        case .double:
            return .doubles(PlainDecoder.decodeDoubles(valueData, count: numValues))
        case .byteArray:
            let values = PlainDecoder.decodeByteArrays(valueData, count: numValues)
            return .strings(values.map { String(decoding: $0.data, as: UTF8.self) })
        case .fixedLenByteArray:
            let values = PlainDecoder.decodeByteArrays(valueData, count: numValues)
            return .byteArrays(values)
        case .int96:
            return .byteArrays(PlainDecoder.decodeByteArrays(valueData, count: numValues))
        }
    }
}
