// ColumnWriter.swift — Accumulate and flush column values to pages
// Port of github.com/apache/arrow-go/parquet/file/column_writer.go
//
// Supports: plain encoding, dictionary encoding, compression, statistics.

import Foundation

// MARK: - Write Options

/// Closure that encrypts page data. Parameters: (pageData, pageOrdinal) → encryptedData.
/// Captures the key and column/row-group ordinals from the encryption config.
typealias PageEncryptor = (Data, Int16) throws -> Data

struct ColumnWriteOptions {
    var compression: CompressionCodec = .uncompressed
    var useDictionary: Bool = false
    var enableStatistics: Bool = true
    var dataPageVersion: DataPageVersion = .v1
    var pageEncryptor: PageEncryptor? = nil
}

// MARK: - Column Chunk Result

struct ColumnChunkResult {
    let pages: [Data]   // raw page bytes (header + body)
    let columnMetaData: ColumnMetaData
    let startOffset: Int64
}

// MARK: - TypeErasedColumnWriter

protocol AnyColumnWriter {
    var descriptor: ColumnDescriptor { get }
    mutating func closeColumn(startOffset: Int64) -> ColumnChunkResult
}

// MARK: - ColumnWriter<T>

struct ColumnWriter<T: ParquetValue>: AnyColumnWriter {
    let descriptor: ColumnDescriptor
    let options: ColumnWriteOptions
    private var values: [T] = []
    private var defLevels: [Int32]?
    private var repLevels: [Int32]?
    private var totalSlots: Int = 0

    init(descriptor: ColumnDescriptor, options: ColumnWriteOptions = ColumnWriteOptions()) {
        self.descriptor = descriptor
        self.options = options
        if descriptor.maxDefinitionLevel > 0 { defLevels = [] }
    }

    mutating func write(values newValues: [T]) {
        values.append(contentsOf: newValues)
        if descriptor.maxDefinitionLevel > 0 {
            defLevels?.append(contentsOf: Array(repeating: Int32(descriptor.maxDefinitionLevel), count: newValues.count))
        }
    }

    /// Write values with pre-computed definition and repetition levels (for nested types).
    mutating func writeWithLevels(values newValues: [T], defLevels newDefLevels: [Int32], repLevels newRepLevels: [Int32]) {
        values.append(contentsOf: newValues)
        if self.defLevels == nil { self.defLevels = [] }
        self.defLevels?.append(contentsOf: newDefLevels)
        if self.repLevels == nil { self.repLevels = [] }
        self.repLevels?.append(contentsOf: newRepLevels)
        totalSlots += newDefLevels.count
    }

    mutating func closeColumn(startOffset: Int64) -> ColumnChunkResult {
        var valueBytes = Data()
        if T.physicalType == .boolean {
            var encoder = PlainBoolEncoder()
            for v in values {
                if let b = v as? Bool { encoder.encode(b) }
            }
            encoder.finalize()
            valueBytes = encoder.buffer
        } else {
            for v in values { v.encodePlain(to: &valueBytes) }
        }

        // numValues in page header = total slots (including nulls) for nested, or value count for flat
        let numValues = totalSlots > 0 ? Int32(totalSlots) : Int32(values.count)
        let encoding: Encoding = .plain

        var statistics: Statistics? = nil
        if options.enableStatistics && !values.isEmpty {
            statistics = buildStatistics()
        }

        var pageResult = buildPage(valueBytes: valueBytes, numValues: numValues,
                                    encoding: encoding, compression: options.compression)

        if let encrypt = options.pageEncryptor,
           let encrypted = try? encrypt(pageResult.bytes, 0) {
            pageResult = PageBuildResult(
                bytes: encrypted,
                uncompressedTotal: pageResult.uncompressedTotal,
                compressedTotal: encrypted.count
            )
        }

        let meta = ColumnMetaData(
            type: descriptor.physicalType,
            encodings: [encoding],
            pathInSchema: descriptor.path,
            codec: options.compression,
            numValues: Int64(numValues),
            totalUncompressedSize: Int64(pageResult.uncompressedTotal),
            totalCompressedSize: Int64(pageResult.compressedTotal),
            dataPageOffset: startOffset,
            dictionaryPageOffset: nil,
            statistics: statistics
        )

        return ColumnChunkResult(pages: [pageResult.bytes], columnMetaData: meta, startOffset: startOffset)
    }

    private func buildStatistics() -> Statistics? {
        guard !values.isEmpty else { return nil }
        var minData = Data()
        var maxData = Data()

        // For comparable types, compute min/max
        if let comparable = values as? [Int32] {
            comparable.min()?.encodePlain(to: &minData)
            comparable.max()?.encodePlain(to: &maxData)
        } else if let comparable = values as? [Int64] {
            comparable.min()?.encodePlain(to: &minData)
            comparable.max()?.encodePlain(to: &maxData)
        } else if let comparable = values as? [Float] {
            comparable.min()?.encodePlain(to: &minData)
            comparable.max()?.encodePlain(to: &maxData)
        } else if let comparable = values as? [Double] {
            comparable.min()?.encodePlain(to: &minData)
            comparable.max()?.encodePlain(to: &maxData)
        } else {
            return nil // No statistics for non-comparable types
        }

        return Statistics(max: nil, min: nil, nullCount: 0, distinctCount: nil,
                         maxValue: maxData, minValue: minData)
    }

    private func buildPage(valueBytes: Data, numValues: Int32, encoding: Encoding, compression: CompressionCodec) -> PageBuildResult {
        if options.dataPageVersion == .v2 {
            return buildPageV2(valueBytes: valueBytes, numValues: numValues, encoding: encoding, compression: compression)
        }
        return buildPageV1(valueBytes: valueBytes, numValues: numValues, encoding: encoding, compression: compression)
    }

    private func buildPageV1(valueBytes: Data, numValues: Int32, encoding: Encoding, compression: CompressionCodec) -> PageBuildResult {
        var pageBody = Data()

        if descriptor.maxRepetitionLevel > 0 {
            let bw = bitWidthForMaxLevel(descriptor.maxRepetitionLevel)
            let encoder = RLEEncoder(bitWidth: bw)
            let rep = repLevels ?? [Int32](repeating: 0, count: Int(numValues))
            pageBody.append(contentsOf: encoder.encodeWithLengthPrefix(rep))
        }

        if descriptor.maxDefinitionLevel > 0, let def = defLevels {
            let bw = bitWidthForMaxLevel(descriptor.maxDefinitionLevel)
            let encoder = RLEEncoder(bitWidth: bw)
            pageBody.append(contentsOf: encoder.encodeWithLengthPrefix(def))
        }

        pageBody.append(contentsOf: valueBytes)

        let uncompressedSize = Int32(pageBody.count)
        var compressedBody = pageBody

        if compression != .uncompressed {
            if let codec = try? CompressionCodecs.codec(for: compression),
               let compressed = try? codec.compress(pageBody) {
                compressedBody = compressed
            }
        }

        let header = PageHeader(
            type: .dataPage,
            uncompressedPageSize: uncompressedSize,
            compressedPageSize: Int32(compressedBody.count),
            dataPageHeader: DataPageHeader(
                numValues: numValues, encoding: encoding,
                definitionLevelEncoding: .rle, repetitionLevelEncoding: .rle
            )
        )
        let headerBytes = ThriftCompactWriter.serialize(header)

        var result = Data(capacity: headerBytes.count + compressedBody.count)
        result.append(contentsOf: headerBytes)
        result.append(contentsOf: compressedBody)

        return PageBuildResult(bytes: result,
                              uncompressedTotal: headerBytes.count + Int(uncompressedSize),
                              compressedTotal: result.count)
    }

    private func buildPageV2(valueBytes: Data, numValues: Int32, encoding: Encoding, compression: CompressionCodec) -> PageBuildResult {
        var repLevelBytes = Data()
        if descriptor.maxRepetitionLevel > 0 {
            let bw = bitWidthForMaxLevel(descriptor.maxRepetitionLevel)
            let encoder = RLEEncoder(bitWidth: bw)
            let rep = repLevels ?? [Int32](repeating: 0, count: Int(numValues))
            repLevelBytes = encoder.encode(rep)
        }

        var defLevelBytes = Data()
        if descriptor.maxDefinitionLevel > 0, let def = defLevels {
            let bw = bitWidthForMaxLevel(descriptor.maxDefinitionLevel)
            let encoder = RLEEncoder(bitWidth: bw)
            defLevelBytes = encoder.encode(def)
        }

        var compressedValues = valueBytes
        if compression != .uncompressed {
            if let codec = try? CompressionCodecs.codec(for: compression),
               let compressed = try? codec.compress(valueBytes) {
                compressedValues = compressed
            }
        }

        let uncompressedSize = Int32(repLevelBytes.count + defLevelBytes.count + valueBytes.count)
        let compressedSize = Int32(repLevelBytes.count + defLevelBytes.count + compressedValues.count)

        let header = PageHeader(
            type: .dataPageV2,
            uncompressedPageSize: uncompressedSize,
            compressedPageSize: compressedSize,
            dataPageHeaderV2: DataPageHeaderV2(
                numValues: numValues, numNulls: 0, numRows: numValues,
                encoding: encoding,
                definitionLevelsByteLength: Int32(defLevelBytes.count),
                repetitionLevelsByteLength: Int32(repLevelBytes.count),
                isCompressed: compression != .uncompressed
            )
        )
        let headerBytes = ThriftCompactWriter.serialize(header)

        var result = Data(capacity: headerBytes.count + Int(compressedSize))
        result.append(contentsOf: headerBytes)
        result.append(contentsOf: repLevelBytes)
        result.append(contentsOf: defLevelBytes)
        result.append(contentsOf: compressedValues)

        return PageBuildResult(bytes: result,
                              uncompressedTotal: headerBytes.count + Int(uncompressedSize),
                              compressedTotal: result.count)
    }
}

// MARK: - ByteArray ColumnWriter (strings)

struct ByteArrayColumnWriter: AnyColumnWriter {
    let descriptor: ColumnDescriptor
    let options: ColumnWriteOptions
    private var values: [ByteArray] = []
    private var defLevels: [Int32]?
    private var repLevels: [Int32]?
    private var totalSlots: Int = 0

    init(descriptor: ColumnDescriptor, options: ColumnWriteOptions = ColumnWriteOptions()) {
        self.descriptor = descriptor
        self.options = options
        if descriptor.maxDefinitionLevel > 0 { defLevels = [] }
    }

    mutating func write(values newValues: [ByteArray]) {
        values.append(contentsOf: newValues)
        if descriptor.maxDefinitionLevel > 0 {
            defLevels?.append(contentsOf: Array(repeating: Int32(descriptor.maxDefinitionLevel), count: newValues.count))
        }
    }

    /// Write values with pre-computed definition and repetition levels (for nested types).
    mutating func writeWithLevels(values newValues: [ByteArray], defLevels newDefLevels: [Int32], repLevels newRepLevels: [Int32]) {
        values.append(contentsOf: newValues)
        if self.defLevels == nil { self.defLevels = [] }
        self.defLevels?.append(contentsOf: newDefLevels)
        if self.repLevels == nil { self.repLevels = [] }
        self.repLevels?.append(contentsOf: newRepLevels)
        totalSlots += newDefLevels.count
    }

    mutating func closeColumn(startOffset: Int64) -> ColumnChunkResult {
        let numValues = totalSlots > 0 ? Int32(totalSlots) : Int32(values.count)
        var pages = [Data]()
        var totalUncompressed: Int64 = 0
        var totalCompressed: Int64 = 0
        var dictPageOffset: Int64? = nil
        var encodings = [Encoding]()

        // Dictionary encoding for strings
        if options.useDictionary {
            var dictEncoder = ByteArrayDictionaryEncoder()
            dictEncoder.encodeAll(values)

            // Dictionary page (compressed with column codec)
            let dictData = dictEncoder.encodeDictionary()
            var compressedDictData = dictData
            if options.compression != .uncompressed {
                if let codec = try? CompressionCodecs.codec(for: options.compression),
                   let compressed = try? codec.compress(dictData) {
                    compressedDictData = compressed
                }
            }
            let dictHeader = PageHeader(
                type: .dictionaryPage,
                uncompressedPageSize: Int32(dictData.count),
                compressedPageSize: Int32(compressedDictData.count),
                dictionaryPageHeader: DictionaryPageHeader(
                    numValues: Int32(dictEncoder.dictionarySize),
                    encoding: .plainDictionary
                )
            )
            let dictHeaderBytes = ThriftCompactWriter.serialize(dictHeader)
            var dictPage = Data()
            dictPage.append(contentsOf: dictHeaderBytes)
            dictPage.append(contentsOf: compressedDictData)
            dictPageOffset = startOffset
            pages.append(dictPage)
            totalUncompressed += Int64(dictHeaderBytes.count + dictData.count)
            totalCompressed += Int64(dictPage.count)
            encodings.append(.plainDictionary)

            // Data page with RLE indices
            let indexData = dictEncoder.encodeIndices()
            let dataPageResult = buildCompressedPage(
                body: indexData, numValues: numValues,
                encoding: .rleDict, compression: options.compression,
                skipLevels: true // indices already include everything
            )
            pages.append(dataPageResult.bytes)
            totalUncompressed += Int64(dataPageResult.uncompressedTotal)
            totalCompressed += Int64(dataPageResult.compressedTotal)
            encodings.append(.rleDict)
        } else {
            // Plain encoding
            var valueBytes = Data()
            for v in values { v.encodePlain(to: &valueBytes) }

            let pageResult = buildCompressedPage(
                body: valueBytes, numValues: numValues,
                encoding: .plain, compression: options.compression,
                skipLevels: false
            )
            pages.append(pageResult.bytes)
            totalUncompressed += Int64(pageResult.uncompressedTotal)
            totalCompressed += Int64(pageResult.compressedTotal)
            encodings.append(.plain)
        }

        // Encrypt pages if encryption is configured
        if let encrypt = options.pageEncryptor {
            totalCompressed = 0
            for i in 0..<pages.count {
                let pageOrdinal = Int16(i)
                if let encrypted = try? encrypt(pages[i], pageOrdinal) {
                    pages[i] = encrypted
                }
                totalCompressed += Int64(pages[i].count)
            }
        }

        // Statistics for strings
        var statistics: Statistics? = nil
        if options.enableStatistics && !values.isEmpty {
            let sorted = values.sorted { $0.data.lexicographicallyPrecedes($1.data) }
            var minData = Data(), maxData = Data()
            sorted.first?.encodePlain(to: &minData)
            sorted.last?.encodePlain(to: &maxData)
            statistics = Statistics(max: nil, min: nil, nullCount: 0, distinctCount: nil,
                                  maxValue: maxData, minValue: minData)
        }

        let dataPageOffset = dictPageOffset != nil ? startOffset + Int64(pages[0].count) : startOffset

        let meta = ColumnMetaData(
            type: descriptor.physicalType,
            encodings: encodings,
            pathInSchema: descriptor.path,
            codec: options.compression,
            numValues: Int64(numValues),
            totalUncompressedSize: totalUncompressed,
            totalCompressedSize: totalCompressed,
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: dictPageOffset,
            statistics: statistics
        )

        return ColumnChunkResult(pages: pages, columnMetaData: meta, startOffset: startOffset)
    }

    private func buildCompressedPage(body: Data, numValues: Int32, encoding: Encoding,
                                      compression: CompressionCodec, skipLevels: Bool) -> PageBuildResult {
        var pageBody = Data()

        if !skipLevels {
            if descriptor.maxRepetitionLevel > 0 {
                let bw = bitWidthForMaxLevel(descriptor.maxRepetitionLevel)
                let encoder = RLEEncoder(bitWidth: bw)
                let rep = repLevels ?? [Int32](repeating: 0, count: Int(numValues))
                pageBody.append(contentsOf: encoder.encodeWithLengthPrefix(rep))
            }
            if descriptor.maxDefinitionLevel > 0, let def = defLevels {
                let bw = bitWidthForMaxLevel(descriptor.maxDefinitionLevel)
                let encoder = RLEEncoder(bitWidth: bw)
                pageBody.append(contentsOf: encoder.encodeWithLengthPrefix(def))
            }
        }

        pageBody.append(contentsOf: body)

        let uncompressedSize = Int32(pageBody.count)
        var compressedBody = pageBody
        if compression != .uncompressed {
            if let codec = try? CompressionCodecs.codec(for: compression),
               let compressed = try? codec.compress(pageBody) {
                compressedBody = compressed
            }
        }
        let compressedSize = Int32(compressedBody.count)

        let header = PageHeader(
            type: .dataPage,
            uncompressedPageSize: uncompressedSize,
            compressedPageSize: compressedSize,
            dataPageHeader: DataPageHeader(
                numValues: numValues, encoding: encoding,
                definitionLevelEncoding: .rle, repetitionLevelEncoding: .rle
            )
        )
        let headerBytes = ThriftCompactWriter.serialize(header)

        var result = Data()
        result.append(contentsOf: headerBytes)
        result.append(contentsOf: compressedBody)

        return PageBuildResult(
            bytes: result,
            uncompressedTotal: headerBytes.count + Int(uncompressedSize),
            compressedTotal: result.count
        )
    }
}

// MARK: - Internal helpers

struct PageBuildResult {
    let bytes: Data
    let uncompressedTotal: Int
    let compressedTotal: Int
}
