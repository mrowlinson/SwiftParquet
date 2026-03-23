// DictionaryEncoding.swift — Dictionary encoding for Parquet columns
// Port of github.com/apache/arrow-go/parquet/internal/encoding/dictionary.go
//
// Dictionary encoding stores unique values in a dictionary page, then encodes
// each value as an RLE-encoded index into the dictionary.
// This is extremely effective for string columns with low cardinality.
//
// Layout:
//   Dictionary page: plain-encoded unique values
//   Data pages: RLE/bit-packed hybrid encoded indices into the dictionary

import Foundation

// MARK: - Dictionary Encoder

/// Builds a dictionary of unique values and encodes indices.
struct DictionaryEncoder<T: ParquetValue & Hashable> {
    private(set) var dictionary: [T] = []
    private var indexMap: [T: Int32] = [:]
    private(set) var indices: [Int32] = []

    /// Add a value, returning its dictionary index.
    @discardableResult
    mutating func encode(_ value: T) -> Int32 {
        if let idx = indexMap[value] {
            indices.append(idx)
            return idx
        }
        let idx = Int32(dictionary.count)
        dictionary.append(value)
        indexMap[value] = idx
        indices.append(idx)
        return idx
    }

    mutating func encodeAll(_ values: [T]) {
        for v in values { encode(v) }
    }

    /// Bit width needed to represent the largest index.
    var bitWidth: Int {
        bitWidthForMaxLevel(Int16(max(dictionary.count - 1, 0)))
    }

    /// Number of unique values.
    var dictionarySize: Int { dictionary.count }

    /// Plain-encode the dictionary values for the dictionary page.
    func encodeDictionary() -> Data {
        var buffer = Data()
        for v in dictionary {
            v.encodePlain(to: &buffer)
        }
        return buffer
    }

    /// RLE-encode the indices for data pages.
    func encodeIndices() -> Data {
        let bw = bitWidth
        guard bw > 0 else { return Data([0]) } // bit-width byte + nothing
        let rle = RLEEncoder(bitWidth: bw)
        var result = Data()
        result.append(UInt8(bw))  // bit-width prefix byte (required by Parquet dict data pages)
        result.append(contentsOf: rle.encode(indices))
        return result
    }
}

// MARK: - ByteArray Dictionary Encoder (strings)

struct ByteArrayDictionaryEncoder {
    private(set) var dictionary: [ByteArray] = []
    private var indexMap: [Data: Int32] = [:]
    private(set) var indices: [Int32] = []

    @discardableResult
    mutating func encode(_ value: ByteArray) -> Int32 {
        if let idx = indexMap[value.data] {
            indices.append(idx)
            return idx
        }
        let idx = Int32(dictionary.count)
        dictionary.append(value)
        indexMap[value.data] = idx
        indices.append(idx)
        return idx
    }

    mutating func encodeAll(_ values: [ByteArray]) {
        for v in values { encode(v) }
    }

    var bitWidth: Int {
        bitWidthForMaxLevel(Int16(max(dictionary.count - 1, 0)))
    }

    var dictionarySize: Int { dictionary.count }

    func encodeDictionary() -> Data {
        var buffer = Data()
        for v in dictionary {
            v.encodePlain(to: &buffer)
        }
        return buffer
    }

    func encodeIndices() -> Data {
        let bw = bitWidth
        guard bw > 0 else { return Data([0]) }
        let rle = RLEEncoder(bitWidth: bw)
        var result = Data()
        result.append(UInt8(bw))
        result.append(contentsOf: rle.encode(indices))
        return result
    }
}

// MARK: - Dictionary Decoder

/// Decodes dictionary-encoded column data.
struct DictionaryDecoder {
    let dictionaryData: Data
    let physicalType: PhysicalType
    let typeLength: Int32?

    /// Decode a data page's RLE-encoded indices and look up values in the dictionary.
    func decodeByteArrays(indexData: Data, numValues: Int) throws -> [ByteArray] {
        let dictValues = try decodeDictionaryByteArrays()
        let indices = try decodeIndices(indexData, numValues: numValues)

        var result = [ByteArray]()
        result.reserveCapacity(indices.count)
        for idx in indices {
            guard idx >= 0 && idx < dictValues.count else {
                throw ParquetError.corruptedFile("dictionary index \(idx) out of range (dict size: \(dictValues.count))")
            }
            result.append(dictValues[Int(idx)])
        }
        return result
    }

    func decodeInt32s(indexData: Data, numValues: Int) throws -> [Int32] {
        let dictValues = try decodeDictionaryValues(as: Int32.self)
        return try lookupIndices(indexData, numValues: numValues, dictionary: dictValues)
    }

    func decodeInt64s(indexData: Data, numValues: Int) throws -> [Int64] {
        let dictValues = try decodeDictionaryValues(as: Int64.self)
        return try lookupIndices(indexData, numValues: numValues, dictionary: dictValues)
    }

    func decodeFloats(indexData: Data, numValues: Int) throws -> [Float] {
        let dictValues = try decodeDictionaryValues(as: Float.self)
        return try lookupIndices(indexData, numValues: numValues, dictionary: dictValues)
    }

    func decodeDoubles(indexData: Data, numValues: Int) throws -> [Double] {
        let dictValues = try decodeDictionaryValues(as: Double.self)
        return try lookupIndices(indexData, numValues: numValues, dictionary: dictValues)
    }

    // MARK: - Internal

    private func decodeIndices(_ data: Data, numValues: Int) throws -> [Int32] {
        guard !data.isEmpty else { return [] }
        let bitWidth = Int(data[data.startIndex])
        guard bitWidth >= 0 && bitWidth <= 32 else {
            throw ParquetError.corruptedFile("invalid dictionary bit width: \(bitWidth)")
        }
        if bitWidth == 0 {
            // All values are index 0
            return [Int32](repeating: 0, count: numValues)
        }
        let rleData = data[(data.startIndex + 1)...]
        return RLEDecoder(bitWidth: bitWidth).decode(rleData, expectedCount: numValues)
    }

    private func decodeDictionaryByteArrays() throws -> [ByteArray] {
        var values = [ByteArray]()
        let si = dictionaryData.startIndex
        dictionaryData.withUnsafeBytes { buf in
            var offset = 0
            while offset + 4 <= buf.count {
                let len = Int(UInt32(littleEndian: buf.loadUnaligned(fromByteOffset: offset, as: UInt32.self)))
                offset += 4
                guard offset + len <= buf.count else { break }
                values.append(ByteArray(Data(dictionaryData[(si + offset)..<(si + offset + len)])))
                offset += len
            }
        }
        return values
    }

    private func decodeDictionaryValues<T>(as type: T.Type) throws -> [T] where T: FixedWidthParquetValue {
        let stride = MemoryLayout<T>.size
        var values = [T]()
        var offset = 0
        while offset + stride <= dictionaryData.count {
            let start = dictionaryData.startIndex + offset
            let v = T.decodePlain(from: dictionaryData, at: start)
            values.append(v)
            offset += stride
        }
        return values
    }

    private func lookupIndices<T>(_ indexData: Data, numValues: Int, dictionary: [T]) throws -> [T] {
        let indices = try decodeIndices(indexData, numValues: numValues)
        var result = [T]()
        result.reserveCapacity(indices.count)
        for idx in indices {
            guard idx >= 0 && idx < dictionary.count else {
                throw ParquetError.corruptedFile("dictionary index \(idx) out of range")
            }
            result.append(dictionary[Int(idx)])
        }
        return result
    }
}

/// Protocol for fixed-size values that can be decoded at a byte offset.
protocol FixedWidthParquetValue: ParquetValue {
    static func decodePlain(from data: Data, at offset: Data.Index) -> Self
}

extension Int32: FixedWidthParquetValue {
    static func decodePlain(from data: Data, at offset: Data.Index) -> Int32 {
        data.withUnsafeBytes { buf in
            Int32(littleEndian: buf.loadUnaligned(fromByteOffset: offset - data.startIndex, as: Int32.self))
        }
    }
}

extension Int64: FixedWidthParquetValue {
    static func decodePlain(from data: Data, at offset: Data.Index) -> Int64 {
        data.withUnsafeBytes { buf in
            Int64(littleEndian: buf.loadUnaligned(fromByteOffset: offset - data.startIndex, as: Int64.self))
        }
    }
}

extension Float: FixedWidthParquetValue {
    static func decodePlain(from data: Data, at offset: Data.Index) -> Float {
        data.withUnsafeBytes { buf in
            Float(bitPattern: UInt32(littleEndian: buf.loadUnaligned(fromByteOffset: offset - data.startIndex, as: UInt32.self)))
        }
    }
}

extension Double: FixedWidthParquetValue {
    static func decodePlain(from data: Data, at offset: Data.Index) -> Double {
        data.withUnsafeBytes { buf in
            Double(bitPattern: UInt64(littleEndian: buf.loadUnaligned(fromByteOffset: offset - data.startIndex, as: UInt64.self)))
        }
    }
}
