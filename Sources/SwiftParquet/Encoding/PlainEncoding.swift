// PlainEncoding.swift — Plain encoding for all Parquet physical types
// Port of github.com/apache/arrow-go/parquet/internal/encoding/plain.go
//
// Plain encoding:
//   BOOLEAN:            bit-packed, 8 values per byte, LSB-first
//   INT32/FLOAT:        4-byte little-endian
//   INT64/DOUBLE:       8-byte little-endian
//   INT96:              12-byte little-endian
//   BYTE_ARRAY:         4-byte LE length prefix + raw bytes
//   FIXED_LEN_BYTE_ARRAY: raw bytes (length known from schema)

import Foundation

// MARK: - ParquetValue Protocol

/// A type that can be plain-encoded and decoded as a Parquet physical value.
protocol ParquetValue: Sendable {
    static var physicalType: PhysicalType { get }
    func encodePlain(to buffer: inout Data)
}

// MARK: - Bool (BOOLEAN — bit-packed 8 per byte, LSB first)

extension Bool: ParquetValue {
    static var physicalType: PhysicalType { .boolean }

    func encodePlain(to buffer: inout Data) {
        // Individual bools are packed externally by PlainBoolEncoder.
        // This method is unused — Bool uses PlainBoolEncoder.flush().
        fatalError("Bool should use PlainBoolEncoder, not encodePlain directly")
    }
}

// MARK: - Int32

extension Int32: ParquetValue {
    static var physicalType: PhysicalType { .int32 }

    func encodePlain(to buffer: inout Data) {
        withUnsafeBytes(of: self.littleEndian) { buffer.append(contentsOf: $0) }
    }
}

// MARK: - Int64

extension Int64: ParquetValue {
    static var physicalType: PhysicalType { .int64 }

    func encodePlain(to buffer: inout Data) {
        withUnsafeBytes(of: self.littleEndian) { buffer.append(contentsOf: $0) }
    }
}

// MARK: - Float

extension Float: ParquetValue {
    static var physicalType: PhysicalType { .float }

    func encodePlain(to buffer: inout Data) {
        withUnsafeBytes(of: self.bitPattern.littleEndian) { buffer.append(contentsOf: $0) }
    }
}

// MARK: - Double

extension Double: ParquetValue {
    static var physicalType: PhysicalType { .double }

    func encodePlain(to buffer: inout Data) {
        withUnsafeBytes(of: self.bitPattern.littleEndian) { buffer.append(contentsOf: $0) }
    }
}

// MARK: - Int96

extension Int96: ParquetValue {
    static var physicalType: PhysicalType { .int96 }

    func encodePlain(to buffer: inout Data) {
        withUnsafeBytes(of: value.0.littleEndian) { buffer.append(contentsOf: $0) }
        withUnsafeBytes(of: value.1.littleEndian) { buffer.append(contentsOf: $0) }
        withUnsafeBytes(of: value.2.littleEndian) { buffer.append(contentsOf: $0) }
    }
}

// MARK: - ByteArray (length-prefixed)

/// Wrapper for BYTE_ARRAY physical type (strings, blobs).
public struct ByteArray: ParquetValue, Sendable {
    public let data: Data

    public init(_ data: Data) { self.data = data }
    public init(_ string: String) { self.data = Data(string.utf8) }
    public init(_ bytes: [UInt8]) { self.data = Data(bytes) }

    static var physicalType: PhysicalType { .byteArray }

    func encodePlain(to buffer: inout Data) {
        // 4-byte little-endian length
        let len = UInt32(data.count)
        withUnsafeBytes(of: len.littleEndian) { buffer.append(contentsOf: $0) }
        buffer.append(contentsOf: data)
    }
}

// MARK: - FixedLenByteArray

/// Wrapper for FIXED_LEN_BYTE_ARRAY physical type.
public struct FixedLenByteArray: ParquetValue, Sendable {
    public let data: Data
    public let length: Int32

    public init(_ data: Data) {
        self.data = data
        self.length = Int32(data.count)
    }

    static var physicalType: PhysicalType { .fixedLenByteArray }

    func encodePlain(to buffer: inout Data) {
        // No length prefix — length is in the schema.
        buffer.append(contentsOf: data)
    }
}

// MARK: - Plain Decoder

/// Decodes plain-encoded values from a Data buffer.
struct PlainDecoder {

    static func decodeInt32s(_ data: Data, count: Int) -> [Int32] {
        var result = [Int32]()
        result.reserveCapacity(count)
        var offset = data.startIndex
        for _ in 0..<count {
            guard offset + 4 <= data.endIndex else { break }
            var v: Int32 = 0
            withUnsafeMutableBytes(of: &v) { ptr in
                for i in 0..<4 { ptr[i] = data[offset + i] }
            }
            result.append(Int32(littleEndian: v))
            offset += 4
        }
        return result
    }

    static func decodeInt64s(_ data: Data, count: Int) -> [Int64] {
        var result = [Int64]()
        result.reserveCapacity(count)
        var offset = data.startIndex
        for _ in 0..<count {
            guard offset + 8 <= data.endIndex else { break }
            var v: Int64 = 0
            withUnsafeMutableBytes(of: &v) { ptr in
                for i in 0..<8 { ptr[i] = data[offset + i] }
            }
            result.append(Int64(littleEndian: v))
            offset += 8
        }
        return result
    }

    static func decodeFloats(_ data: Data, count: Int) -> [Float] {
        var result = [Float]()
        result.reserveCapacity(count)
        var offset = data.startIndex
        for _ in 0..<count {
            guard offset + 4 <= data.endIndex else { break }
            var bits: UInt32 = 0
            withUnsafeMutableBytes(of: &bits) { ptr in
                for i in 0..<4 { ptr[i] = data[offset + i] }
            }
            result.append(Float(bitPattern: UInt32(littleEndian: bits)))
            offset += 4
        }
        return result
    }

    static func decodeDoubles(_ data: Data, count: Int) -> [Double] {
        var result = [Double]()
        result.reserveCapacity(count)
        var offset = data.startIndex
        for _ in 0..<count {
            guard offset + 8 <= data.endIndex else { break }
            var bits: UInt64 = 0
            withUnsafeMutableBytes(of: &bits) { ptr in
                for i in 0..<8 { ptr[i] = data[offset + i] }
            }
            result.append(Double(bitPattern: UInt64(littleEndian: bits)))
            offset += 8
        }
        return result
    }

    static func decodeBooleans(_ data: Data, count: Int) -> [Bool] {
        var result = [Bool]()
        result.reserveCapacity(count)
        for i in 0..<count {
            let byteIdx = i / 8
            let bitIdx = i % 8
            guard data.startIndex + byteIdx < data.endIndex else { break }
            let bit = (data[data.startIndex + byteIdx] >> bitIdx) & 1
            result.append(bit != 0)
        }
        return result
    }

    static func decodeByteArrays(_ data: Data, count: Int) -> [ByteArray] {
        var result = [ByteArray]()
        result.reserveCapacity(count)
        var offset = data.startIndex
        for _ in 0..<count {
            guard offset + 4 <= data.endIndex else { break }
            var len: UInt32 = 0
            withUnsafeMutableBytes(of: &len) { ptr in
                for i in 0..<4 { ptr[i] = data[offset + i] }
            }
            len = UInt32(littleEndian: len)
            offset += 4
            guard offset + Int(len) <= data.endIndex else { break }
            result.append(ByteArray(Data(data[offset..<(offset + Int(len))])))
            offset += Int(len)
        }
        return result
    }

    static func decodeFixedLenByteArrays(_ data: Data, count: Int, typeLength: Int) -> [FixedLenByteArray] {
        var result = [FixedLenByteArray]()
        result.reserveCapacity(count)
        var offset = data.startIndex
        for _ in 0..<count {
            guard offset + typeLength <= data.endIndex else { break }
            result.append(FixedLenByteArray(Data(data[offset..<(offset + typeLength)])))
            offset += typeLength
        }
        return result
    }

    static func decodeInt96s(_ data: Data, count: Int) -> [Int96] {
        var result = [Int96]()
        result.reserveCapacity(count)
        var offset = data.startIndex
        for _ in 0..<count {
            guard offset + 12 <= data.endIndex else { break }
            var w0: UInt32 = 0, w1: UInt32 = 0, w2: UInt32 = 0
            withUnsafeMutableBytes(of: &w0) { ptr in for i in 0..<4 { ptr[i] = data[offset + i] } }
            withUnsafeMutableBytes(of: &w1) { ptr in for i in 0..<4 { ptr[i] = data[offset + 4 + i] } }
            withUnsafeMutableBytes(of: &w2) { ptr in for i in 0..<4 { ptr[i] = data[offset + 8 + i] } }
            result.append(Int96((UInt32(littleEndian: w0), UInt32(littleEndian: w1), UInt32(littleEndian: w2))))
            offset += 12
        }
        return result
    }
}

// MARK: - Plain Encoder (generic)

/// Encodes a sequence of ParquetValues using plain encoding.
struct PlainEncoder<T: ParquetValue> {
    private(set) var buffer: Data = Data()

    mutating func encode(_ value: T) {
        value.encodePlain(to: &buffer)
    }

    mutating func encodeAll(_ values: [T]) {
        for v in values { encode(v) }
    }

    var bytes: Data { buffer }
    var byteCount: Int { buffer.count }
}

// MARK: - Bool Bit-Packer

/// Encodes booleans as bit-packed bytes (LSB first, 8 bools per byte).
struct PlainBoolEncoder {
    private(set) var buffer: Data = Data()
    private var currentByte: UInt8 = 0
    private var bitIndex: Int = 0

    mutating func encode(_ value: Bool) {
        if value {
            currentByte |= (1 << bitIndex)
        }
        bitIndex += 1
        if bitIndex == 8 {
            flush()
        }
    }

    mutating func encodeAll(_ values: [Bool]) {
        for v in values { encode(v) }
    }

    mutating func flush() {
        buffer.append(currentByte)
        currentByte = 0
        bitIndex = 0
    }

    /// Call after encoding all values to flush any partial byte.
    mutating func finalize() {
        if bitIndex > 0 {
            flush()
        }
    }
}
