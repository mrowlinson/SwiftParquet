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
        data.withUnsafeBytes { buf in
            var result = [Int32]()
            result.reserveCapacity(count)
            var off = 0
            for _ in 0..<count {
                guard off + 4 <= buf.count else { break }
                result.append(Int32(littleEndian: buf.loadUnaligned(fromByteOffset: off, as: Int32.self)))
                off += 4
            }
            return result
        }
    }

    static func decodeInt64s(_ data: Data, count: Int) -> [Int64] {
        data.withUnsafeBytes { buf in
            var result = [Int64]()
            result.reserveCapacity(count)
            var off = 0
            for _ in 0..<count {
                guard off + 8 <= buf.count else { break }
                result.append(Int64(littleEndian: buf.loadUnaligned(fromByteOffset: off, as: Int64.self)))
                off += 8
            }
            return result
        }
    }

    static func decodeFloats(_ data: Data, count: Int) -> [Float] {
        data.withUnsafeBytes { buf in
            var result = [Float]()
            result.reserveCapacity(count)
            var off = 0
            for _ in 0..<count {
                guard off + 4 <= buf.count else { break }
                result.append(Float(bitPattern: UInt32(littleEndian: buf.loadUnaligned(fromByteOffset: off, as: UInt32.self))))
                off += 4
            }
            return result
        }
    }

    static func decodeDoubles(_ data: Data, count: Int) -> [Double] {
        data.withUnsafeBytes { buf in
            var result = [Double]()
            result.reserveCapacity(count)
            var off = 0
            for _ in 0..<count {
                guard off + 8 <= buf.count else { break }
                result.append(Double(bitPattern: UInt64(littleEndian: buf.loadUnaligned(fromByteOffset: off, as: UInt64.self))))
                off += 8
            }
            return result
        }
    }

    static func decodeBooleans(_ data: Data, count: Int) -> [Bool] {
        data.withUnsafeBytes { buf in
            var result = [Bool]()
            result.reserveCapacity(count)
            for i in 0..<count {
                let byteIdx = i >> 3
                let bitIdx = i & 7
                guard byteIdx < buf.count else { break }
                result.append((buf[byteIdx] >> bitIdx) & 1 != 0)
            }
            return result
        }
    }

    static func decodeByteArrays(_ data: Data, count: Int) -> [ByteArray] {
        var result = [ByteArray]()
        result.reserveCapacity(count)
        let si = data.startIndex
        data.withUnsafeBytes { buf in
            var off = 0
            for _ in 0..<count {
                guard off + 4 <= buf.count else { return }
                let len = Int(UInt32(littleEndian: buf.loadUnaligned(fromByteOffset: off, as: UInt32.self)))
                off += 4
                guard off + len <= buf.count else { return }
                result.append(ByteArray(Data(data[(si + off)..<(si + off + len)])))
                off += len
            }
        }
        return result
    }

    static func decodeFixedLenByteArrays(_ data: Data, count: Int, typeLength: Int) -> [FixedLenByteArray] {
        var result = [FixedLenByteArray]()
        result.reserveCapacity(count)
        let si = data.startIndex
        var off = 0
        for _ in 0..<count {
            guard off + typeLength <= data.count else { break }
            result.append(FixedLenByteArray(Data(data[(si + off)..<(si + off + typeLength)])))
            off += typeLength
        }
        return result
    }

    static func decodeInt96s(_ data: Data, count: Int) -> [Int96] {
        data.withUnsafeBytes { buf in
            var result = [Int96]()
            result.reserveCapacity(count)
            var off = 0
            for _ in 0..<count {
                guard off + 12 <= buf.count else { break }
                let w0 = UInt32(littleEndian: buf.loadUnaligned(fromByteOffset: off, as: UInt32.self))
                let w1 = UInt32(littleEndian: buf.loadUnaligned(fromByteOffset: off + 4, as: UInt32.self))
                let w2 = UInt32(littleEndian: buf.loadUnaligned(fromByteOffset: off + 8, as: UInt32.self))
                result.append(Int96((w0, w1, w2)))
                off += 12
            }
            return result
        }
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
