// RLE.swift — Run-Length Encoding / Bit-Packing Hybrid
// Port of github.com/apache/arrow-go/parquet/internal/utils/rle.go
//
// Used for encoding definition and repetition levels in Parquet data pages.
//
// Format:
//   encoded-block := run*
//   run := literal-run | repeated-run
//   literal-run:   varint((numGroups << 1) | 1)  then  ceil(numValues * bitWidth / 8) bytes
//   repeated-run:  varint(count << 1)             then  value padded to ceil(bitWidth / 8) bytes
//
// Groups are always 8 values wide (for literal runs). Final group is zero-padded.
// Bit-packing is LSB-first within each byte.

import Foundation

// MARK: - RLE Encoder

/// Encodes a sequence of small integer values (levels) using the Parquet RLE/bit-packing hybrid.
struct RLEEncoder {
    private let bitWidth: Int

    init(bitWidth: Int) {
        self.bitWidth = bitWidth
    }

    /// Encode values and return the encoded bytes (WITHOUT the 4-byte length prefix).
    func encode(_ values: [Int32]) -> Data {
        guard !values.isEmpty else { return Data() }
        guard bitWidth > 0 else {
            // bit width 0 means all values are 0 — nothing to write
            return Data()
        }

        var output = Data()
        var i = 0

        while i < values.count {
            // Check for a repeated run
            let runStart = i
            let runValue = values[i]
            while i < values.count && values[i] == runValue {
                i += 1
            }
            let runLength = i - runStart

            if runLength >= 8 {
                // Emit as RLE run
                appendVarint(UInt64(runLength) << 1, to: &output)
                appendPackedValue(UInt64(runValue), bitWidth: bitWidth, to: &output)
            } else {
                // Rewind and collect a literal run (groups of 8)
                i = runStart
                let literalStart = i
                var literalCount = 0

                // Collect values until we find a long repeated run or end of data
                while i < values.count {
                    // Peek ahead for a potential RLE run
                    let peekValue = values[i]
                    var peekCount = 0
                    var j = i
                    while j < values.count && values[j] == peekValue && peekCount < 8 {
                        j += 1
                        peekCount += 1
                    }
                    if peekCount >= 8 { break }  // start of an RLE run

                    literalCount += 1
                    i += 1
                }

                if literalCount == 0 { continue }

                // Pad to multiple of 8
                let numGroups = (literalCount + 7) / 8
                let paddedCount = numGroups * 8

                // Header: (numGroups << 1) | 1
                appendVarint((UInt64(numGroups) << 1) | 1, to: &output)

                // Bit-pack directly from source array, treating out-of-bounds as 0
                let packed = bitPackSlice(values, start: literalStart, count: paddedCount, bitWidth: bitWidth)
                output.append(contentsOf: packed)
            }
        }

        return output
    }

    /// Encode values and return bytes WITH the 4-byte LE length prefix.
    /// This is the format used for levels in Parquet data pages V1.
    func encodeWithLengthPrefix(_ values: [Int32]) -> Data {
        let encoded = encode(values)
        var result = Data(capacity: 4 + encoded.count)
        let len = UInt32(encoded.count)
        withUnsafeBytes(of: len.littleEndian) { result.append(contentsOf: $0) }
        result.append(contentsOf: encoded)
        return result
    }

    // MARK: - Helpers

    private func appendVarint(_ value: UInt64, to data: inout Data) {
        var v = value
        while v >= 0x80 {
            data.append(UInt8((v & 0x7F) | 0x80))
            v >>= 7
        }
        data.append(UInt8(v))
    }

    /// Append a single value packed into ceil(bitWidth/8) bytes.
    private func appendPackedValue(_ value: UInt64, bitWidth: Int, to data: inout Data) {
        let byteCount = (bitWidth + 7) / 8
        var v = value
        for _ in 0..<byteCount {
            data.append(UInt8(v & 0xFF))
            v >>= 8
        }
    }

    /// Bit-pack values from a slice of the source array (LSB-first within each byte).
    /// Uses shift-and-OR per value instead of bit-by-bit inner loop.
    private func bitPackSlice(_ values: [Int32], start: Int, count: Int, bitWidth: Int) -> Data {
        let totalBits = count * bitWidth
        let byteCount = (totalBits + 7) / 8
        var result = Data(count: byteCount)

        result.withUnsafeMutableBytes { buf in
            let ptr = buf.baseAddress!.assumingMemoryBound(to: UInt8.self)
            var bitPos = 0
            for i in 0..<count {
                let idx = start + i
                let v = idx < values.count ? UInt64(UInt32(bitPattern: values[idx])) : 0
                // Shift-and-OR: write the value's bits starting at bitPos
                let byteOff = bitPos >> 3
                let bitOff = bitPos & 7
                var shifted = v << bitOff
                var b = byteOff
                while shifted != 0 && b < byteCount {
                    ptr[b] |= UInt8(shifted & 0xFF)
                    shifted >>= 8
                    b += 1
                }
                bitPos += bitWidth
            }
        }

        return result
    }
}

// MARK: - RLE Decoder

/// Decodes RLE/bit-packing hybrid encoded data.
struct RLEDecoder {
    private let bitWidth: Int

    init(bitWidth: Int) {
        self.bitWidth = bitWidth
    }

    /// Decode values from RLE-encoded bytes (WITHOUT length prefix).
    /// Data may be a slice with non-zero startIndex.
    func decode(_ data: Data, expectedCount: Int) -> [Int32] {
        guard bitWidth > 0 && !data.isEmpty else {
            return [Int32](repeating: 0, count: expectedCount)
        }

        var result = [Int32]()
        result.reserveCapacity(expectedCount)
        let mask: UInt64 = bitWidth < 64 ? (UInt64(1) << bitWidth) - 1 : UInt64.max

        data.withUnsafeBytes { buf in
            var offset = 0

            while offset < buf.count && result.count < expectedCount {
                // Read varint header
                var header: UInt64 = 0
                var shift: UInt64 = 0
                while offset < buf.count {
                    let b = buf[offset]
                    offset += 1
                    header |= UInt64(b & 0x7F) << shift
                    if b & 0x80 == 0 { break }
                    shift += 7
                }

                if header & 1 == 1 {
                    // Literal run: (numGroups << 1) | 1
                    let numGroups = Int(header >> 1)
                    let numValues = numGroups * 8
                    let totalBits = numValues * bitWidth
                    let byteCount = (totalBits + 7) / 8

                    guard offset + byteCount <= buf.count else { return }

                    // Word-at-a-time bit unpacking
                    var bitPos = 0
                    for _ in 0..<numValues {
                        guard result.count < expectedCount else { break }
                        let byteOff = offset + (bitPos >> 3)
                        let bitOff = bitPos & 7

                        var word: UInt64 = 0
                        if byteOff + 8 <= buf.count {
                            word = buf.loadUnaligned(fromByteOffset: byteOff, as: UInt64.self)
                        } else {
                            // Fallback for last partial word near end of buffer
                            for b in 0..<min(8, buf.count - byteOff) {
                                word |= UInt64(buf[byteOff + b]) << (b * 8)
                            }
                        }
                        let value = UInt32((word >> bitOff) & mask)
                        result.append(Int32(bitPattern: value))
                        bitPos += bitWidth
                    }
                    offset += byteCount
                } else {
                    // RLE run: (count << 1)
                    let count = Int(header >> 1)
                    let valueByteCount = (bitWidth + 7) / 8
                    guard offset + valueByteCount <= buf.count else { return }

                    var value: UInt64 = 0
                    for i in 0..<valueByteCount {
                        value |= UInt64(buf[offset + i]) << (i * 8)
                    }
                    offset += valueByteCount

                    let val = Int32(bitPattern: UInt32(value & 0xFFFF_FFFF))
                    let n = min(count, expectedCount - result.count)
                    result.append(contentsOf: repeatElement(val, count: n))
                }
            }
        }

        // Pad if we didn't get enough
        if result.count < expectedCount {
            result.append(contentsOf: repeatElement(Int32(0), count: expectedCount - result.count))
        }

        return Array(result.prefix(expectedCount))
    }

    /// Decode from data WITH the 4-byte LE length prefix.
    /// Data may be a slice with non-zero startIndex.
    func decodeWithLengthPrefix(_ data: Data, at offset: Int, expectedCount: Int) -> (values: [Int32], bytesConsumed: Int) {
        guard offset + 4 <= data.count else {
            return ([Int32](repeating: 0, count: expectedCount), 0)
        }
        let start = data.startIndex + offset
        let len = data.withUnsafeBytes { buf in
            UInt32(littleEndian: buf.loadUnaligned(fromByteOffset: offset, as: UInt32.self))
        }
        let rleData = data[(start + 4)..<(start + 4 + Int(len))]
        let values = decode(rleData, expectedCount: expectedCount)
        return (values, 4 + Int(len))
    }
}

// MARK: - Convenience: compute bit width for a max level value

/// Returns the number of bits needed to represent values in [0, maxValue].
func bitWidthForMaxLevel(_ maxLevel: Int16) -> Int {
    if maxLevel == 0 { return 0 }
    var width = 0
    var v = Int(maxLevel)
    while v > 0 {
        width += 1
        v >>= 1
    }
    return width
}
