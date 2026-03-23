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

                var literalValues = Array(values[literalStart..<(literalStart + literalCount)])
                while literalValues.count < paddedCount {
                    literalValues.append(0)
                }

                // Header: (numGroups << 1) | 1
                appendVarint((UInt64(numGroups) << 1) | 1, to: &output)

                // Bit-pack the values
                let packed = bitPack(literalValues, bitWidth: bitWidth)
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

    /// Bit-pack an array of values (LSB-first within each byte).
    private func bitPack(_ values: [Int32], bitWidth: Int) -> Data {
        let totalBits = values.count * bitWidth
        let byteCount = (totalBits + 7) / 8
        var result = Data(count: byteCount)

        var bitPos = 0
        for value in values {
            var v = UInt64(UInt32(bitPattern: value))
            for _ in 0..<bitWidth {
                let byteIdx = bitPos / 8
                let bitIdx = bitPos % 8
                result[result.startIndex + byteIdx] |= UInt8((v & 1) << bitIdx)
                v >>= 1
                bitPos += 1
            }
        }

        return result
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
