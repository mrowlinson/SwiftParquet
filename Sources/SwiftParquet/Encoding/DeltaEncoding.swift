// DeltaEncoding.swift — Delta Binary Packed and Delta Byte Array encodings
// Port of github.com/apache/arrow-go/parquet/internal/encoding/delta_*.go
//
// Delta Binary Packed (DELTA_BINARY_PACKED):
//   Header: block_size(uleb128), miniblock_count(uleb128), total_count(uleb128), first_value(zigzag)
//   Then blocks of miniblocks containing bit-packed deltas.
//
// Delta Byte Array (DELTA_BYTE_ARRAY):
//   Prefix lengths: delta-binary-packed encoded
//   Suffixes: delta-length-byte-array encoded
//
// Delta Length Byte Array (DELTA_LENGTH_BYTE_ARRAY):
//   Lengths: delta-binary-packed encoded
//   Concatenated byte data

import Foundation

// MARK: - Delta Binary Packed Encoder

struct DeltaBinaryPackedEncoder {
    private static let defaultBlockSize = 128
    private static let defaultMiniblockCount = 4

    static func encode(_ values: [Int64]) -> Data {
        guard !values.isEmpty else { return Data() }

        let blockSize = defaultBlockSize
        let miniblockCount = defaultMiniblockCount
        let miniblockSize = blockSize / miniblockCount
        let totalCount = values.count

        var output = Data()

        // Header
        appendULEB128(UInt64(blockSize), to: &output)
        appendULEB128(UInt64(miniblockCount), to: &output)
        appendULEB128(UInt64(totalCount), to: &output)
        appendZigzag(values[0], to: &output)

        // Compute deltas
        var deltas = [Int64]()
        deltas.reserveCapacity(values.count - 1)
        for i in 1..<values.count {
            deltas.append(values[i] - values[i - 1])
        }

        // Process blocks
        var deltaIdx = 0
        while deltaIdx < deltas.count {
            // Collect a block of deltas
            let blockEnd = min(deltaIdx + blockSize, deltas.count)
            var blockDeltas = Array(deltas[deltaIdx..<blockEnd])
            while blockDeltas.count < blockSize { blockDeltas.append(0) }

            // Find min delta in block
            let minDelta = blockDeltas.min()!

            // Subtract min delta
            let adjusted = blockDeltas.map { $0 - minDelta }

            appendZigzag(minDelta, to: &output)

            // For each miniblock, compute bit width and bit-pack
            var bitWidths = [UInt8]()
            for mb in 0..<miniblockCount {
                let mbStart = mb * miniblockSize
                let mbEnd = min(mbStart + miniblockSize, adjusted.count)
                let maxVal = adjusted[mbStart..<mbEnd].max() ?? 0
                let bw = maxVal == 0 ? 0 : (64 - maxVal.leadingZeroBitCount)
                bitWidths.append(UInt8(bw))
            }
            output.append(contentsOf: bitWidths)

            // Bit-pack each miniblock using shift-and-OR per value
            for mb in 0..<miniblockCount {
                let mbStart = mb * miniblockSize
                let bw = Int(bitWidths[mb])
                if bw == 0 { continue }
                let totalBits = miniblockSize * bw
                let byteCount = (totalBits + 7) / 8
                var bitBuf = Data(count: byteCount)
                bitBuf.withUnsafeMutableBytes { buf in
                    let ptr = buf.baseAddress!.assumingMemoryBound(to: UInt8.self)
                    var bitPos = 0
                    for i in 0..<miniblockSize {
                        let val = mbStart + i < adjusted.count ? UInt64(adjusted[mbStart + i]) : 0
                        let byteOff = bitPos >> 3
                        let bitOff = bitPos & 7
                        var shifted = val << bitOff
                        var b = byteOff
                        while shifted != 0 && b < byteCount {
                            ptr[b] |= UInt8(shifted & 0xFF)
                            shifted >>= 8
                            b += 1
                        }
                        bitPos += bw
                    }
                }
                output.append(bitBuf)
            }

            deltaIdx += blockSize
        }

        return output
    }

    private static func appendULEB128(_ value: UInt64, to data: inout Data) {
        var v = value
        while v >= 0x80 {
            data.append(UInt8((v & 0x7F) | 0x80))
            v >>= 7
        }
        data.append(UInt8(v))
    }

    private static func appendZigzag(_ value: Int64, to data: inout Data) {
        let u = UInt64(bitPattern: value)
        let zigzag = (u << 1) ^ UInt64(bitPattern: value >> 63)
        appendULEB128(zigzag, to: &data)
    }
}

// MARK: - Delta Binary Packed Decoder

struct DeltaBinaryPackedDecoder {
    static func decode(_ data: Data) throws -> [Int64] {
        var offset = 0

        func readULEB128() throws -> UInt64 {
            var result: UInt64 = 0
            var shift: UInt64 = 0
            while true {
                guard offset < data.count else { throw ParquetError.unexpectedEOF }
                let b = data[data.startIndex + offset]
                offset += 1
                result |= UInt64(b & 0x7F) << shift
                if b & 0x80 == 0 { break }
                shift += 7
            }
            return result
        }

        func readZigzag() throws -> Int64 {
            let n = try readULEB128()
            return Int64(bitPattern: (n >> 1) ^ UInt64(bitPattern: -(Int64(n & 1))))
        }

        let blockSize = Int(try readULEB128())
        let miniblockCount = Int(try readULEB128())
        let totalCount = Int(try readULEB128())
        let firstValue = try readZigzag()

        guard blockSize > 0 && miniblockCount > 0 else {
            return totalCount > 0 ? [firstValue] : []
        }

        let miniblockSize = blockSize / miniblockCount
        var values = [Int64]()
        values.reserveCapacity(totalCount)
        values.append(firstValue)

        var lastValue = firstValue

        while values.count < totalCount {
            let minDelta = try readZigzag()

            // Read bit widths for miniblocks
            var bitWidths = [Int]()
            for _ in 0..<miniblockCount {
                guard offset < data.count else { break }
                bitWidths.append(Int(data[data.startIndex + offset]))
                offset += 1
            }

            // Decode miniblocks
            for mb in 0..<miniblockCount {
                guard values.count < totalCount else { break }
                let bw = mb < bitWidths.count ? bitWidths[mb] : 0

                if bw == 0 {
                    for _ in 0..<miniblockSize {
                        guard values.count < totalCount else { break }
                        lastValue = lastValue + minDelta
                        values.append(lastValue)
                    }
                } else {
                    let mask: UInt64 = bw < 64 ? (UInt64(1) << bw) - 1 : UInt64.max
                    data.withUnsafeBytes { buf in
                        var bitPos = 0
                        let baseOffset = offset
                        for _ in 0..<miniblockSize {
                            guard values.count < totalCount else { break }
                            let byteOff = baseOffset + (bitPos >> 3)
                            let bitOff = bitPos & 7
                            var word: UInt64 = 0
                            if byteOff + 8 <= buf.count {
                                word = buf.loadUnaligned(fromByteOffset: byteOff, as: UInt64.self)
                            } else {
                                for b in 0..<min(8, buf.count - byteOff) {
                                    word |= UInt64(buf[byteOff + b]) << (b * 8)
                                }
                            }
                            let val = (word >> bitOff) & mask
                            let delta = Int64(val) + minDelta
                            lastValue = lastValue + delta
                            values.append(lastValue)
                            bitPos += bw
                        }
                        offset = baseOffset + (bitPos + 7) / 8
                    }
                }
            }
        }

        return Array(values.prefix(totalCount))
    }
}

// MARK: - Delta Length Byte Array Encoder

struct DeltaLengthByteArrayEncoder {
    static func encode(_ values: [ByteArray]) -> Data {
        let lengths = values.map { Int64($0.data.count) }
        var result = DeltaBinaryPackedEncoder.encode(lengths)
        for v in values {
            result.append(v.data)
        }
        return result
    }
}

// MARK: - Delta Length Byte Array Decoder

struct DeltaLengthByteArrayDecoder {
    static func decode(_ data: Data) throws -> [ByteArray] {
        let lengths = try DeltaBinaryPackedDecoder.decode(data)

        // Find where the lengths encoding ends and data begins
        // Re-decode to find the byte offset
        var offset = 0
        func readULEB128() throws -> UInt64 {
            var result: UInt64 = 0
            var shift: UInt64 = 0
            while true {
                guard offset < data.count else { throw ParquetError.unexpectedEOF }
                let b = data[data.startIndex + offset]
                offset += 1
                result |= UInt64(b & 0x7F) << shift
                if b & 0x80 == 0 { break }
                shift += 7
            }
            return result
        }
        // Skip header
        let blockSize = Int(try readULEB128())
        let miniblockCount = Int(try readULEB128())
        _ = try readULEB128() // totalCount
        _ = try readULEB128() // firstValue (zigzag)

        let miniblockSize = blockSize > 0 && miniblockCount > 0 ? blockSize / miniblockCount : 0
        var remaining = lengths.count - 1

        while remaining > 0 && offset < data.count {
            _ = try readULEB128() // minDelta
            let mbCount = min(miniblockCount, (remaining + miniblockSize - 1) / miniblockSize)
            var bitWidths = [Int]()
            for _ in 0..<miniblockCount {
                guard offset < data.count else { break }
                bitWidths.append(Int(data[data.startIndex + offset]))
                offset += 1
            }
            for mb in 0..<mbCount {
                let bw = mb < bitWidths.count ? bitWidths[mb] : 0
                let count = min(miniblockSize, remaining)
                if bw > 0 {
                    let totalBits = miniblockSize * bw
                    offset += (totalBits + 7) / 8
                }
                remaining -= count
            }
        }

        // Now read the actual byte data
        var values = [ByteArray]()
        values.reserveCapacity(lengths.count)
        for length in lengths {
            let len = Int(length)
            guard offset + len <= data.count else { break }
            let start = data.startIndex + offset
            values.append(ByteArray(Data(data[start..<(start + len)])))
            offset += len
        }

        return values
    }
}

// MARK: - Delta Byte Array Encoder

struct DeltaByteArrayEncoder {
    static func encode(_ values: [ByteArray]) -> Data {
        guard !values.isEmpty else { return Data() }

        // Compute prefix lengths
        var prefixLengths = [Int64]()
        var suffixes = [ByteArray]()

        prefixLengths.append(0) // First value has no prefix
        suffixes.append(values[0])

        for i in 1..<values.count {
            let prev = values[i - 1].data
            let curr = values[i].data
            var prefix = 0
            let minLen = min(prev.count, curr.count)
            while prefix < minLen && prev[prev.startIndex + prefix] == curr[curr.startIndex + prefix] {
                prefix += 1
            }
            prefixLengths.append(Int64(prefix))
            let suffixStart = curr.startIndex + prefix
            suffixes.append(ByteArray(Data(curr[suffixStart...])))
        }

        var result = DeltaBinaryPackedEncoder.encode(prefixLengths)
        result.append(DeltaLengthByteArrayEncoder.encode(suffixes))
        return result
    }
}

// MARK: - Delta Byte Array Decoder

struct DeltaByteArrayDecoder {
    static func decode(_ data: Data) throws -> [ByteArray] {
        let prefixLengths = try DeltaBinaryPackedDecoder.decode(data)

        // Find where prefix lengths end and suffix data begins
        // This requires re-scanning the delta binary packed encoding
        let offset = skipDeltaBinaryPacked(data, count: prefixLengths.count)

        let suffixData = Data(data[(data.startIndex + offset)...])
        let suffixes = try DeltaLengthByteArrayDecoder.decode(suffixData)

        guard prefixLengths.count == suffixes.count else {
            throw ParquetError.corruptedFile("delta byte array: prefix/suffix count mismatch")
        }

        var values = [ByteArray]()
        var previous = Data()

        for i in 0..<prefixLengths.count {
            let prefixLen = Int(prefixLengths[i])
            let prefix = prefixLen > 0 ? previous.prefix(prefixLen) : Data()
            var value = Data(prefix)
            value.append(suffixes[i].data)
            values.append(ByteArray(value))
            previous = value
        }

        return values
    }

    private static func skipDeltaBinaryPacked(_ data: Data, count: Int) -> Int {
        // Quick scan to find end of delta binary packed encoding
        var offset = 0
        func readULEB128() -> UInt64 {
            var result: UInt64 = 0
            var shift: UInt64 = 0
            while offset < data.count {
                let b = data[data.startIndex + offset]
                offset += 1
                result |= UInt64(b & 0x7F) << shift
                if b & 0x80 == 0 { break }
                shift += 7
            }
            return result
        }

        let blockSize = Int(readULEB128())
        let miniblockCount = Int(readULEB128())
        _ = readULEB128() // totalCount
        _ = readULEB128() // firstValue

        guard blockSize > 0 && miniblockCount > 0 else { return offset }
        let miniblockSize = blockSize / miniblockCount
        var remaining = count - 1

        while remaining > 0 && offset < data.count {
            _ = readULEB128() // minDelta
            for _ in 0..<miniblockCount {
                guard offset < data.count else { return offset }
                offset += 1
            }
            for mb in 0..<miniblockCount {
                guard remaining > 0 else { break }
                let bw = offset - miniblockCount + mb < data.count ? Int(data[data.startIndex + offset - miniblockCount + mb]) : 0
                let _ = min(miniblockSize, remaining)
                if bw > 0 {
                    offset += (miniblockSize * bw + 7) / 8
                }
                remaining -= min(miniblockSize, remaining)
            }
        }

        return offset
    }
}

// MARK: - Int32 Delta Encoder/Decoder (convenience wrappers)

extension DeltaBinaryPackedEncoder {
    static func encodeInt32s(_ values: [Int32]) -> Data {
        encode(values.map { Int64($0) })
    }
}

extension DeltaBinaryPackedDecoder {
    static func decodeInt32s(_ data: Data) throws -> [Int32] {
        try decode(data).map { Int32($0) }
    }
}
