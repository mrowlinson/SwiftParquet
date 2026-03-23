// Snappy.swift — Pure Swift Snappy compression/decompression
// Reference: https://github.com/google/snappy/blob/main/format_description.txt
//
// Snappy compressed format:
//   <preamble> <element>*
//   preamble := uvarint(uncompressed_length)
//   element  := literal | copy1 | copy2 | copy4
//
// Tag byte bits [1:0] determine element type:
//   00 = literal, 01 = copy (1-byte offset), 10 = copy (2-byte offset), 11 = copy (4-byte offset)

import Foundation

struct SnappyCodec: CompressionCodecProtocol {

    func decompress(_ input: Data, uncompressedSize: Int) throws -> Data {
        try SnappyDecompressor.decompress(input)
    }

    func compress(_ input: Data) throws -> Data {
        SnappyCompressor.compress(input)
    }
}

// MARK: - Decompressor

enum SnappyDecompressor {

    static func decompress(_ data: Data) throws -> Data {
        var offset = 0

        func readByte() throws -> UInt8 {
            guard offset < data.count else { throw ParquetError.corruptedFile("snappy: unexpected EOF") }
            let b = data[data.startIndex + offset]
            offset += 1
            return b
        }

        func readUVarint() throws -> Int {
            var result = 0
            var shift = 0
            while true {
                let b = try readByte()
                result |= Int(b & 0x7F) << shift
                if b & 0x80 == 0 { break }
                shift += 7
                guard shift < 64 else { throw ParquetError.corruptedFile("snappy: varint overflow") }
            }
            return result
        }

        let uncompressedLength = try readUVarint()
        var output = Data(capacity: uncompressedLength)

        while offset < data.count {
            let tag = try readByte()
            let elementType = tag & 0x03

            switch elementType {
            case 0x00: // Literal
                var length: Int
                let lenField = Int(tag >> 2)
                if lenField < 60 {
                    length = lenField + 1
                } else {
                    let extraBytes = lenField - 59
                    length = 0
                    for i in 0..<extraBytes {
                        length |= Int(try readByte()) << (i * 8)
                    }
                    length += 1
                }
                guard offset + length <= data.count else {
                    throw ParquetError.corruptedFile("snappy: literal overflows input")
                }
                let start = data.startIndex + offset
                output.append(data[start..<(start + length)])
                offset += length

            case 0x01: // Copy with 1-byte offset (length 4..11, offset 0..2047)
                let length = 4 + Int((tag >> 2) & 0x07)
                let offsetHigh = Int(tag >> 5) & 0x07
                let offsetLow = Int(try readByte())
                let copyOffset = (offsetHigh << 8) | offsetLow
                guard copyOffset > 0 && copyOffset <= output.count else {
                    throw ParquetError.corruptedFile("snappy: invalid copy offset \(copyOffset)")
                }
                try appendCopy(to: &output, offset: copyOffset, length: length)

            case 0x02: // Copy with 2-byte offset (length 1..64, offset 0..65535)
                let length = 1 + Int(tag >> 2)
                let b0 = Int(try readByte())
                let b1 = Int(try readByte())
                let copyOffset = b0 | (b1 << 8)
                guard copyOffset > 0 && copyOffset <= output.count else {
                    throw ParquetError.corruptedFile("snappy: invalid copy offset \(copyOffset)")
                }
                try appendCopy(to: &output, offset: copyOffset, length: length)

            case 0x03: // Copy with 4-byte offset (length 1..64, offset 0..2^31-1)
                let length = 1 + Int(tag >> 2)
                var copyOffset = 0
                for i in 0..<4 {
                    copyOffset |= Int(try readByte()) << (i * 8)
                }
                guard copyOffset > 0 && copyOffset <= output.count else {
                    throw ParquetError.corruptedFile("snappy: invalid copy offset \(copyOffset)")
                }
                try appendCopy(to: &output, offset: copyOffset, length: length)

            default:
                throw ParquetError.corruptedFile("snappy: invalid tag type")
            }
        }

        guard output.count == uncompressedLength else {
            throw ParquetError.corruptedFile(
                "snappy: expected \(uncompressedLength) bytes, got \(output.count)")
        }
        return output
    }

    private static func appendCopy(to output: inout Data, offset: Int, length: Int) throws {
        let srcStart = output.count - offset
        // Must copy byte-by-byte because source and dest can overlap
        for i in 0..<length {
            output.append(output[output.startIndex + srcStart + (i % offset)])
        }
    }
}

// MARK: - Compressor

enum SnappyCompressor {
    private static let maxHashTableBits = 14
    private static let maxHashTableSize = 1 << maxHashTableBits

    static func compress(_ data: Data) -> Data {
        var output = Data()

        // Preamble: uncompressed length as uvarint
        appendUVarint(data.count, to: &output)

        guard !data.isEmpty else { return output }

        data.withUnsafeBytes { rawPtr in
            guard let src = rawPtr.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return }
            let srcLen = data.count

            var hashTable = [Int](repeating: -1, count: maxHashTableSize)
            var ip = 0          // current input position
            var litStart = 0    // start of pending literal

            func hash4(_ pos: Int) -> Int {
                guard pos + 3 < srcLen else { return 0 }
                let v = UInt32(src[pos]) | (UInt32(src[pos+1]) << 8) |
                        (UInt32(src[pos+2]) << 16) | (UInt32(src[pos+3]) << 24)
                return Int((v &* 0x1e35a7bd) >> (32 - maxHashTableBits))
            }

            func emitLiteral(_ start: Int, _ length: Int) {
                if length <= 60 {
                    output.append(UInt8((length - 1) << 2))
                } else if length <= 256 {
                    output.append(UInt8(60 << 2))
                    output.append(UInt8(length - 1))
                } else if length <= 65536 {
                    output.append(UInt8(61 << 2))
                    output.append(UInt8((length - 1) & 0xFF))
                    output.append(UInt8(((length - 1) >> 8) & 0xFF))
                } else if length <= 16_777_216 {
                    output.append(UInt8(62 << 2))
                    output.append(UInt8((length - 1) & 0xFF))
                    output.append(UInt8(((length - 1) >> 8) & 0xFF))
                    output.append(UInt8(((length - 1) >> 16) & 0xFF))
                } else {
                    output.append(UInt8(63 << 2))
                    output.append(UInt8((length - 1) & 0xFF))
                    output.append(UInt8(((length - 1) >> 8) & 0xFF))
                    output.append(UInt8(((length - 1) >> 16) & 0xFF))
                    output.append(UInt8(((length - 1) >> 24) & 0xFF))
                }
                output.append(contentsOf: UnsafeBufferPointer(start: src + start, count: length))
            }

            func emitCopy(offset: Int, length: Int) {
                var remaining = length
                // Emit copy2 for lengths > 64 or offsets > 2047
                while remaining > 0 {
                    if remaining >= 4 && remaining <= 11 && offset <= 2047 {
                        // copy1: 1-byte offset
                        let tag = UInt8(((remaining - 4) << 2) | ((offset >> 8) << 5) | 0x01)
                        output.append(tag)
                        output.append(UInt8(offset & 0xFF))
                        remaining = 0
                    } else {
                        // copy2: 2-byte offset
                        let len = min(remaining, 64)
                        let tag = UInt8(((len - 1) << 2) | 0x02)
                        output.append(tag)
                        output.append(UInt8(offset & 0xFF))
                        output.append(UInt8((offset >> 8) & 0xFF))
                        remaining -= len
                    }
                }
            }

            while ip + 4 <= srcLen {
                let h = hash4(ip)
                let candidate = hashTable[h]
                hashTable[h] = ip

                if candidate >= 0 && ip - candidate <= 65535 {
                    // Check match
                    var matchLen = 0
                    while ip + matchLen < srcLen && candidate + matchLen < ip &&
                          src[ip + matchLen] == src[candidate + matchLen] {
                        matchLen += 1
                    }

                    if matchLen >= 4 {
                        // Emit pending literal
                        if ip > litStart {
                            emitLiteral(litStart, ip - litStart)
                        }
                        emitCopy(offset: ip - candidate, length: matchLen)
                        ip += matchLen
                        litStart = ip
                        continue
                    }
                }

                ip += 1
            }

            // Emit remaining literal
            if litStart < srcLen {
                emitLiteral(litStart, srcLen - litStart)
            }
        }

        return output
    }

    private static func appendUVarint(_ value: Int, to data: inout Data) {
        var v = value
        while v >= 0x80 {
            data.append(UInt8((v & 0x7F) | 0x80))
            v >>= 7
        }
        data.append(UInt8(v))
    }
}
