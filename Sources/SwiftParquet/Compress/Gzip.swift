// Gzip.swift — Gzip compression/decompression
// RFC 1952 framing around raw deflate.
// Apple: uses Compression framework. Linux: uses system zlib via CZlib module.

import Foundation

#if canImport(Compression)
import Compression
#elseif canImport(CZlib)
import CZlib
#endif

struct GzipCodec: CompressionCodecProtocol {

    func compress(_ input: Data) throws -> Data {
        guard !input.isEmpty else {
            return GzipCodec.emptyGzip
        }

        let deflated = try rawDeflate(input)

        var result = Data(capacity: 10 + deflated.count + 8)
        result.append(contentsOf: [0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF])
        result.append(deflated)
        let crc = CRC32.checksum(input)
        withUnsafeBytes(of: crc.littleEndian) { result.append(contentsOf: $0) }
        let size = UInt32(truncatingIfNeeded: input.count)
        withUnsafeBytes(of: size.littleEndian) { result.append(contentsOf: $0) }

        return result
    }

    func decompress(_ input: Data, uncompressedSize: Int) throws -> Data {
        guard input.count >= 18 else {
            throw ParquetError.corruptedFile("gzip: data too short")
        }
        guard input[input.startIndex] == 0x1f && input[input.startIndex + 1] == 0x8b else {
            throw ParquetError.corruptedFile("gzip: invalid magic bytes")
        }
        guard input[input.startIndex + 2] == 0x08 else {
            throw ParquetError.corruptedFile("gzip: unsupported compression method")
        }

        let flags = input[input.startIndex + 3]
        var offset = 10

        if flags & 0x04 != 0 {
            guard offset + 2 <= input.count else { throw ParquetError.corruptedFile("gzip: truncated FEXTRA") }
            let xlen = Int(input[input.startIndex + offset]) | (Int(input[input.startIndex + offset + 1]) << 8)
            offset += 2 + xlen
        }
        if flags & 0x08 != 0 {
            while offset < input.count && input[input.startIndex + offset] != 0 { offset += 1 }
            offset += 1
        }
        if flags & 0x10 != 0 {
            while offset < input.count && input[input.startIndex + offset] != 0 { offset += 1 }
            offset += 1
        }
        if flags & 0x02 != 0 { offset += 2 }

        guard input.count >= offset + 8 else {
            throw ParquetError.corruptedFile("gzip: truncated data")
        }

        let trailerStart = input.startIndex + input.count - 8
        let deflatedData = input[(input.startIndex + offset)..<trailerStart]

        let result = try rawInflate(Data(deflatedData), expectedSize: uncompressedSize)

        var expectedCRC: UInt32 = 0
        withUnsafeMutableBytes(of: &expectedCRC) { ptr in
            for i in 0..<4 { ptr[i] = input[trailerStart + i] }
        }
        expectedCRC = UInt32(littleEndian: expectedCRC)
        let actualCRC = CRC32.checksum(result)
        guard actualCRC == expectedCRC else {
            throw ParquetError.corruptedFile("gzip: CRC32 mismatch (expected \(expectedCRC), got \(actualCRC))")
        }

        return result
    }

    // MARK: - Raw deflate/inflate (platform-specific)

#if canImport(Compression)
    private func rawDeflate(_ input: Data) throws -> Data {
        try input.withUnsafeBytes { srcPtr -> Data in
            guard let src = srcPtr.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return Data() }
            let dstCapacity = input.count + input.count / 10 + 64
            var dst = Data(count: dstCapacity)
            let compressedSize = dst.withUnsafeMutableBytes { dstPtr -> Int in
                guard let dstBase = dstPtr.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return 0 }
                return compression_encode_buffer(dstBase, dstCapacity, src, input.count, nil, COMPRESSION_ZLIB)
            }
            guard compressedSize > 0 else {
                throw ParquetError.unsupportedCompression("gzip: deflate failed")
            }
            return dst.prefix(compressedSize)
        }
    }

    private func rawInflate(_ input: Data, expectedSize: Int) throws -> Data {
        try input.withUnsafeBytes { srcPtr -> Data in
            guard let src = srcPtr.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return Data() }
            let dstCapacity = max(expectedSize, input.count * 4)
            var dst = Data(count: dstCapacity)
            let decompressedSize = dst.withUnsafeMutableBytes { dstPtr -> Int in
                guard let dstBase = dstPtr.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return 0 }
                return compression_decode_buffer(dstBase, dstCapacity, src, input.count, nil, COMPRESSION_ZLIB)
            }
            guard decompressedSize > 0 else {
                throw ParquetError.unsupportedCompression("gzip: inflate failed")
            }
            return dst.prefix(decompressedSize)
        }
    }
#else
    // Linux: use system zlib via CZlib module
    private func rawDeflate(_ input: Data) throws -> Data {
        try input.withUnsafeBytes { srcPtr -> Data in
            guard let src = srcPtr.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return Data() }
            let dstCapacity = input.count + input.count / 10 + 64
            var dst = Data(count: dstCapacity)
            let compressedSize = try dst.withUnsafeMutableBytes { dstPtr -> Int in
                guard let dstBase = dstPtr.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return 0 }
                var stream = z_stream()
                stream.next_in = UnsafeMutablePointer(mutating: src)
                stream.avail_in = uInt(input.count)
                stream.next_out = dstBase
                stream.avail_out = uInt(dstCapacity)
                // windowBits = -15 for raw deflate (no zlib/gzip header)
                guard deflateInit2_(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -15, 8,
                                     Z_DEFAULT_STRATEGY, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size)) == Z_OK else {
                    throw ParquetError.unsupportedCompression("gzip: deflateInit2 failed")
                }
                defer { deflateEnd(&stream) }
                guard deflate(&stream, Z_FINISH) == Z_STREAM_END else {
                    throw ParquetError.unsupportedCompression("gzip: deflate failed")
                }
                return Int(stream.total_out)
            }
            return dst.prefix(compressedSize)
        }
    }

    private func rawInflate(_ input: Data, expectedSize: Int) throws -> Data {
        try input.withUnsafeBytes { srcPtr -> Data in
            guard let src = srcPtr.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return Data() }
            let dstCapacity = max(expectedSize, input.count * 4)
            var dst = Data(count: dstCapacity)
            let decompressedSize = try dst.withUnsafeMutableBytes { dstPtr -> Int in
                guard let dstBase = dstPtr.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return 0 }
                var stream = z_stream()
                stream.next_in = UnsafeMutablePointer(mutating: src)
                stream.avail_in = uInt(input.count)
                stream.next_out = dstBase
                stream.avail_out = uInt(dstCapacity)
                guard inflateInit2_(&stream, -15, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size)) == Z_OK else {
                    throw ParquetError.unsupportedCompression("gzip: inflateInit2 failed")
                }
                defer { inflateEnd(&stream) }
                guard inflate(&stream, Z_FINISH) == Z_STREAM_END else {
                    throw ParquetError.unsupportedCompression("gzip: inflate failed")
                }
                return Int(stream.total_out)
            }
            return dst.prefix(decompressedSize)
        }
    }
#endif

    private static let emptyGzip: Data = {
        var d = Data([0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF])
        d.append(contentsOf: [0x03, 0x00])
        d.append(contentsOf: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
        return d
    }()
}
