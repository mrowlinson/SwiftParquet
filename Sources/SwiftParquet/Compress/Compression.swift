// Compression.swift — Codec abstraction for Parquet compression
// Each codec implements compress/decompress for page data.

import Foundation

protocol CompressionCodecProtocol {
    func compress(_ input: Data) throws -> Data
    func decompress(_ input: Data, uncompressedSize: Int) throws -> Data
}

/// Registry mapping CompressionCodec enum values to implementations.
enum CompressionCodecs {
    static func codec(for codec: CompressionCodec) throws -> CompressionCodecProtocol {
        switch codec {
        case .uncompressed:
            return UncompressedCodec()
        case .snappy:
            return SnappyCodec()
        case .gzip:
            return GzipCodec()
        case .zstd:
            return ZstdCodec()
        default:
            throw ParquetError.unsupportedCompression("\(codec)")
        }
    }
}

/// Pass-through codec for uncompressed data.
struct UncompressedCodec: CompressionCodecProtocol {
    func compress(_ input: Data) throws -> Data { input }
    func decompress(_ input: Data, uncompressedSize: Int) throws -> Data { input }
}
