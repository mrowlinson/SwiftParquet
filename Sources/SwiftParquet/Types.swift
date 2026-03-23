// Types.swift — Physical types, encodings, repetition levels, error types
// Port of github.com/apache/arrow-go/parquet/types.go

import Foundation

// MARK: - Physical Type

public enum PhysicalType: Int32, Sendable {
    case boolean           = 0
    case int32             = 1
    case int64             = 2
    case int96             = 3
    case float             = 4
    case double            = 5
    case byteArray         = 6
    case fixedLenByteArray = 7
}

// MARK: - Encoding

public enum Encoding: Int32, Sendable {
    case plain                = 0
    case plainDictionary      = 2  // deprecated alias for rleDict when reading
    case rle                  = 3
    case bitPacked            = 4  // deprecated
    case deltaBinaryPacked    = 5
    case deltaLengthByteArray = 6
    case deltaByteArray       = 7
    case rleDict              = 8
    case byteStreamSplit      = 9
}

// MARK: - Compression

public enum CompressionCodec: Int32, Sendable {
    case uncompressed = 0
    case snappy       = 1
    case gzip         = 2
    case lzo          = 3
    case brotli       = 4
    case lz4          = 5
    case zstd         = 6
    case lz4Raw       = 7
}

// MARK: - Repetition / Field Types

public enum Repetition: Int32, Sendable {
    case required = 0
    case optional = 1
    case repeated = 2
}

// MARK: - ConvertedType (legacy, superseded by LogicalType)

public enum ConvertedType: Int32, Sendable {
    case utf8            = 0
    case map             = 1
    case mapKeyValue     = 2
    case list            = 3
    case `enum`          = 4
    case decimal         = 5
    case date            = 6
    case timeMillis      = 7
    case timeMicros      = 8
    case timestampMillis = 9
    case timestampMicros = 10
    case uint8           = 11
    case uint16          = 12
    case uint32          = 13
    case uint64          = 14
    case int8            = 15
    case int16           = 16
    case int32           = 17
    case int64           = 18
    case json            = 19
    case bson            = 20
    case interval        = 21
}

// MARK: - Page Type

public enum PageType: Int32, Sendable {
    case dataPage       = 0
    case indexPage      = 1
    case dictionaryPage = 2
    case dataPageV2     = 3
}

// MARK: - Int96

/// 12-byte integer used for representing timestamps (deprecated in modern Parquet).
public struct Int96: Sendable, Hashable {
    /// The three 32-bit words stored as a flat 12-byte array (little-endian).
    public var value: (UInt32, UInt32, UInt32)

    public init(_ value: (UInt32, UInt32, UInt32) = (0, 0, 0)) {
        self.value = value
    }

    public static func == (lhs: Int96, rhs: Int96) -> Bool {
        lhs.value.0 == rhs.value.0 && lhs.value.1 == rhs.value.1 && lhs.value.2 == rhs.value.2
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(value.0)
        hasher.combine(value.1)
        hasher.combine(value.2)
    }
}

// MARK: - Error

public enum ParquetError: Error, LocalizedError {
    case unexpectedEOF
    case invalidMagicBytes
    case unsupportedEncoding(Encoding)
    case unsupportedCompression(String)
    case invalidSchema(String)
    case invalidPageHeader(String)
    case typeMismatch(expected: PhysicalType, got: PhysicalType)
    case corruptedFile(String)
    case thriftError(String)
    case ioError(String)

    public var errorDescription: String? {
        switch self {
        case .unexpectedEOF:                  return "Unexpected end of file"
        case .invalidMagicBytes:              return "Invalid Parquet magic bytes (expected PAR1)"
        case .unsupportedEncoding(let e):     return "Unsupported encoding: \(e)"
        case .unsupportedCompression(let c):  return "Unsupported compression: \(c)"
        case .invalidSchema(let m):           return "Invalid schema: \(m)"
        case .invalidPageHeader(let m):       return "Invalid page header: \(m)"
        case .typeMismatch(let e, let g):     return "Type mismatch: expected \(e), got \(g)"
        case .corruptedFile(let m):           return "Corrupted file: \(m)"
        case .thriftError(let m):             return "Thrift error: \(m)"
        case .ioError(let m):                 return "IO error: \(m)"
        }
    }
}
