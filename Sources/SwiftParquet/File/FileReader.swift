// FileReader.swift — Read a complete Parquet file
// Port of github.com/apache/arrow-go/parquet/file/file_reader.go
//
// Parquet file layout:
//   [4 bytes] Magic: "PAR1"
//   [N bytes] Row groups (column chunks → pages)
//   [M bytes] FileMetaData (Thrift compact)
//   [4 bytes] Footer length (M as little-endian i32)
//   [4 bytes] Magic: "PAR1"

import Foundation

#if canImport(CryptoKit)
import CryptoKit
#else
import Crypto
#endif

private let parquetMagicBytes = Data([0x50, 0x41, 0x52, 0x31])  // "PAR1"
private let parquetEncMagicBytes = Data([0x50, 0x41, 0x52, 0x45])  // "PARE"

// MARK: - Parquet Table (read result)

/// The result of reading a Parquet file: column names + typed values per column.
public struct ParquetTable: Sendable {
    public let columns: [(name: String, values: ColumnValues)]
    public let numRows: Int
    public let metadata: [String: String]

    /// Get a column by name.
    public func column(_ name: String) -> ColumnValues? {
        columns.first { $0.name == name }?.values
    }

    /// All column names in schema order.
    public var columnNames: [String] { columns.map { $0.name } }
}

// MARK: - File Reader

/// Low-level Parquet file reader.
public struct ParquetFileReaderCore {
    let metadata: FileMetaData
    public let schema: ParquetSchema
    private let data: Data
    private let footerDecryptor: FooterEncryptor?  // reuses the closure type
    private let columnDecryptorFactory: ((String, Int16, Int16) -> PageDecryptor?)?

    /// Initialize from raw file data.
    public init(data: Data, encryption: ParquetEncryptionConfig? = nil) throws {
        guard data.count >= 12 else {
            throw ParquetError.corruptedFile("file too small (\(data.count) bytes)")
        }

        let headerMagic = data[data.startIndex..<(data.startIndex + 4)]
        let footerMagic = data[(data.endIndex - 4)..<data.endIndex]
        let isEncryptedFooter = headerMagic == parquetEncMagicBytes && footerMagic == parquetEncMagicBytes

        if !isEncryptedFooter {
            guard headerMagic == parquetMagicBytes else { throw ParquetError.invalidMagicBytes }
            guard footerMagic == parquetMagicBytes else { throw ParquetError.invalidMagicBytes }
        }

        let footerLenStart = data.endIndex - 8
        let footerLength = data.withUnsafeBytes { buf in
            UInt32(littleEndian: buf.loadUnaligned(fromByteOffset: footerLenStart - data.startIndex, as: UInt32.self))
        }

        guard Int(footerLength) + 8 <= data.count else {
            throw ParquetError.corruptedFile("footer length \(footerLength) exceeds file size")
        }

        let metadataStart = data.endIndex - 8 - Int(footerLength)
        let metadataSlice = data[metadataStart..<(metadataStart + Int(footerLength))]

        // Decrypt footer if PARE magic, otherwise pass slice directly (no copy)
        let metadataData: Data
        if isEncryptedFooter {
            guard let enc = encryption, let footerKey = enc.footerKey else {
                throw ParquetError.corruptedFile("encrypted footer requires encryption key")
            }
            let aad = ParquetAESGCM.buildAAD(moduleType: .footer)
            metadataData = try ParquetAESGCM.decrypt(Data(metadataSlice), key: footerKey, aad: aad)
        } else {
            metadataData = metadataSlice
        }

        var reader = ThriftCompactReader(data: metadataData)
        self.metadata = try FileMetaData.read(from: &reader)
        self.data = data
        self.footerDecryptor = nil

        // Page-level decryption not yet supported (only footer encryption)
        self.columnDecryptorFactory = nil

        self.schema = try ParquetFileReaderCore.buildSchema(from: self.metadata.schema)
    }

    /// Initialize from a file path.
    public init(path: String, encryption: ParquetEncryptionConfig? = nil) throws {
        let url = URL(fileURLWithPath: path)
        let fileData = try Data(contentsOf: url, options: .mappedIfSafe)
        try self.init(data: fileData, encryption: encryption)
    }

    /// Number of row groups in the file.
    public var numRowGroups: Int { metadata.rowGroups.count }

    /// Total number of rows across all row groups.
    public var numRows: Int64 { metadata.numRows }

    /// Number of columns.
    public var numColumns: Int { schema.numColumns }

    /// Column names in schema order.
    public var columnNames: [String] { schema.columns.map { $0.name } }

    /// Key-value metadata from the file.
    public var keyValueMetadata: [String: String] {
        var result = [String: String]()
        for kv in metadata.keyValueMetadata ?? [] {
            result[kv.key] = kv.value
        }
        return result
    }

    /// Read all columns from a specific row group.
    public func readRowGroup(_ index: Int) throws -> [(name: String, values: ColumnValues)] {
        guard index >= 0 && index < metadata.rowGroups.count else {
            throw ParquetError.corruptedFile("row group index \(index) out of range")
        }

        let rowGroup = metadata.rowGroups[index]
        let leafColumns = schema.columns

        var result = [(name: String, values: ColumnValues)]()
        result.reserveCapacity(rowGroup.columns.count)

        for (i, chunk) in rowGroup.columns.enumerated() {
            guard i < leafColumns.count else { break }
            let leaf = leafColumns[i]
            let descriptor = ColumnDescriptor(node: leaf)

            let decryptor = columnDecryptorFactory?(leaf.name, Int16(index), Int16(i))
            let reader = ColumnChunkReader(
                columnMeta: chunk.metaData,
                fileData: data,
                maxDefLevel: descriptor.maxDefinitionLevel,
                maxRepLevel: descriptor.maxRepetitionLevel,
                decryptor: decryptor
            )
            let values = try reader.readAll()
            result.append((name: leaf.name, values: values))
        }

        return result
    }

    /// Read the entire file into a ParquetTable.
    public func readAll() throws -> ParquetTable {
        var allColumns: [(name: String, values: ColumnValues)]? = nil

        for rgIndex in 0..<numRowGroups {
            let rgColumns = try readRowGroup(rgIndex)

            if allColumns == nil {
                allColumns = rgColumns
            } else {
                // Merge row groups by appending values
                for (i, (_, values)) in rgColumns.enumerated() {
                    guard i < allColumns!.count else { break }
                    allColumns![i].values = mergeColumnValues(allColumns![i].values, values)
                }
            }
        }

        return ParquetTable(
            columns: allColumns ?? [],
            numRows: Int(numRows),
            metadata: keyValueMetadata
        )
    }

    // MARK: - Async Reading (parallel column decoding)

    /// Read all columns from a row group in parallel using structured concurrency.
    public func readRowGroupAsync(_ index: Int) async throws -> [(name: String, values: ColumnValues)] {
        guard index >= 0 && index < metadata.rowGroups.count else {
            throw ParquetError.corruptedFile("row group index \(index) out of range")
        }

        let rowGroup = metadata.rowGroups[index]
        let leafColumns = schema.columns
        let fileData = self.data

        return try await withThrowingTaskGroup(of: (Int, String, ColumnValues).self) { group in
            for (i, chunk) in rowGroup.columns.enumerated() {
                guard i < leafColumns.count else { break }
                let leaf = leafColumns[i]
                let descriptor = ColumnDescriptor(node: leaf)

                group.addTask {
                    let reader = ColumnChunkReader(
                        columnMeta: chunk.metaData,
                        fileData: fileData,
                        maxDefLevel: descriptor.maxDefinitionLevel,
                        maxRepLevel: descriptor.maxRepetitionLevel
                    )
                    let values = try reader.readAll()
                    return (i, leaf.name, values)
                }
            }

            var results = [(Int, String, ColumnValues)]()
            results.reserveCapacity(rowGroup.columns.count)
            for try await result in group {
                results.append(result)
            }
            results.sort { $0.0 < $1.0 }
            return results.map { ($0.1, $0.2) }
        }
    }

    /// Read the entire file with parallel column decoding.
    public func readAllAsync() async throws -> ParquetTable {
        var allColumns: [(name: String, values: ColumnValues)]? = nil

        for rgIndex in 0..<numRowGroups {
            let rgColumns = try await readRowGroupAsync(rgIndex)

            if allColumns == nil {
                allColumns = rgColumns
            } else {
                for (i, (_, values)) in rgColumns.enumerated() {
                    guard i < allColumns!.count else { break }
                    allColumns![i].values = mergeColumnValues(allColumns![i].values, values)
                }
            }
        }

        return ParquetTable(
            columns: allColumns ?? [],
            numRows: Int(numRows),
            metadata: keyValueMetadata
        )
    }

    // MARK: - Schema Building

    private static func buildSchema(from elements: [SchemaElement]) throws -> ParquetSchema {
        guard !elements.isEmpty else {
            throw ParquetError.invalidSchema("empty schema")
        }

        var index = 0
        let root = try buildNode(from: elements, index: &index)
        guard let group = root as? GroupNode else {
            throw ParquetError.invalidSchema("root must be a group node")
        }
        return ParquetSchema(fields: group.children)
    }

    private static func buildNode(from elements: [SchemaElement], index: inout Int) throws -> any SchemaNode {
        guard index < elements.count else {
            throw ParquetError.invalidSchema("schema element index out of range")
        }
        let element = elements[index]
        index += 1

        if let numChildren = element.numChildren, numChildren > 0 {
            var children = [any SchemaNode]()
            for _ in 0..<numChildren {
                let child = try buildNode(from: elements, index: &index)
                children.append(child)
            }
            return GroupNode(
                name: element.name,
                repetition: element.repetitionType ?? .required,
                children: children,
                convertedType: element.convertedType
            )
        } else {
            // Primitive node
            return PrimitiveNode(
                name: element.name,
                repetition: element.repetitionType ?? .required,
                physicalType: element.type ?? .byteArray,
                convertedType: element.convertedType,
                logicalType: element.logicalType,
                typeLength: element.typeLength
            )
        }
    }

    private func mergeColumnValues(_ a: ColumnValues, _ b: ColumnValues) -> ColumnValues {
        switch (a, b) {
        case (.strings(let va), .strings(let vb)): return .strings(va + vb)
        case (.int32s(let va), .int32s(let vb)): return .int32s(va + vb)
        case (.int64s(let va), .int64s(let vb)): return .int64s(va + vb)
        case (.floats(let va), .floats(let vb)): return .floats(va + vb)
        case (.doubles(let va), .doubles(let vb)): return .doubles(va + vb)
        case (.booleans(let va), .booleans(let vb)): return .booleans(va + vb)
        case (.byteArrays(let va), .byteArrays(let vb)): return .byteArrays(va + vb)
        default: return a // type mismatch: keep first
        }
    }
}
