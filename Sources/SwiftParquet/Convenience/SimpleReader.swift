// SimpleReader.swift — High-level API for reading Parquet files
// Mirrors SimpleWriter.swift's ParquetFileWriter with a symmetric API.

import Foundation

// MARK: - ParquetFileReader

/// High-level Parquet file reader.
///
/// Example:
/// ```swift
/// let table = try ParquetFileReader.read(path: "data.parquet")
/// print(table.columnNames)
/// if case .strings(let names) = table.column("name") {
///     print(names)
/// }
/// ```
public struct ParquetFileReader {

    /// Read a Parquet file and return all data as a ParquetTable.
    public static func read(path: String) throws -> ParquetTable {
        let reader = try ParquetFileReaderCore(path: path)
        return try reader.readAll()
    }

    /// Read a Parquet file from Data.
    public static func read(data: Data) throws -> ParquetTable {
        let reader = try ParquetFileReaderCore(data: data)
        return try reader.readAll()
    }

    /// Read only the metadata (schema, row counts, etc.) without reading data.
    public static func readMetadata(path: String) throws -> ParquetFileReaderCore {
        try ParquetFileReaderCore(path: path)
    }

    /// Read specific row groups.
    public static func read(path: String, rowGroups: [Int]) throws -> ParquetTable {
        let reader = try ParquetFileReaderCore(path: path)
        var allColumns: [(name: String, values: ColumnValues)]? = nil
        var totalRows = 0

        for rgIndex in rowGroups {
            let rgColumns = try reader.readRowGroup(rgIndex)
            totalRows += Int(reader.metadata.rowGroups[rgIndex].numRows)

            if allColumns == nil {
                allColumns = rgColumns
            } else {
                for (i, (_, values)) in rgColumns.enumerated() {
                    guard i < allColumns!.count else { break }
                    allColumns![i].values = merge(allColumns![i].values, values)
                }
            }
        }

        return ParquetTable(
            columns: allColumns ?? [],
            numRows: totalRows,
            metadata: reader.keyValueMetadata
        )
    }

    /// Read a Parquet file with parallel column decoding.
    public static func readAsync(path: String) async throws -> ParquetTable {
        let reader = try ParquetFileReaderCore(path: path)
        return try await reader.readAllAsync()
    }

    /// Read from Data with parallel column decoding.
    public static func readAsync(data: Data) async throws -> ParquetTable {
        let reader = try ParquetFileReaderCore(data: data)
        return try await reader.readAllAsync()
    }

    private static func merge(_ a: ColumnValues, _ b: ColumnValues) -> ColumnValues {
        switch (a, b) {
        case (.strings(let va), .strings(let vb)): return .strings(va + vb)
        case (.int32s(let va), .int32s(let vb)): return .int32s(va + vb)
        case (.int64s(let va), .int64s(let vb)): return .int64s(va + vb)
        case (.floats(let va), .floats(let vb)): return .floats(va + vb)
        case (.doubles(let va), .doubles(let vb)): return .doubles(va + vb)
        case (.booleans(let va), .booleans(let vb)): return .booleans(va + vb)
        case (.byteArrays(let va), .byteArrays(let vb)): return .byteArrays(va + vb)
        default: return a
        }
    }
}
