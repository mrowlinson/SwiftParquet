// SimpleWriter.swift — High-level API for writing Parquet files
// Hides the schema/column/page/row-group details behind a simple typed API.

import Foundation

// MARK: - Column Value Box

/// Type-erased column values. Used by ParquetFileWriter.writeRowGroup.
public enum ColumnValues: Sendable {
    case strings([String])
    case int32s([Int32])
    case int64s([Int64])
    case floats([Float])
    case doubles([Double])
    case booleans([Bool])
    case byteArrays([ByteArray])

    var count: Int {
        switch self {
        case .strings(let v):    return v.count
        case .int32s(let v):     return v.count
        case .int64s(let v):     return v.count
        case .floats(let v):     return v.count
        case .doubles(let v):    return v.count
        case .booleans(let v):   return v.count
        case .byteArrays(let v): return v.count
        }
    }
}

// MARK: - Write Options

/// Options for controlling how data is written.
public struct ParquetWriteOptions: Sendable {
    public var compression: CompressionCodec
    public var useDictionary: Bool
    public var enableStatistics: Bool
    public var dataPageVersion: DataPageVersion

    public init(
        compression: CompressionCodec = .uncompressed,
        useDictionary: Bool = false,
        enableStatistics: Bool = true,
        dataPageVersion: DataPageVersion = .v1
    ) {
        self.compression = compression
        self.useDictionary = useDictionary
        self.enableStatistics = enableStatistics
        self.dataPageVersion = dataPageVersion
    }

    /// Snappy compression with dictionary encoding.
    public static let snappy = ParquetWriteOptions(compression: .snappy, useDictionary: true)

    /// Gzip compression with dictionary encoding.
    public static let gzip = ParquetWriteOptions(compression: .gzip, useDictionary: true)

    /// Zstd compression with dictionary encoding.
    public static let zstd = ParquetWriteOptions(compression: .zstd, useDictionary: true)
}

// MARK: - Schema Builder

/// Build a flat (non-nested) Parquet schema from column name+type pairs.
public struct SchemaBuilder {
    private var fields: [any SchemaNode] = []

    public init() {}

    @discardableResult
    public mutating func addColumn(name: String, type: PhysicalType, repetition: Repetition = .required) -> SchemaBuilder {
        let convertedType: ConvertedType? = (type == .byteArray) ? .utf8 : nil
        let logicalType: LogicalTypeThrift? = (type == .byteArray) ? LogicalTypeThrift(kind: .string) : nil
        let node = PrimitiveNode(
            name: name, repetition: repetition, physicalType: type,
            convertedType: convertedType, logicalType: logicalType
        )
        fields.append(node)
        return self
    }

    /// Add a nested group.
    @discardableResult
    public mutating func addGroup(name: String, repetition: Repetition = .required, children: [any SchemaNode]) -> SchemaBuilder {
        let node = GroupNode(name: name, repetition: repetition, children: children)
        fields.append(node)
        return self
    }

    /// Add a List column (standard 3-level encoding).
    /// Schema: optional group <name> (LIST) { repeated group list { optional <type> element } }
    @discardableResult
    public mutating func addList(name: String, elementType: PhysicalType, elementRepetition: Repetition = .optional, repetition: Repetition = .optional) -> SchemaBuilder {
        let elementConvertedType: ConvertedType? = (elementType == .byteArray) ? .utf8 : nil
        let elementLogicalType: LogicalTypeThrift? = (elementType == .byteArray) ? LogicalTypeThrift(kind: .string) : nil
        let element = PrimitiveNode(name: "element", repetition: elementRepetition,
                                     physicalType: elementType, convertedType: elementConvertedType,
                                     logicalType: elementLogicalType)
        let listGroup = GroupNode(name: "list", repetition: .repeated, children: [element])
        let outerGroup = GroupNode(name: name, repetition: repetition, children: [listGroup],
                                    convertedType: .list)
        fields.append(outerGroup)
        return self
    }

    /// Add a Map column (standard encoding).
    /// Schema: optional group <name> (MAP) { repeated group key_value { required <key> key; optional <value> value } }
    @discardableResult
    public mutating func addMap(name: String, keyType: PhysicalType, valueType: PhysicalType, repetition: Repetition = .optional) -> SchemaBuilder {
        let keyConvertedType: ConvertedType? = (keyType == .byteArray) ? .utf8 : nil
        let keyLogicalType: LogicalTypeThrift? = (keyType == .byteArray) ? LogicalTypeThrift(kind: .string) : nil
        let valConvertedType: ConvertedType? = (valueType == .byteArray) ? .utf8 : nil
        let valLogicalType: LogicalTypeThrift? = (valueType == .byteArray) ? LogicalTypeThrift(kind: .string) : nil

        let key = PrimitiveNode(name: "key", repetition: .required, physicalType: keyType,
                                 convertedType: keyConvertedType, logicalType: keyLogicalType)
        let value = PrimitiveNode(name: "value", repetition: .optional, physicalType: valueType,
                                   convertedType: valConvertedType, logicalType: valLogicalType)
        let kvGroup = GroupNode(name: "key_value", repetition: .repeated, children: [key, value],
                                 convertedType: .mapKeyValue)
        let outerGroup = GroupNode(name: name, repetition: repetition, children: [kvGroup],
                                    convertedType: .map)
        fields.append(outerGroup)
        return self
    }

    /// Add a Struct column.
    @discardableResult
    public mutating func addStruct(name: String, fields structFields: [any SchemaNode], repetition: Repetition = .optional) -> SchemaBuilder {
        let node = GroupNode(name: name, repetition: repetition, children: structFields)
        fields.append(node)
        return self
    }

    public func build() -> ParquetSchema {
        ParquetSchema(fields: fields)
    }
}

// MARK: - ParquetFileWriter

/// High-level Parquet file writer with optional compression and dictionary encoding.
public struct ParquetFileWriter {
    private let path: String
    private let schema: ParquetSchema
    private let writeOptions: ParquetWriteOptions
    private var fileWriter: FileWriter

    public init(path: String, schema: ParquetSchema, options: ParquetWriteOptions = ParquetWriteOptions()) {
        self.path = path
        self.schema = schema
        self.writeOptions = options
        self.fileWriter = FileWriter(schema: schema)
    }

    public mutating func writeRowGroup(columns: [(String, ColumnValues)]) throws {
        guard columns.count == schema.numColumns else {
            throw ParquetError.invalidSchema(
                "Expected \(schema.numColumns) columns, got \(columns.count)"
            )
        }

        let counts = columns.map { $0.1.count }
        guard let numRows = counts.first else { return }
        guard counts.allSatisfy({ $0 == numRows }) else {
            throw ParquetError.invalidSchema("All columns must have the same number of rows")
        }

        let schemaCols = schema.columns
        var colWriters: [AnyColumnWriter] = []

        let colOpts = ColumnWriteOptions(
            compression: writeOptions.compression,
            useDictionary: writeOptions.useDictionary,
            enableStatistics: writeOptions.enableStatistics,
            dataPageVersion: writeOptions.dataPageVersion
        )

        for (i, (_, values)) in columns.enumerated() {
            let desc = ColumnDescriptor(node: schemaCols[i])
            switch values {
            case .strings(let vs):
                var w = ByteArrayColumnWriter(descriptor: desc, options: colOpts)
                w.write(values: vs.map { ByteArray($0) })
                colWriters.append(w)
            case .int32s(let vs):
                var w = ColumnWriter<Int32>(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            case .int64s(let vs):
                var w = ColumnWriter<Int64>(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            case .floats(let vs):
                var w = ColumnWriter<Float>(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            case .doubles(let vs):
                var w = ColumnWriter<Double>(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            case .booleans(let vs):
                var w = ColumnWriter<Bool>(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            case .byteArrays(let vs):
                var w = ByteArrayColumnWriter(descriptor: desc, options: colOpts)
                w.write(values: vs)
                colWriters.append(w)
            }
        }

        var rowGroupWriter = RowGroupWriter(
            schema: schema, numRows: Int64(numRows), columnWriters: colWriters
        )
        fileWriter.addRowGroup(&rowGroupWriter)
    }

    /// Write rows of nested data (for schemas with List/Map/Struct columns).
    /// Each row should be a .struct with fields matching the schema columns.
    public mutating func writeRows(_ rows: [ParquetRecord]) throws {
        guard !rows.isEmpty else { return }

        let shredded = DremelShredder.shred(rows: rows, schema: schema.root)
        let schemaCols = schema.columns

        guard shredded.count == schemaCols.count else {
            throw ParquetError.invalidSchema(
                "Shredded \(shredded.count) columns but schema has \(schemaCols.count)")
        }

        let colOpts = ColumnWriteOptions(
            compression: writeOptions.compression,
            useDictionary: writeOptions.useDictionary,
            enableStatistics: writeOptions.enableStatistics,
            dataPageVersion: writeOptions.dataPageVersion
        )

        var colWriters: [AnyColumnWriter] = []
        for (i, col) in shredded.enumerated() {
            let desc = ColumnDescriptor(node: schemaCols[i])
            switch col.physicalType {
            case .byteArray:
                var w = ByteArrayColumnWriter(descriptor: desc, options: colOpts)
                let byteValues = col.values.map { v -> ByteArray in
                    if case .string(let s) = v { return ByteArray(s) }
                    if case .bytes(let d) = v { return ByteArray(d) }
                    return ByteArray("")
                }
                w.writeWithLevels(values: byteValues, defLevels: col.defLevels, repLevels: col.repLevels)
                colWriters.append(w)
            case .int32:
                var w = ColumnWriter<Int32>(descriptor: desc, options: colOpts)
                let vals = col.values.map { v -> Int32 in
                    if case .int32(let n) = v { return n }
                    return 0
                }
                w.writeWithLevels(values: vals, defLevels: col.defLevels, repLevels: col.repLevels)
                colWriters.append(w)
            case .int64:
                var w = ColumnWriter<Int64>(descriptor: desc, options: colOpts)
                let vals = col.values.map { v -> Int64 in
                    if case .int64(let n) = v { return n }
                    return 0
                }
                w.writeWithLevels(values: vals, defLevels: col.defLevels, repLevels: col.repLevels)
                colWriters.append(w)
            case .double:
                var w = ColumnWriter<Double>(descriptor: desc, options: colOpts)
                let vals = col.values.map { v -> Double in
                    if case .double(let n) = v { return n }
                    return 0
                }
                w.writeWithLevels(values: vals, defLevels: col.defLevels, repLevels: col.repLevels)
                colWriters.append(w)
            case .float:
                var w = ColumnWriter<Float>(descriptor: desc, options: colOpts)
                let vals = col.values.map { v -> Float in
                    if case .float(let n) = v { return n }
                    return 0
                }
                w.writeWithLevels(values: vals, defLevels: col.defLevels, repLevels: col.repLevels)
                colWriters.append(w)
            case .boolean:
                var w = ColumnWriter<Bool>(descriptor: desc, options: colOpts)
                let vals = col.values.map { v -> Bool in
                    if case .bool(let b) = v { return b }
                    return false
                }
                w.writeWithLevels(values: vals, defLevels: col.defLevels, repLevels: col.repLevels)
                colWriters.append(w)
            default:
                var w = ByteArrayColumnWriter(descriptor: desc, options: colOpts)
                w.writeWithLevels(values: [], defLevels: col.defLevels, repLevels: col.repLevels)
                colWriters.append(w)
            }
        }

        var rowGroupWriter = RowGroupWriter(
            schema: schema, numRows: Int64(rows.count), columnWriters: colWriters
        )
        fileWriter.addRowGroup(&rowGroupWriter)
    }

    public mutating func close() throws {
        try fileWriter.write(to: path)
    }
}
