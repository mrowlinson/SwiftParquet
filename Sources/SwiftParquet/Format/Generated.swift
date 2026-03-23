// Generated.swift — Swift structs matching parquet.thrift definitions
// Field IDs match the Thrift IDL exactly (required for correct serialization).
// Reference: github.com/apache/arrow-go/parquet/internal/gen-go/parquet/parquet.go

import Foundation

// MARK: - FileMetaData (field IDs: version=1, schema=2, num_rows=3, row_groups=4, key_value_metadata=5, created_by=6)

struct FileMetaData {
    var version: Int32
    var schema: [SchemaElement]
    var numRows: Int64
    var rowGroups: [RowGroup]
    var createdBy: String?
    var keyValueMetadata: [KeyValue]?
}

extension FileMetaData: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeI32Field(id: 1, value: version)
        writer.writeStructListField(id: 2, elements: schema)
        writer.writeI64Field(id: 3, value: numRows)
        writer.writeStructListField(id: 4, elements: rowGroups)
        if let kvm = keyValueMetadata, !kvm.isEmpty {
            writer.writeStructListField(id: 5, elements: kvm)
        }
        if let cb = createdBy {
            writer.writeStringField(id: 6, value: cb)
        }
    }
}

extension FileMetaData: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> FileMetaData {
        var version: Int32 = 0
        var schema = [SchemaElement]()
        var numRows: Int64 = 0
        var rowGroups = [RowGroup]()
        var createdBy: String? = nil
        var keyValueMetadata: [KeyValue]? = nil

        var prevFieldID: Int32 = 0
        while let (fieldID, typeID) = try reader.readFieldHeader(previousFieldID: prevFieldID) {
            prevFieldID = fieldID
            switch fieldID {
            case 1: version = try reader.readI32()
            case 2: schema = try reader.readStructList()
            case 3: numRows = try reader.readI64()
            case 4: rowGroups = try reader.readStructList()
            case 5: keyValueMetadata = try reader.readStructList()
            case 6: createdBy = try reader.readString()
            default: try reader.skip(typeID: typeID)
            }
        }

        return FileMetaData(
            version: version, schema: schema, numRows: numRows,
            rowGroups: rowGroups, createdBy: createdBy, keyValueMetadata: keyValueMetadata
        )
    }
}

// MARK: - SchemaElement (field IDs: type=1, type_length=2, repetition_type=3, name=4,
//                                   num_children=5, converted_type=6, logical_type=10)

struct SchemaElement {
    var type: PhysicalType?
    var typeLength: Int32?
    var repetitionType: Repetition?
    var name: String
    var numChildren: Int32?
    var convertedType: ConvertedType?
    var logicalType: LogicalTypeThrift?
}

extension SchemaElement: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        if let t = type { writer.writeI32Field(id: 1, value: t.rawValue) }
        if let tl = typeLength { writer.writeI32Field(id: 2, value: tl) }
        if let rt = repetitionType { writer.writeI32Field(id: 3, value: rt.rawValue) }
        writer.writeStringField(id: 4, value: name)
        if let nc = numChildren { writer.writeI32Field(id: 5, value: nc) }
        if let ct = convertedType { writer.writeI32Field(id: 6, value: ct.rawValue) }
        if let lt = logicalType { writer.writeStructField(id: 10, value: lt) }
    }
}

extension SchemaElement: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> SchemaElement {
        var type: PhysicalType? = nil
        var typeLength: Int32? = nil
        var repetitionType: Repetition? = nil
        var name = ""
        var numChildren: Int32? = nil
        var convertedType: ConvertedType? = nil
        var logicalType: LogicalTypeThrift? = nil

        var prevFieldID: Int32 = 0
        while let (fieldID, typeID) = try reader.readFieldHeader(previousFieldID: prevFieldID) {
            prevFieldID = fieldID
            switch fieldID {
            case 1: type = PhysicalType(rawValue: try reader.readI32())
            case 2: typeLength = try reader.readI32()
            case 3: repetitionType = Repetition(rawValue: try reader.readI32())
            case 4: name = try reader.readString()
            case 5: numChildren = try reader.readI32()
            case 6: convertedType = ConvertedType(rawValue: try reader.readI32())
            case 10: logicalType = try reader.readStruct()
            default: try reader.skip(typeID: typeID)
            }
        }

        return SchemaElement(
            type: type, typeLength: typeLength, repetitionType: repetitionType,
            name: name, numChildren: numChildren, convertedType: convertedType, logicalType: logicalType
        )
    }
}

// MARK: - LogicalType (union — only one field set at a time)

public struct LogicalTypeThrift {
    public enum Kind {
        case string            // field 1 — StringType (empty struct)
        case integer(bitWidth: Int8, isSigned: Bool)  // field 10 — IntType
        case date              // field 2
        case time(isAdjustedToUTC: Bool, unit: TimeUnit) // field 3
        case timestamp(isAdjustedToUTC: Bool, unit: TimeUnit) // field 4
        case decimal(scale: Int32, precision: Int32) // field 6
        case unknown           // field 13
    }

    public enum TimeUnit {
        case millis, micros, nanos
    }

    public var kind: Kind

    public init(kind: Kind) { self.kind = kind }
}

extension LogicalTypeThrift: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        switch kind {
        case .string:
            writer.writeStructField(id: 1, value: EmptyThriftStruct())
        case .integer(let bitWidth, let isSigned):
            writer.writeStructField(id: 10, value: IntTypeThrift(bitWidth: bitWidth, isSigned: isSigned))
        case .date:
            writer.writeStructField(id: 2, value: EmptyThriftStruct())
        case .time(let adj, let unit):
            writer.writeStructField(id: 3, value: TimeTypeThrift(isAdjustedToUTC: adj, unit: unit))
        case .timestamp(let adj, let unit):
            writer.writeStructField(id: 4, value: TimestampTypeThrift(isAdjustedToUTC: adj, unit: unit))
        case .decimal(let scale, let precision):
            writer.writeStructField(id: 6, value: DecimalTypeThrift(scale: scale, precision: precision))
        case .unknown:
            writer.writeStructField(id: 13, value: EmptyThriftStruct())
        }
    }
}

extension LogicalTypeThrift: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> LogicalTypeThrift {
        var kind: Kind = .unknown
        var prevFieldID: Int32 = 0
        while let (fieldID, typeID) = try reader.readFieldHeader(previousFieldID: prevFieldID) {
            prevFieldID = fieldID
            switch fieldID {
            case 1: try reader.skip(typeID: typeID); kind = .string
            case 2: try reader.skip(typeID: typeID); kind = .date
            case 3:
                let tt: TimeTypeThrift = try reader.readStruct()
                kind = .time(isAdjustedToUTC: tt.isAdjustedToUTC, unit: tt.unit)
            case 4:
                let tt: TimestampTypeThrift = try reader.readStruct()
                kind = .timestamp(isAdjustedToUTC: tt.isAdjustedToUTC, unit: tt.unit)
            case 6:
                let dt: DecimalTypeThrift = try reader.readStruct()
                kind = .decimal(scale: dt.scale, precision: dt.precision)
            case 10:
                let it: IntTypeThrift = try reader.readStruct()
                kind = .integer(bitWidth: it.bitWidth, isSigned: it.isSigned)
            case 13: try reader.skip(typeID: typeID); kind = .unknown
            default: try reader.skip(typeID: typeID)
            }
        }
        return LogicalTypeThrift(kind: kind)
    }
}

struct EmptyThriftStruct: ThriftWritable { func write(to writer: inout ThriftCompactWriter) {} }

struct IntTypeThrift: ThriftWritable, ThriftReadable {
    var bitWidth: Int8; var isSigned: Bool
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeI32Field(id: 1, value: Int32(bitWidth))
        writer.writeBoolField(id: 2, value: isSigned)
    }
    static func read(from reader: inout ThriftCompactReader) throws -> IntTypeThrift {
        var bitWidth: Int8 = 0; var isSigned = false
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: bitWidth = Int8(try reader.readI32())
            case 2: isSigned = (tid == TType.boolTrue.rawValue)
            default: try reader.skip(typeID: tid)
            }
        }
        return IntTypeThrift(bitWidth: bitWidth, isSigned: isSigned)
    }
}

struct TimeTypeThrift: ThriftWritable, ThriftReadable {
    var isAdjustedToUTC: Bool; var unit: LogicalTypeThrift.TimeUnit
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeBoolField(id: 1, value: isAdjustedToUTC)
        switch unit {
        case .millis: writer.writeStructField(id: 2, value: EmptyThriftStruct())
        case .micros: writer.writeStructField(id: 3, value: EmptyThriftStruct())
        case .nanos: writer.writeStructField(id: 4, value: EmptyThriftStruct())
        }
    }
    static func read(from reader: inout ThriftCompactReader) throws -> TimeTypeThrift {
        var adj = false; var unit: LogicalTypeThrift.TimeUnit = .millis
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: adj = (tid == TType.boolTrue.rawValue)
            case 2: try reader.skip(typeID: tid); unit = .millis
            case 3: try reader.skip(typeID: tid); unit = .micros
            case 4: try reader.skip(typeID: tid); unit = .nanos
            default: try reader.skip(typeID: tid)
            }
        }
        return TimeTypeThrift(isAdjustedToUTC: adj, unit: unit)
    }
}

struct TimestampTypeThrift: ThriftWritable, ThriftReadable {
    var isAdjustedToUTC: Bool; var unit: LogicalTypeThrift.TimeUnit
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeBoolField(id: 1, value: isAdjustedToUTC)
        switch unit {
        case .millis: writer.writeStructField(id: 2, value: EmptyThriftStruct())
        case .micros: writer.writeStructField(id: 3, value: EmptyThriftStruct())
        case .nanos: writer.writeStructField(id: 4, value: EmptyThriftStruct())
        }
    }
    static func read(from reader: inout ThriftCompactReader) throws -> TimestampTypeThrift {
        var adj = false; var unit: LogicalTypeThrift.TimeUnit = .millis
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: adj = (tid == TType.boolTrue.rawValue)
            case 2: try reader.skip(typeID: tid); unit = .millis
            case 3: try reader.skip(typeID: tid); unit = .micros
            case 4: try reader.skip(typeID: tid); unit = .nanos
            default: try reader.skip(typeID: tid)
            }
        }
        return TimestampTypeThrift(isAdjustedToUTC: adj, unit: unit)
    }
}

struct DecimalTypeThrift: ThriftWritable, ThriftReadable {
    var scale: Int32; var precision: Int32
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeI32Field(id: 1, value: scale)
        writer.writeI32Field(id: 2, value: precision)
    }
    static func read(from reader: inout ThriftCompactReader) throws -> DecimalTypeThrift {
        var scale: Int32 = 0; var precision: Int32 = 0
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: scale = try reader.readI32()
            case 2: precision = try reader.readI32()
            default: try reader.skip(typeID: tid)
            }
        }
        return DecimalTypeThrift(scale: scale, precision: precision)
    }
}

// MARK: - RowGroup (field IDs: columns=1, total_byte_size=2, num_rows=3)

struct RowGroup {
    var columns: [ColumnChunk]
    var totalByteSize: Int64
    var numRows: Int64
}

extension RowGroup: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeStructListField(id: 1, elements: columns)
        writer.writeI64Field(id: 2, value: totalByteSize)
        writer.writeI64Field(id: 3, value: numRows)
    }
}

extension RowGroup: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> RowGroup {
        var columns = [ColumnChunk]()
        var totalByteSize: Int64 = 0
        var numRows: Int64 = 0
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: columns = try reader.readStructList()
            case 2: totalByteSize = try reader.readI64()
            case 3: numRows = try reader.readI64()
            default: try reader.skip(typeID: tid)
            }
        }
        return RowGroup(columns: columns, totalByteSize: totalByteSize, numRows: numRows)
    }
}

// MARK: - ColumnChunk (field IDs: file_path=1, file_offset=2, meta_data=3)

struct ColumnChunk {
    var filePath: String?
    var fileOffset: Int64
    var metaData: ColumnMetaData
}

extension ColumnChunk: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        if let fp = filePath { writer.writeStringField(id: 1, value: fp) }
        writer.writeI64Field(id: 2, value: fileOffset)
        writer.writeStructField(id: 3, value: metaData)
    }
}

extension ColumnChunk: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> ColumnChunk {
        var filePath: String? = nil
        var fileOffset: Int64 = 0
        var metaData = ColumnMetaData(type: .int32, encodings: [], pathInSchema: [],
                                       codec: .uncompressed, numValues: 0,
                                       totalUncompressedSize: 0, totalCompressedSize: 0,
                                       dataPageOffset: 0)
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: filePath = try reader.readString()
            case 2: fileOffset = try reader.readI64()
            case 3: metaData = try reader.readStruct()
            default: try reader.skip(typeID: tid)
            }
        }
        return ColumnChunk(filePath: filePath, fileOffset: fileOffset, metaData: metaData)
    }
}

// MARK: - ColumnMetaData (field IDs: type=1, encodings=2, path_in_schema=3, codec=4,
//                         num_values=5, total_uncompressed_size=6, total_compressed_size=7,
//                         data_page_offset=9, dictionary_page_offset=11, statistics=12)

struct ColumnMetaData {
    var type: PhysicalType
    var encodings: [Encoding]
    var pathInSchema: [String]
    var codec: CompressionCodec
    var numValues: Int64
    var totalUncompressedSize: Int64
    var totalCompressedSize: Int64
    var dataPageOffset: Int64
    var dictionaryPageOffset: Int64?
    var statistics: Statistics?
}

extension ColumnMetaData: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeI32Field(id: 1, value: type.rawValue)
        writer.writeI32ListField(id: 2, values: encodings.map { Int32($0.rawValue) })
        writer.writeStringListField(id: 3, values: pathInSchema)
        writer.writeI32Field(id: 4, value: codec.rawValue)
        writer.writeI64Field(id: 5, value: numValues)
        writer.writeI64Field(id: 6, value: totalUncompressedSize)
        writer.writeI64Field(id: 7, value: totalCompressedSize)
        writer.writeI64Field(id: 9, value: dataPageOffset)
        if let dpo = dictionaryPageOffset { writer.writeI64Field(id: 11, value: dpo) }
        if let s = statistics { writer.writeStructField(id: 12, value: s) }
    }
}

extension ColumnMetaData: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> ColumnMetaData {
        var type: PhysicalType = .int32
        var encodings = [Encoding]()
        var pathInSchema = [String]()
        var codec: CompressionCodec = .uncompressed
        var numValues: Int64 = 0
        var totalUncompressedSize: Int64 = 0
        var totalCompressedSize: Int64 = 0
        var dataPageOffset: Int64 = 0
        var dictionaryPageOffset: Int64? = nil
        var statistics: Statistics? = nil

        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: type = PhysicalType(rawValue: try reader.readI32()) ?? .int32
            case 2:
                let rawEncodings = try reader.readI32List()
                encodings = rawEncodings.compactMap { Encoding(rawValue: $0) }
            case 3: pathInSchema = try reader.readStringList()
            case 4: codec = CompressionCodec(rawValue: try reader.readI32()) ?? .uncompressed
            case 5: numValues = try reader.readI64()
            case 6: totalUncompressedSize = try reader.readI64()
            case 7: totalCompressedSize = try reader.readI64()
            case 9: dataPageOffset = try reader.readI64()
            case 11: dictionaryPageOffset = try reader.readI64()
            case 12: statistics = try reader.readStruct()
            default: try reader.skip(typeID: tid)
            }
        }

        return ColumnMetaData(
            type: type, encodings: encodings, pathInSchema: pathInSchema,
            codec: codec, numValues: numValues,
            totalUncompressedSize: totalUncompressedSize,
            totalCompressedSize: totalCompressedSize,
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: dictionaryPageOffset,
            statistics: statistics
        )
    }
}

// MARK: - Statistics (field IDs: max=1, min=2, null_count=3, distinct_count=4, max_value=5, min_value=6)

struct Statistics {
    var max: Data?
    var min: Data?
    var nullCount: Int64?
    var distinctCount: Int64?
    var maxValue: Data?
    var minValue: Data?
}

extension Statistics: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        if let m = max { writer.writeBinaryField(id: 1, value: m) }
        if let m = min { writer.writeBinaryField(id: 2, value: m) }
        if let nc = nullCount { writer.writeI64Field(id: 3, value: nc) }
        if let dc = distinctCount { writer.writeI64Field(id: 4, value: dc) }
        if let mv = maxValue { writer.writeBinaryField(id: 5, value: mv) }
        if let mv = minValue { writer.writeBinaryField(id: 6, value: mv) }
    }
}

extension Statistics: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> Statistics {
        var max: Data? = nil, min: Data? = nil
        var nullCount: Int64? = nil, distinctCount: Int64? = nil
        var maxValue: Data? = nil, minValue: Data? = nil
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: max = try reader.readBinary()
            case 2: min = try reader.readBinary()
            case 3: nullCount = try reader.readI64()
            case 4: distinctCount = try reader.readI64()
            case 5: maxValue = try reader.readBinary()
            case 6: minValue = try reader.readBinary()
            default: try reader.skip(typeID: tid)
            }
        }
        return Statistics(max: max, min: min, nullCount: nullCount,
                         distinctCount: distinctCount, maxValue: maxValue, minValue: minValue)
    }
}

// MARK: - PageHeader (field IDs: type=1, uncompressed_page_size=2, compressed_page_size=3,
//                                data_page_header=5, dictionary_page_header=7)

struct PageHeader {
    var type: PageType
    var uncompressedPageSize: Int32
    var compressedPageSize: Int32
    var dataPageHeader: DataPageHeader?
    var dictionaryPageHeader: DictionaryPageHeader?
    var dataPageHeaderV2: DataPageHeaderV2?
}

extension PageHeader: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeI32Field(id: 1, value: type.rawValue)
        writer.writeI32Field(id: 2, value: uncompressedPageSize)
        writer.writeI32Field(id: 3, value: compressedPageSize)
        if let dph = dataPageHeader { writer.writeStructField(id: 5, value: dph) }
        if let dicph = dictionaryPageHeader { writer.writeStructField(id: 7, value: dicph) }
        if let v2 = dataPageHeaderV2 { writer.writeStructField(id: 8, value: v2) }
    }
}

extension PageHeader: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> PageHeader {
        var type: PageType = .dataPage
        var uncompressedPageSize: Int32 = 0
        var compressedPageSize: Int32 = 0
        var dataPageHeader: DataPageHeader? = nil
        var dictionaryPageHeader: DictionaryPageHeader? = nil
        var dataPageHeaderV2: DataPageHeaderV2? = nil
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: type = PageType(rawValue: try reader.readI32()) ?? .dataPage
            case 2: uncompressedPageSize = try reader.readI32()
            case 3: compressedPageSize = try reader.readI32()
            case 5: dataPageHeader = try reader.readStruct()
            case 7: dictionaryPageHeader = try reader.readStruct()
            case 8: dataPageHeaderV2 = try reader.readStruct()
            default: try reader.skip(typeID: tid)
            }
        }
        return PageHeader(type: type, uncompressedPageSize: uncompressedPageSize,
                         compressedPageSize: compressedPageSize,
                         dataPageHeader: dataPageHeader,
                         dictionaryPageHeader: dictionaryPageHeader,
                         dataPageHeaderV2: dataPageHeaderV2)
    }
}

// MARK: - DataPageHeader (field IDs: num_values=1, encoding=2,
//                                    definition_level_encoding=3, repetition_level_encoding=4)

struct DataPageHeader {
    var numValues: Int32
    var encoding: Encoding
    var definitionLevelEncoding: Encoding
    var repetitionLevelEncoding: Encoding
}

extension DataPageHeader: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeI32Field(id: 1, value: numValues)
        writer.writeI32Field(id: 2, value: encoding.rawValue)
        writer.writeI32Field(id: 3, value: definitionLevelEncoding.rawValue)
        writer.writeI32Field(id: 4, value: repetitionLevelEncoding.rawValue)
    }
}

extension DataPageHeader: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> DataPageHeader {
        var numValues: Int32 = 0
        var encoding: Encoding = .plain
        var defEnc: Encoding = .rle
        var repEnc: Encoding = .rle
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: numValues = try reader.readI32()
            case 2: encoding = Encoding(rawValue: try reader.readI32()) ?? .plain
            case 3: defEnc = Encoding(rawValue: try reader.readI32()) ?? .rle
            case 4: repEnc = Encoding(rawValue: try reader.readI32()) ?? .rle
            default: try reader.skip(typeID: tid)
            }
        }
        return DataPageHeader(numValues: numValues, encoding: encoding,
                             definitionLevelEncoding: defEnc, repetitionLevelEncoding: repEnc)
    }
}

// MARK: - DataPageHeaderV2 (field IDs: num_values=1, num_nulls=2, num_rows=3, encoding=4,
//                            definition_levels_byte_length=5, repetition_levels_byte_length=6,
//                            is_compressed=7, statistics=8)

struct DataPageHeaderV2 {
    var numValues: Int32
    var numNulls: Int32
    var numRows: Int32
    var encoding: Encoding
    var definitionLevelsByteLength: Int32
    var repetitionLevelsByteLength: Int32
    var isCompressed: Bool
    var statistics: Statistics?
}

extension DataPageHeaderV2: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeI32Field(id: 1, value: numValues)
        writer.writeI32Field(id: 2, value: numNulls)
        writer.writeI32Field(id: 3, value: numRows)
        writer.writeI32Field(id: 4, value: encoding.rawValue)
        writer.writeI32Field(id: 5, value: definitionLevelsByteLength)
        writer.writeI32Field(id: 6, value: repetitionLevelsByteLength)
        if !isCompressed { writer.writeBoolField(id: 7, value: false) }
        if let s = statistics { writer.writeStructField(id: 8, value: s) }
    }
}

extension DataPageHeaderV2: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> DataPageHeaderV2 {
        var numValues: Int32 = 0, numNulls: Int32 = 0, numRows: Int32 = 0
        var encoding: Encoding = .plain
        var defByteLen: Int32 = 0, repByteLen: Int32 = 0
        var isCompressed = true
        var statistics: Statistics? = nil
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: numValues = try reader.readI32()
            case 2: numNulls = try reader.readI32()
            case 3: numRows = try reader.readI32()
            case 4: encoding = Encoding(rawValue: try reader.readI32()) ?? .plain
            case 5: defByteLen = try reader.readI32()
            case 6: repByteLen = try reader.readI32()
            case 7: isCompressed = (tid == TType.boolTrue.rawValue)
            case 8: statistics = try reader.readStruct()
            default: try reader.skip(typeID: tid)
            }
        }
        return DataPageHeaderV2(
            numValues: numValues, numNulls: numNulls, numRows: numRows,
            encoding: encoding, definitionLevelsByteLength: defByteLen,
            repetitionLevelsByteLength: repByteLen, isCompressed: isCompressed,
            statistics: statistics
        )
    }
}

// MARK: - DictionaryPageHeader (field IDs: num_values=1, encoding=2)

struct DictionaryPageHeader {
    var numValues: Int32
    var encoding: Encoding
}

extension DictionaryPageHeader: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeI32Field(id: 1, value: numValues)
        writer.writeI32Field(id: 2, value: encoding.rawValue)
    }
}

extension DictionaryPageHeader: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> DictionaryPageHeader {
        var numValues: Int32 = 0
        var encoding: Encoding = .plainDictionary
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: numValues = try reader.readI32()
            case 2: encoding = Encoding(rawValue: try reader.readI32()) ?? .plainDictionary
            default: try reader.skip(typeID: tid)
            }
        }
        return DictionaryPageHeader(numValues: numValues, encoding: encoding)
    }
}

// MARK: - KeyValue (field IDs: key=1, value=2)

struct KeyValue {
    var key: String
    var value: String?
}

extension KeyValue: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeStringField(id: 1, value: key)
        if let v = value { writer.writeStringField(id: 2, value: v) }
    }
}

extension KeyValue: ThriftReadable {
    static func read(from reader: inout ThriftCompactReader) throws -> KeyValue {
        var key = ""; var value: String? = nil
        var prev: Int32 = 0
        while let (fid, tid) = try reader.readFieldHeader(previousFieldID: prev) {
            prev = fid
            switch fid {
            case 1: key = try reader.readString()
            case 2: value = try reader.readString()
            default: try reader.skip(typeID: tid)
            }
        }
        return KeyValue(key: key, value: value)
    }
}
