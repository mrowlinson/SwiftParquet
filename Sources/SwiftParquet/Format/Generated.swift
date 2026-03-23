// Generated.swift — Swift structs matching parquet.thrift definitions
// Field IDs match the Thrift IDL exactly (required for correct serialization).
// Reference: github.com/apache/arrow-go/parquet/internal/gen-go/parquet/parquet.go

import Foundation

// MARK: - FileMetaData (thrift field IDs: version=1, schema=2, num_rows=3, row_groups=4, created_by=6)

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
        if let t = type {
            writer.writeI32Field(id: 1, value: t.rawValue)
        }
        if let tl = typeLength {
            writer.writeI32Field(id: 2, value: tl)
        }
        if let rt = repetitionType {
            writer.writeI32Field(id: 3, value: rt.rawValue)
        }
        writer.writeStringField(id: 4, value: name)
        if let nc = numChildren {
            writer.writeI32Field(id: 5, value: nc)
        }
        if let ct = convertedType {
            writer.writeI32Field(id: 6, value: ct.rawValue)
        }
        if let lt = logicalType {
            writer.writeStructField(id: 10, value: lt)
        }
    }
}

// MARK: - LogicalType (union — only one field set at a time)

public struct LogicalTypeThrift {
    public enum Kind {
        case string   // field 1 — StringType (empty struct)
        case integer(bitWidth: Int8, isSigned: Bool)  // field 10 — IntType
    }
    public var kind: Kind

    public init(kind: Kind) {
        self.kind = kind
    }
}

extension LogicalTypeThrift: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        switch kind {
        case .string:
            writer.writeStructField(id: 1, value: EmptyThriftStruct())
        case .integer(let bitWidth, let isSigned):
            writer.writeStructField(id: 10, value: IntTypeThrift(bitWidth: bitWidth, isSigned: isSigned))
        }
    }
}

struct EmptyThriftStruct: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {}
}

struct IntTypeThrift: ThriftWritable {
    var bitWidth: Int8
    var isSigned: Bool
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeI32Field(id: 1, value: Int32(bitWidth))
        writer.writeBoolField(id: 2, value: isSigned)
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

// MARK: - ColumnChunk (field IDs: file_offset=2, meta_data=3)

struct ColumnChunk {
    var fileOffset: Int64
    var metaData: ColumnMetaData
}

extension ColumnChunk: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeI64Field(id: 2, value: fileOffset)
        writer.writeStructField(id: 3, value: metaData)
    }
}

// MARK: - ColumnMetaData (field IDs: type=1, encodings=2, path_in_schema=3, codec=4,
//                                    num_values=5, total_uncompressed_size=6,
//                                    total_compressed_size=7, data_page_offset=9,
//                                    dictionary_page_offset=11)

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
        if let dpo = dictionaryPageOffset {
            writer.writeI64Field(id: 11, value: dpo)
        }
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
}

extension PageHeader: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeI32Field(id: 1, value: type.rawValue)
        writer.writeI32Field(id: 2, value: uncompressedPageSize)
        writer.writeI32Field(id: 3, value: compressedPageSize)
        if let dph = dataPageHeader {
            writer.writeStructField(id: 5, value: dph)
        }
        if let dicph = dictionaryPageHeader {
            writer.writeStructField(id: 7, value: dicph)
        }
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

// MARK: - KeyValue (field IDs: key=1, value=2)

struct KeyValue {
    var key: String
    var value: String?
}

extension KeyValue: ThriftWritable {
    func write(to writer: inout ThriftCompactWriter) {
        writer.writeStringField(id: 1, value: key)
        if let v = value {
            writer.writeStringField(id: 2, value: v)
        }
    }
}
