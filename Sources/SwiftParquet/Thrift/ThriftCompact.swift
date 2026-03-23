// ThriftCompact.swift — Hand-rolled TCompactProtocol writer
// Parquet uses Thrift compact binary encoding for all metadata.
//
// Protocol spec:
//   Field type IDs: BOOLEAN_TRUE=1, BOOLEAN_FALSE=2, I8=3, I16=4, I32=5, I64=6,
//                   DOUBLE=7, BINARY=8, LIST=9, SET=10, MAP=11, STRUCT=12
//   Field header: if 1 <= delta <= 15: (delta << 4) | typeID
//                 otherwise: typeID byte, then zigzag-i16 field ID as varint
//   Integers: zigzag varint (little-endian base-128)
//   Lists: if count <= 14: (count << 4) | elemType
//          else: 0xF0 | elemType, then varint count
//   Booleans: value encoded in field header type nibble (1=true, 2=false), no value byte

import Foundation

// MARK: - Compact Type IDs

enum TType: UInt8 {
    case boolTrue  = 1
    case boolFalse = 2
    case i8        = 3
    case i16       = 4
    case i32       = 5
    case i64       = 6
    case double    = 7
    case binary    = 8
    case list      = 9
    case set       = 10
    case map       = 11
    case `struct`  = 12
}

// MARK: - Writer

/// Writes Thrift compact-encoded structs to a Data buffer.
/// Usage:
///   var w = ThriftCompactWriter()
///   w.writeStruct(myValue)          // top-level struct
///   let bytes = w.buffer
struct ThriftCompactWriter {
    var buffer: Data = Data(capacity: 4096)

    // Stack of previous field IDs, one per struct nesting level.
    // Starts with one entry (for the top-level use context, though typically
    // callers drive beginStruct/endStruct themselves).
    private var fieldIDStack: [Int32] = [0]

    private var lastFieldID: Int32 {
        get { fieldIDStack.last! }
        set { fieldIDStack[fieldIDStack.count - 1] = newValue }
    }

    // MARK: - Struct management

    mutating func beginStruct() {
        fieldIDStack.append(0)
    }

    mutating func endStruct() {
        precondition(fieldIDStack.count > 1, "endStruct without matching beginStruct")
        fieldIDStack.removeLast()
    }

    mutating func writeFieldStop() {
        buffer.append(0x00)
    }

    // MARK: - Primitive varint helpers

    mutating func writeUVarint(_ value: UInt64) {
        var v = value
        while v >= 0x80 {
            buffer.append(UInt8((v & 0x7F) | 0x80))
            v >>= 7
        }
        buffer.append(UInt8(v))
    }

    private func zigzagI32(_ n: Int32) -> UInt64 {
        let u = UInt32(bitPattern: n)
        return UInt64((u &<< 1) ^ UInt32(bitPattern: n >> 31))
    }

    private func zigzagI64(_ n: Int64) -> UInt64 {
        let u = UInt64(bitPattern: n)
        return (u &<< 1) ^ UInt64(bitPattern: n >> 63)
    }

    private func zigzagI16(_ n: Int16) -> UInt64 {
        let u = UInt16(bitPattern: n)
        return UInt64((u &<< 1) ^ UInt16(bitPattern: n >> 15))
    }

    // MARK: - Field header

    mutating func writeFieldBegin(id: Int32, typeID: UInt8) {
        let delta = id - lastFieldID
        if delta >= 1 && delta <= 15 {
            buffer.append((UInt8(delta) << 4) | typeID)
        } else {
            buffer.append(typeID)
            writeUVarint(zigzagI16(Int16(id)))
        }
        lastFieldID = id
    }

    // MARK: - Typed field writers

    mutating func writeI32Field(id: Int32, value: Int32) {
        writeFieldBegin(id: id, typeID: TType.i32.rawValue)
        writeUVarint(zigzagI32(value))
    }

    mutating func writeI64Field(id: Int32, value: Int64) {
        writeFieldBegin(id: id, typeID: TType.i64.rawValue)
        writeUVarint(zigzagI64(value))
    }

    mutating func writeBoolField(id: Int32, value: Bool) {
        // Bool value is encoded in the field type nibble; no additional byte.
        let typeID: UInt8 = value ? TType.boolTrue.rawValue : TType.boolFalse.rawValue
        writeFieldBegin(id: id, typeID: typeID)
    }

    mutating func writeBinaryField(id: Int32, value: Data) {
        writeFieldBegin(id: id, typeID: TType.binary.rawValue)
        writeUVarint(UInt64(value.count))
        buffer.append(contentsOf: value)
    }

    mutating func writeStringField(id: Int32, value: String) {
        writeBinaryField(id: id, value: Data(value.utf8))
    }

    // MARK: - List helpers

    mutating func writeListBegin(id: Int32, elementType: UInt8, count: Int) {
        writeFieldBegin(id: id, typeID: TType.list.rawValue)
        writeListHeader(elementType: elementType, count: count)
    }

    // Write just the list header (type + count) without a field header.
    // Used when lists are nested inside other lists.
    mutating func writeListHeader(elementType: UInt8, count: Int) {
        if count <= 14 {
            buffer.append((UInt8(count) << 4) | elementType)
        } else {
            buffer.append(0xF0 | elementType)
            writeUVarint(UInt64(count))
        }
    }

    // Write list of i32 values (enums, encoding types, etc.)
    mutating func writeI32ListField(id: Int32, values: [Int32]) {
        writeListBegin(id: id, elementType: TType.i32.rawValue, count: values.count)
        for v in values {
            writeUVarint(zigzagI32(v))
        }
    }

    // Write list of strings (path components, etc.)
    mutating func writeStringListField(id: Int32, values: [String]) {
        writeListBegin(id: id, elementType: TType.binary.rawValue, count: values.count)
        for s in values {
            let bytes = Data(s.utf8)
            writeUVarint(UInt64(bytes.count))
            buffer.append(contentsOf: bytes)
        }
    }

    // MARK: - Struct helpers (for typed ThriftWritable values)

    /// Write a struct (any ThriftWritable) managing begin/stop/end.
    mutating func writeStruct<T: ThriftWritable>(_ value: T) {
        beginStruct()
        value.write(to: &self)
        writeFieldStop()
        endStruct()
    }

    /// Write a struct as a named field.
    mutating func writeStructField<T: ThriftWritable>(id: Int32, value: T) {
        writeFieldBegin(id: id, typeID: TType.struct.rawValue)
        writeStruct(value)
    }

    /// Write a list of structs as a named field.
    mutating func writeStructListField<T: ThriftWritable>(id: Int32, elements: [T]) {
        writeListBegin(id: id, elementType: TType.struct.rawValue, count: elements.count)
        for elem in elements {
            writeStruct(elem)
        }
    }

    /// Write a top-level struct and return the resulting bytes.
    static func serialize<T: ThriftWritable>(_ value: T) -> Data {
        var writer = ThriftCompactWriter()
        writer.writeStruct(value)
        return writer.buffer
    }
}

// MARK: - ThriftWritable Protocol

/// Conforming types write their Thrift fields (no begin/stop — the writer manages those).
protocol ThriftWritable {
    func write(to writer: inout ThriftCompactWriter)
}

// MARK: - ThriftCompactReader (Phase 2 — stubs for now)

/// Reads Thrift compact-encoded structs from a Data buffer.
struct ThriftCompactReader {
    private let data: Data
    private var offset: Int

    init(data: Data) {
        self.data = data
        self.offset = 0
    }

    var bytesRead: Int { offset }
    var remaining: Int { data.count - offset }

    private mutating func readByte() throws -> UInt8 {
        guard offset < data.count else { throw ParquetError.unexpectedEOF }
        let b = data[data.startIndex + offset]
        offset += 1
        return b
    }

    mutating func readUVarint() throws -> UInt64 {
        var result: UInt64 = 0
        var shift: UInt64 = 0
        while true {
            let b = try readByte()
            result |= UInt64(b & 0x7F) << shift
            if b & 0x80 == 0 { break }
            shift += 7
            guard shift < 64 else { throw ParquetError.thriftError("varint overflow") }
        }
        return result
    }

    mutating func readI32() throws -> Int32 {
        let n = try readUVarint()
        // zigzag decode
        return Int32(bitPattern: UInt32(n >> 1) ^ UInt32(bitPattern: -(Int32(n & 1))))
    }

    mutating func readI64() throws -> Int64 {
        let n = try readUVarint()
        return Int64(bitPattern: (n >> 1) ^ UInt64(bitPattern: -(Int64(n & 1))))
    }

    mutating func readI16() throws -> Int16 {
        let n = try readUVarint()
        return Int16(bitPattern: UInt16(n >> 1) ^ UInt16(bitPattern: -(Int16(n & 1))))
    }

    mutating func readBinary() throws -> Data {
        let length = try readUVarint()
        guard length <= UInt64(remaining) else { throw ParquetError.unexpectedEOF }
        let start = data.startIndex + offset
        let end = start + Int(length)
        offset += Int(length)
        return data[start..<end]
    }

    mutating func readString() throws -> String {
        let bytes = try readBinary()
        guard let s = String(data: bytes, encoding: .utf8) else {
            throw ParquetError.thriftError("invalid UTF-8 string")
        }
        return s
    }

    // Read field header; returns (delta, typeID) or nil if STOP
    mutating func readFieldHeader(previousFieldID: Int32) throws -> (fieldID: Int32, typeID: UInt8)? {
        let byte = try readByte()
        if byte == 0x00 { return nil }  // STOP

        let typeID = byte & 0x0F
        let delta = Int32(byte >> 4)

        if delta != 0 {
            return (previousFieldID + delta, typeID)
        } else {
            // Long form: next bytes are zigzag i16 field ID
            let fieldID = Int32(try readI16())
            return (fieldID, typeID)
        }
    }

    // Read list header; returns (elementTypeID, count)
    mutating func readListHeader() throws -> (elementType: UInt8, count: Int) {
        let byte = try readByte()
        let elementType = byte & 0x0F
        let shortCount = Int(byte >> 4)
        if shortCount != 0x0F {
            return (elementType, shortCount)
        } else {
            let count = try readUVarint()
            return (elementType, Int(count))
        }
    }

    mutating func skip(typeID: UInt8) throws {
        switch typeID {
        case TType.boolTrue.rawValue, TType.boolFalse.rawValue:
            break  // bool value is in the field header
        case TType.i8.rawValue:
            _ = try readByte()
        case TType.i16.rawValue:
            _ = try readI16()
        case TType.i32.rawValue:
            _ = try readI32()
        case TType.i64.rawValue:
            _ = try readI64()
        case TType.double.rawValue:
            guard offset + 8 <= data.count else { throw ParquetError.unexpectedEOF }
            offset += 8
        case TType.binary.rawValue:
            _ = try readBinary()
        case TType.list.rawValue, TType.set.rawValue:
            let (elemType, count) = try readListHeader()
            for _ in 0..<count { try skip(typeID: elemType) }
        case TType.struct.rawValue:
            var prevField: Int32 = 0
            while true {
                guard let (fid, ftid) = try readFieldHeader(previousFieldID: prevField) else { break }
                prevField = fid
                try skip(typeID: ftid)
            }
        default:
            throw ParquetError.thriftError("unknown Thrift type ID \(typeID) in skip")
        }
    }
}
