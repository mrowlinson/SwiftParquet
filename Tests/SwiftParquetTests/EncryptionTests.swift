import Testing
import Foundation
@testable import SwiftParquet

#if canImport(CryptoKit)
import CryptoKit
#else
import Crypto
#endif

// MARK: - AES-GCM Encryption Roundtrip

@Test func encryptDecryptRoundtrip() throws {
    let key = SymmetricKey(size: .bits128)
    let plaintext = Data("Hello, encrypted Parquet!".utf8)
    let aad = ParquetAESGCM.buildAAD(moduleType: .dataPage, rowGroupOrdinal: 0, columnOrdinal: 0, pageOrdinal: 0)

    let encrypted = try ParquetAESGCM.encrypt(plaintext, key: key, aad: aad)
    #expect(encrypted != plaintext)
    #expect(encrypted.count > plaintext.count) // nonce + tag overhead

    let decrypted = try ParquetAESGCM.decrypt(encrypted, key: key, aad: aad)
    #expect(decrypted == plaintext)
}

@Test func wrongKeyFails() throws {
    let key1 = SymmetricKey(size: .bits128)
    let key2 = SymmetricKey(size: .bits128)
    let plaintext = Data("Secret data".utf8)
    let aad = ParquetAESGCM.buildAAD(moduleType: .footer)

    let encrypted = try ParquetAESGCM.encrypt(plaintext, key: key1, aad: aad)

    #expect(throws: (any Error).self) {
        _ = try ParquetAESGCM.decrypt(encrypted, key: key2, aad: aad)
    }
}

// MARK: - Encrypted Parquet File Roundtrip

@Test func footerEncryptionDirect() throws {
    // Test the footer encryption/decryption directly as FileWriter does it
    let key = SymmetricKey(size: .bits128)
    let footerData = Data("simulated footer thrift bytes".utf8)
    let aad = ParquetAESGCM.buildAAD(moduleType: .footer)
    let encrypted = try ParquetAESGCM.encrypt(footerData, key: key, aad: aad)
    let decrypted = try ParquetAESGCM.decrypt(encrypted, key: key, aad: aad)
    #expect(decrypted == footerData)
}

@Test func fileWriterEncryption() throws {
    let key = SymmetricKey(data: Data(repeating: 0x42, count: 16))
    var builder = SchemaBuilder()
    builder.addColumn(name: "x", type: .int32)
    let schema = builder.build()

    var fw = FileWriter(schema: schema)
    fw.footerEncryptor = { footerBytes in
        let aad = ParquetAESGCM.buildAAD(moduleType: .footer)
        return try ParquetAESGCM.encrypt(footerBytes, key: key, aad: aad)
    }

    let desc = ColumnDescriptor(node: schema.columns[0])
    var cw = ColumnWriter<Int32>(descriptor: desc)
    cw.write(values: [1, 2, 3])
    var rgw = RowGroupWriter(schema: schema, numRows: 3, columnWriters: [cw])
    fw.addRowGroup(&rgw)

    let fileData = try fw.finalize()
    let headerMagic = fileData[fileData.startIndex..<(fileData.startIndex + 4)]
    #expect(headerMagic == Data([0x50, 0x41, 0x52, 0x45]))
}

@Test func writeAndReadEncrypted() throws {
    let path = NSTemporaryDirectory() + "encrypted-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let keyData = Data(repeating: 0x42, count: 16)
    let key = SymmetricKey(data: keyData)

    // Write using FileWriter directly (bypassing ParquetFileWriter)
    var builder = SchemaBuilder()
    builder.addColumn(name: "x", type: .int32)
    let schema = builder.build()

    var fw = FileWriter(schema: schema)
    fw.footerEncryptor = { footerBytes in
        let aad = ParquetAESGCM.buildAAD(moduleType: .footer)
        return try ParquetAESGCM.encrypt(footerBytes, key: SymmetricKey(data: Data(repeating: 0x42, count: 16)), aad: aad)
    }

    let desc = ColumnDescriptor(node: schema.columns[0])
    var cw = ColumnWriter<Int32>(descriptor: desc)
    cw.write(values: [1, 2, 3])
    var rgw = RowGroupWriter(schema: schema, numRows: 3, columnWriters: [cw])
    fw.addRowGroup(&rgw)
    try fw.write(to: path)

    // Verify PARE magic
    let fileData = try Data(contentsOf: URL(fileURLWithPath: path))
    let headerMagic = fileData[fileData.startIndex..<(fileData.startIndex + 4)]
    #expect(headerMagic == Data([0x50, 0x41, 0x52, 0x45]))

    // Read back with encryption config
    let enc = ParquetEncryptionConfig.uniformEncryption(key: key)
    let table = try ParquetFileReader.read(path: path, encryption: enc)
    #expect(table.numRows == 3)
    if case .int32s(let v) = table.column("x") { #expect(v == [1, 2, 3]) }
}

@Test func encryptedFileWithoutKeyFails() throws {
    let path = NSTemporaryDirectory() + "enc-fail-\(UUID().uuidString).parquet"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let key = SymmetricKey(size: .bits128)
    let enc = ParquetEncryptionConfig.uniformEncryption(key: key)

    var builder = SchemaBuilder()
    builder.addColumn(name: "x", type: .int32)
    let schema = builder.build()

    var writer = ParquetFileWriter(path: path, schema: schema, encryption: enc)
    try writer.writeRowGroup(columns: [("x", .int32s([1, 2, 3]))])
    try writer.close()

    // Reading without encryption key should fail
    #expect(throws: (any Error).self) {
        _ = try ParquetFileReader.read(path: path)
    }
}
