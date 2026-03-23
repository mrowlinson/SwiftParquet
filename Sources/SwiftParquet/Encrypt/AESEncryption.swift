// AESEncryption.swift — AES-GCM encryption for Parquet column/footer encryption
// Reference: https://github.com/apache/parquet-format/blob/master/Encryption.md
//
// Parquet encryption uses AES-GCM-128/192/256 for encrypting:
//   - Column data (page data)
//   - Column metadata (ColumnMetaData Thrift)
//   - Footer (FileMetaData Thrift)
//
// Uses CryptoKit (Apple) or swift-crypto (Linux) for AES-GCM operations.

import Foundation

#if canImport(CryptoKit)
import CryptoKit
#else
import Crypto
#endif

// MARK: - Encryption Configuration

public struct ParquetEncryptionConfig: Sendable {
    public let footerKey: SymmetricKey?
    public let columnKeys: [String: SymmetricKey]  // column path → key
    public let plaintextFooter: Bool  // if true, footer is not encrypted (but columns can be)

    public init(
        footerKey: SymmetricKey? = nil,
        columnKeys: [String: SymmetricKey] = [:],
        plaintextFooter: Bool = false
    ) {
        self.footerKey = footerKey
        self.columnKeys = columnKeys
        self.plaintextFooter = plaintextFooter
    }

    /// Create with a single key for all columns and footer.
    public static func uniformEncryption(key: SymmetricKey) -> ParquetEncryptionConfig {
        ParquetEncryptionConfig(footerKey: key, columnKeys: [:], plaintextFooter: false)
    }
}

// MARK: - AES-GCM Module

/// Parquet AES-GCM module format:
///   [4 bytes] nonce length (little-endian)
///   [N bytes] nonce (12 bytes for GCM)
///   [M bytes] ciphertext + GCM tag (16 bytes appended)
///
/// AAD (Additional Authenticated Data) includes module type + column ordinal.
struct ParquetAESGCM {

    enum ModuleType: UInt8 {
        case footer = 0
        case columnMetaData = 1
        case dataPage = 2
        case dictionaryPage = 3
        case dataPageHeader = 4
        case dictionaryPageHeader = 5
        case columnIndex = 6
        case offsetIndex = 7
        case bloomFilterHeader = 8
        case bloomFilterBitset = 9
    }

    /// Encrypt data with AES-GCM.
    static func encrypt(
        _ plaintext: Data,
        key: SymmetricKey,
        aad: Data = Data(),
        nonce: AES.GCM.Nonce? = nil
    ) throws -> Data {
        let actualNonce = try nonce ?? AES.GCM.Nonce()
        let sealed = try AES.GCM.seal(plaintext, using: key, nonce: actualNonce, authenticating: aad)

        // Parquet format: [4-byte nonce length LE] [nonce] [ciphertext + tag]
        var result = Data()
        let nonceData = actualNonce.withUnsafeBytes { Data($0) }
        let nonceLen = UInt32(nonceData.count)
        withUnsafeBytes(of: nonceLen.littleEndian) { result.append(contentsOf: $0) }
        result.append(nonceData)
        result.append(sealed.ciphertext)
        result.append(sealed.tag)
        return result
    }

    /// Decrypt data with AES-GCM.
    static func decrypt(
        _ ciphertext: Data,
        key: SymmetricKey,
        aad: Data = Data()
    ) throws -> Data {
        guard ciphertext.count >= 4 else {
            throw ParquetError.corruptedFile("encrypted module too short (\(ciphertext.count) bytes)")
        }


        let base = ciphertext.startIndex
        let nonceLen = UInt32(ciphertext[base]) |
                       (UInt32(ciphertext[base + 1]) << 8) |
                       (UInt32(ciphertext[base + 2]) << 16) |
                       (UInt32(ciphertext[base + 3]) << 24)

        let nonceEnd = 4 + Int(nonceLen)
        guard nonceEnd + 16 <= ciphertext.count else {
            throw ParquetError.corruptedFile("encrypted module truncated (count=\(ciphertext.count), nonceLen=\(nonceLen), need=\(nonceEnd + 16))")
        }

        let nonceData = ciphertext[(base + 4)..<(base + nonceEnd)]
        let nonce = try AES.GCM.Nonce(data: nonceData)

        let tagStart = ciphertext.endIndex - 16
        let tag = ciphertext[tagStart..<ciphertext.endIndex]
        let encryptedContent = ciphertext[(base + nonceEnd)..<tagStart]

        let sealedBox = try AES.GCM.SealedBox(nonce: nonce, ciphertext: encryptedContent, tag: tag)
        return try AES.GCM.open(sealedBox, using: key, authenticating: aad)
    }

    /// Build AAD for a Parquet encryption module.
    static func buildAAD(moduleType: ModuleType, rowGroupOrdinal: Int16 = -1, columnOrdinal: Int16 = -1, pageOrdinal: Int16 = -1) -> Data {
        var aad = Data()
        aad.append(moduleType.rawValue)
        if rowGroupOrdinal >= 0 {
            withUnsafeBytes(of: rowGroupOrdinal.littleEndian) { aad.append(contentsOf: $0) }
        }
        if columnOrdinal >= 0 {
            withUnsafeBytes(of: columnOrdinal.littleEndian) { aad.append(contentsOf: $0) }
        }
        if pageOrdinal >= 0 {
            withUnsafeBytes(of: pageOrdinal.littleEndian) { aad.append(contentsOf: $0) }
        }
        return aad
    }
}
