// CRC32.swift — CRC-32 (ISO 3309 / ITU-T V.42) used by gzip
// Pure Swift, table-based, polynomial 0xEDB88320

import Foundation

struct CRC32 {
    private static let table: [UInt32] = {
        var t = [UInt32](repeating: 0, count: 256)
        for i in 0..<256 {
            var c = UInt32(i)
            for _ in 0..<8 {
                if c & 1 != 0 {
                    c = 0xEDB88320 ^ (c >> 1)
                } else {
                    c >>= 1
                }
            }
            t[i] = c
        }
        return t
    }()

    private var crc: UInt32 = 0xFFFF_FFFF

    mutating func update(_ data: Data) {
        data.withUnsafeBytes { ptr in
            guard let base = ptr.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return }
            for i in 0..<data.count {
                let index = Int((crc ^ UInt32(base[i])) & 0xFF)
                crc = CRC32.table[index] ^ (crc >> 8)
            }
        }
    }

    mutating func update(_ bytes: [UInt8]) {
        for b in bytes {
            let index = Int((crc ^ UInt32(b)) & 0xFF)
            crc = CRC32.table[index] ^ (crc >> 8)
        }
    }

    var value: UInt32 { crc ^ 0xFFFF_FFFF }

    static func checksum(_ data: Data) -> UInt32 {
        var c = CRC32()
        c.update(data)
        return c.value
    }
}
