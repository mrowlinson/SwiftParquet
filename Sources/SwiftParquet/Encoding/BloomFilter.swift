// BloomFilter.swift — Split Block Bloom Filter for Parquet
// Reference: https://github.com/apache/parquet-format/blob/master/BloomFilter.md
//
// Parquet uses a "split block Bloom filter" — a cache-friendly variant where
// each element is hashed to a single 256-bit block (32 bytes) and sets 8 bits
// within that block using 8 independent salt values.
//
// Hash function: xxHash64 folded to 32 bits, then block index = hash % numBlocks.

import Foundation

// MARK: - Split Block Bloom Filter

public struct BloomFilter: Sendable {
    private var blocks: Data  // Each block is 32 bytes (256 bits)
    let numBlocks: Int

    /// Create a bloom filter with approximately `numDistinct` expected elements
    /// and a target false positive rate of ~1%.
    public init(numDistinct: Int) {
        // Optimal number of bits per element for 1% FPR is about 10
        let numBits = max(numDistinct * 10, 256)
        let numBlocks = max((numBits + 255) / 256, 1)
        self.numBlocks = numBlocks
        self.blocks = Data(count: numBlocks * 32)
    }

    /// Create from existing filter data.
    public init(data: Data) {
        self.blocks = data
        self.numBlocks = data.count / 32
    }

    // 8 salt values for the 8 probes within a block
    private static let salts: [UInt32] = [
        0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
        0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
    ]

    /// Insert a value (hashed to UInt64 via xxHash64).
    public mutating func insert(hash: UInt64) {
        let blockIndex = Int(UInt32(truncatingIfNeeded: hash) % UInt32(numBlocks))
        let blockOffset = blockIndex * 32
        let key = UInt32(truncatingIfNeeded: hash >> 32)

        blocks.withUnsafeMutableBytes { buf in
            for i in 0..<8 {
                let bitIndex = Int((key &* BloomFilter.salts[i]) >> 27)
                let wordOffset = blockOffset + i * 4
                guard wordOffset + 4 <= buf.count else { continue }
                var word = buf.loadUnaligned(fromByteOffset: wordOffset, as: UInt32.self)
                word |= (1 << bitIndex)
                buf.storeBytes(of: word, toByteOffset: wordOffset, as: UInt32.self)
            }
        }
    }

    /// Check if a value might be in the set.
    public func mightContain(hash: UInt64) -> Bool {
        let blockIndex = Int(UInt32(truncatingIfNeeded: hash) % UInt32(numBlocks))
        let blockOffset = blockIndex * 32
        let key = UInt32(truncatingIfNeeded: hash >> 32)

        return blocks.withUnsafeBytes { buf in
            for i in 0..<8 {
                let bitIndex = Int((key &* BloomFilter.salts[i]) >> 27)
                let wordOffset = blockOffset + i * 4
                guard wordOffset + 4 <= buf.count else { return false }
                let word = buf.loadUnaligned(fromByteOffset: wordOffset, as: UInt32.self)
                if word & (1 << bitIndex) == 0 { return false }
            }
            return true
        }
    }

    /// Serialized filter data.
    public var data: Data { blocks }

    // MARK: - Hash helpers

    /// Hash bytes using xxHash64 (simplified).
    public static func xxHash64(_ data: Data, seed: UInt64 = 0) -> UInt64 {
        let prime1: UInt64 = 0x9E3779B185EBCA87
        let prime2: UInt64 = 0xC2B2AE3D27D4EB4F
        let prime3: UInt64 = 0x165667B19E3779F9
        let prime4: UInt64 = 0x85EBCA77C2B2AE63
        let prime5: UInt64 = 0x27D4EB2F165667C5

        let len = data.count

        // Hoist withUnsafeBytes to function level — eliminates per-read closure overhead
        return data.withUnsafeBytes { buf in
            @inline(__always) func rd64(_ off: Int) -> UInt64 {
                UInt64(littleEndian: buf.loadUnaligned(fromByteOffset: off, as: UInt64.self))
            }
            @inline(__always) func rd32(_ off: Int) -> UInt32 {
                UInt32(littleEndian: buf.loadUnaligned(fromByteOffset: off, as: UInt32.self))
            }

            var h: UInt64

            if len >= 32 {
                var v1 = seed &+ prime1 &+ prime2
                var v2 = seed &+ prime2
                var v3 = seed
                var v4 = seed &- prime1

                var offset = 0
                while offset + 32 <= len {
                    v1 = xxRound(v1, rd64(offset))
                    v2 = xxRound(v2, rd64(offset + 8))
                    v3 = xxRound(v3, rd64(offset + 16))
                    v4 = xxRound(v4, rd64(offset + 24))
                    offset += 32
                }

                h = rotl(v1, 1) &+ rotl(v2, 7) &+ rotl(v3, 12) &+ rotl(v4, 18)
                h = mergeRound(h, v1)
                h = mergeRound(h, v2)
                h = mergeRound(h, v3)
                h = mergeRound(h, v4)

                while offset + 8 <= len {
                    h ^= xxRound(0, rd64(offset))
                    h = rotl(h, 27) &* prime1 &+ prime4
                    offset += 8
                }
                while offset + 4 <= len {
                    h ^= UInt64(rd32(offset)) &* prime1
                    h = rotl(h, 23) &* prime2 &+ prime3
                    offset += 4
                }
                while offset < len {
                    h ^= UInt64(buf[offset]) &* prime5
                    h = rotl(h, 11) &* prime1
                    offset += 1
                }
            } else {
                h = seed &+ prime5
                var offset = 0
                while offset + 8 <= len {
                    h ^= xxRound(0, rd64(offset))
                    h = rotl(h, 27) &* prime1 &+ prime4
                    offset += 8
                }
                while offset + 4 <= len {
                    h ^= UInt64(rd32(offset)) &* prime1
                    h = rotl(h, 23) &* prime2 &+ prime3
                    offset += 4
                }
                while offset < len {
                    h ^= UInt64(buf[offset]) &* prime5
                    h = rotl(h, 11) &* prime1
                    offset += 1
                }
            }

            h &+= UInt64(len)

            h ^= h >> 33
            h &*= prime2
            h ^= h >> 29
            h &*= prime3
            h ^= h >> 32

            return h
        }
    }

    /// Hash a string for bloom filter insertion/lookup.
    public static func hashString(_ s: String) -> UInt64 {
        xxHash64(Data(s.utf8))
    }

    /// Hash an Int64 for bloom filter insertion/lookup.
    public static func hashInt64(_ v: Int64) -> UInt64 {
        var data = Data(count: 8)
        withUnsafeBytes(of: v.littleEndian) { data = Data($0) }
        return xxHash64(data)
    }

    /// Hash an Int32.
    public static func hashInt32(_ v: Int32) -> UInt64 {
        var data = Data(count: 4)
        withUnsafeBytes(of: v.littleEndian) { data = Data($0) }
        return xxHash64(data)
    }

    /// Hash a Double.
    public static func hashDouble(_ v: Double) -> UInt64 {
        var data = Data(count: 8)
        withUnsafeBytes(of: v.bitPattern.littleEndian) { data = Data($0) }
        return xxHash64(data)
    }

    /// Hash a Float.
    public static func hashFloat(_ v: Float) -> UInt64 {
        var data = Data(count: 4)
        withUnsafeBytes(of: v.bitPattern.littleEndian) { data = Data($0) }
        return xxHash64(data)
    }

    // MARK: - xxHash helpers

    private static func xxRound(_ acc: UInt64, _ input: UInt64) -> UInt64 {
        var a = acc &+ input &* 0xC2B2AE3D27D4EB4F
        a = rotl(a, 31)
        a &*= 0x9E3779B185EBCA87
        return a
    }

    private static func mergeRound(_ acc: UInt64, _ val: UInt64) -> UInt64 {
        var a = acc ^ xxRound(0, val)
        a = a &* 0x9E3779B185EBCA87 &+ 0x85EBCA77C2B2AE63
        return a
    }

    private static func rotl(_ v: UInt64, _ n: Int) -> UInt64 {
        (v << n) | (v >> (64 - n))
    }

}
