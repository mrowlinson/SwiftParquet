// Zstd.swift — Pure Swift Zstandard decompressor + compressor
// Reference: RFC 8878, https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md

import Foundation

struct ZstdCodec: CompressionCodecProtocol {
    func compress(_ input: Data) throws -> Data {
        try ZstdCompressor.compress(input)
    }

    func decompress(_ input: Data, uncompressedSize: Int) throws -> Data {
        try ZstdDecompressor.decompress(input)
    }
}

// MARK: - Zstd Decompressor

enum ZstdDecompressor {
    static func decompress(_ data: Data) throws -> Data {
        var reader = ZstdByteReader(data: data)
        var output = Data()

        while reader.remaining > 0 {
            if reader.remaining >= 4 {
                let peek = reader.peekUInt32LE()
                if (peek & 0xFFFFFFF0) == 0x184D2A50 {
                    _ = try reader.readUInt32LE()
                    let frameSize = Int(try reader.readUInt32LE())
                    try reader.skip(frameSize)
                    continue
                }
            }
            let frame = try readFrame(&reader)
            output.append(frame)
        }

        return output
    }

    private static func readFrame(_ reader: inout ZstdByteReader) throws -> Data {
        let magic = try reader.readUInt32LE()
        guard magic == 0xFD2FB528 else {
            throw ParquetError.corruptedFile("zstd: invalid frame magic 0x\(String(magic, radix: 16))")
        }

        let fhd = try reader.readByte()
        let checksumFlag = (fhd >> 2) & 1
        let singleSegment = (fhd >> 5) & 1
        let fcsFieldSize: Int
        switch (fhd >> 6) & 3 {
        case 0: fcsFieldSize = singleSegment == 1 ? 1 : 0
        case 1: fcsFieldSize = 2
        case 2: fcsFieldSize = 4
        case 3: fcsFieldSize = 8
        default: fcsFieldSize = 0
        }
        let dictIDFieldSize: Int
        switch fhd & 3 {
        case 0: dictIDFieldSize = 0
        case 1: dictIDFieldSize = 1
        case 2: dictIDFieldSize = 2
        case 3: dictIDFieldSize = 4
        default: dictIDFieldSize = 0
        }

        var windowSize = 0
        if singleSegment == 0 {
            let wd = try reader.readByte()
            let exponent = Int(wd >> 3)
            let mantissa = Int(wd & 7)
            windowSize = (1 << (10 + exponent)) + (mantissa << (7 + exponent))
        }

        if dictIDFieldSize > 0 { try reader.skip(dictIDFieldSize) }

        var frameContentSize = 0
        if fcsFieldSize > 0 {
            switch fcsFieldSize {
            case 1: frameContentSize = Int(try reader.readByte())
            case 2: frameContentSize = Int(try reader.readUInt16LE()) + 256
            case 4: frameContentSize = Int(try reader.readUInt32LE())
            case 8: frameContentSize = Int(try reader.readUInt64LE())
            default: break
            }
        }
        if windowSize == 0 { windowSize = max(frameContentSize, 1 << 10) }

        var output = Data(capacity: frameContentSize > 0 ? frameContentSize : windowSize)
        var lastBlock = false
        var prevLLTable: [FSEEntry]?
        var prevOFTable: [FSEEntry]?
        var prevMLTable: [FSEEntry]?
        var repOffsets: [Int] = [1, 4, 8]

        while !lastBlock {
            let blockHeader = try reader.readUInt24LE()
            lastBlock = (blockHeader & 1) != 0
            let blockType = Int((blockHeader >> 1) & 3)
            let blockSize = Int(blockHeader >> 3)

            switch blockType {
            case 0:
                let raw = try reader.readData(blockSize)
                output.append(raw)
            case 1:
                let b = try reader.readByte()
                output.append(contentsOf: [UInt8](repeating: b, count: blockSize))
            case 2:
                let blockData = try reader.readData(blockSize)
                try decodeCompressedBlock(blockData, output: &output,
                                          prevLLTable: &prevLLTable, prevOFTable: &prevOFTable,
                                          prevMLTable: &prevMLTable, repOffsets: &repOffsets)
            case 3:
                throw ParquetError.corruptedFile("zstd: reserved block type")
            default:
                break
            }
        }

        if checksumFlag != 0 {
            try reader.skip(4)
        }

        return output
    }

    // MARK: - Compressed Block Decoding

    private static func decodeCompressedBlock(_ blockData: Data, output: inout Data,
                                               prevLLTable: inout [FSEEntry]?,
                                               prevOFTable: inout [FSEEntry]?,
                                               prevMLTable: inout [FSEEntry]?,
                                               repOffsets: inout [Int]) throws {
        var br = ZstdForwardBitReader(data: blockData)

        // --- Literals Section ---
        let litSectionHeader = try br.readByteDirect()
        let litType = Int(litSectionHeader & 3)
        let sizeFormat = Int((litSectionHeader >> 2) & 3)

        var regeneratedSize = 0
        var compressedSize = 0
        var numStreams = 1

        switch litType {
        case 0, 1: // Raw or RLE
            switch sizeFormat {
            case 0, 2:
                regeneratedSize = Int(litSectionHeader >> 3)
            case 1:
                regeneratedSize = Int(litSectionHeader >> 4) | (Int(try br.readByteDirect()) << 4)
            case 3:
                let b1 = Int(try br.readByteDirect())
                let b2 = Int(try br.readByteDirect())
                regeneratedSize = Int(litSectionHeader >> 4) | (b1 << 4) | (b2 << 12)
            default: break
            }
        case 2, 3: // Compressed or Treeless
            numStreams = (sizeFormat == 0) ? 1 : 4
            switch sizeFormat {
            case 0, 1:
                let b1 = Int(try br.readByteDirect())
                let b2 = Int(try br.readByteDirect())
                let combined = Int(litSectionHeader >> 4) | (b1 << 4) | (b2 << 12)
                regeneratedSize = combined & 0x3FF
                compressedSize = combined >> 10
            case 2:
                let b1 = Int(try br.readByteDirect())
                let b2 = Int(try br.readByteDirect())
                let b3 = Int(try br.readByteDirect())
                let combined = Int(litSectionHeader >> 4) | (b1 << 4) | (b2 << 12) | (b3 << 20)
                regeneratedSize = combined & 0x3FFF
                compressedSize = combined >> 14
            case 3:
                let b1 = Int(try br.readByteDirect())
                let b2 = Int(try br.readByteDirect())
                let b3 = Int(try br.readByteDirect())
                let b4 = Int(try br.readByteDirect())
                let combined = Int(litSectionHeader >> 4) | (b1 << 4) | (b2 << 12) | (b3 << 20) | (b4 << 28)
                regeneratedSize = combined & 0x3FFFF
                compressedSize = combined >> 18
            default: break
            }
        default: break
        }

        var literals = Data()
        switch litType {
        case 0: // Raw
            literals = try br.readBytesDirect(regeneratedSize)
        case 1: // RLE
            let b = try br.readByteDirect()
            literals = Data(repeating: b, count: regeneratedSize)
        case 2: // Compressed Huffman
            let compData = try br.readBytesDirect(compressedSize)
            literals = try decodeHuffmanLiterals(compData, regeneratedSize: regeneratedSize, numStreams: numStreams)
        case 3: // Treeless (reuse previous Huffman tree — not yet supported, fall back to raw)
            literals = try br.readBytesDirect(compressedSize)
            if literals.count < regeneratedSize {
                literals.append(Data(count: regeneratedSize - literals.count))
            }
        default: break
        }

        // --- Sequences Section ---
        let numSequencesHeader = try br.readByteDirect()
        var numSequences = 0
        if numSequencesHeader == 0 {
            numSequences = 0
        } else if numSequencesHeader < 128 {
            numSequences = Int(numSequencesHeader)
        } else if numSequencesHeader < 255 {
            let b1 = Int(try br.readByteDirect())
            numSequences = ((Int(numSequencesHeader) - 128) << 8) + b1
        } else {
            let b1 = Int(try br.readByteDirect())
            let b2 = Int(try br.readByteDirect())
            numSequences = b1 + (b2 << 8) + 0x7F00
        }

        if numSequences == 0 {
            output.append(literals)
            return
        }

        let symbolModes = try br.readByteDirect()
        let llMode = Int((symbolModes >> 6) & 3)
        let ofMode = Int((symbolModes >> 4) & 3)
        let mlMode = Int((symbolModes >> 2) & 3)

        let llTable = try readFSETable(&br, mode: llMode, defaultTable: Self.defaultLLTable, prevTable: prevLLTable, maxSymbol: 35, defaultLog: 6)
        let ofTable = try readFSETable(&br, mode: ofMode, defaultTable: Self.defaultOFTable, prevTable: prevOFTable, maxSymbol: 31, defaultLog: 5)
        let mlTable = try readFSETable(&br, mode: mlMode, defaultTable: Self.defaultMLTable, prevTable: prevMLTable, maxSymbol: 52, defaultLog: 6)

        prevLLTable = llTable
        prevOFTable = ofTable
        prevMLTable = mlTable

        let seqData = br.readRemainingBytes()
        let sequences = try decodeSequences(seqData, count: numSequences,
                                             llTable: llTable, ofTable: ofTable, mlTable: mlTable,
                                             repOffsets: &repOffsets)

        // Execute sequences
        var litPos = 0
        for seq in sequences {
            let litEnd = min(litPos + seq.litLength, literals.count)
            if litPos < litEnd {
                output.append(literals[literals.startIndex + litPos ..< literals.startIndex + litEnd])
            }
            litPos += seq.litLength

            if seq.matchLength > 0 {
                let matchStart = output.count - seq.offset
                guard matchStart >= 0 else {
                    throw ParquetError.corruptedFile("zstd: match offset \(seq.offset) exceeds history")
                }
                for i in 0..<seq.matchLength {
                    output.append(output[output.startIndex + matchStart + (i % max(seq.offset, 1))])
                }
            }
        }

        if litPos < literals.count {
            output.append(literals[literals.startIndex + litPos ..< literals.endIndex])
        }
    }

    // MARK: - Huffman Literal Decoding

    private static func decodeHuffmanLiterals(_ compData: Data, regeneratedSize: Int, numStreams: Int) throws -> Data {
        guard !compData.isEmpty else { return Data(count: regeneratedSize) }

        var offset = 0
        let headerByte = compData[compData.startIndex]
        offset += 1

        var weights = [Int]()

        if headerByte < 128 {
            // FSE-compressed weights
            let compSize = Int(headerByte)
            guard compSize > 0 && offset + compSize <= compData.count else {
                return Data(count: regeneratedSize)
            }
            let weightData = compData[(compData.startIndex + offset)..<(compData.startIndex + offset + compSize)]
            weights = try decodeFSEWeights(Data(weightData))
            offset += compSize
        } else {
            // Direct representation: (headerByte - 127) symbols, packed as 4-bit pairs
            let numSymbols = Int(headerByte) - 127
            for i in stride(from: 0, to: numSymbols - 1, by: 2) {
                guard offset < compData.count else { break }
                let b = compData[compData.startIndex + offset]
                offset += 1
                weights.append(Int(b >> 4))
                if i + 1 < numSymbols {
                    weights.append(Int(b & 0x0F))
                }
            }
            if numSymbols % 2 == 1 && numSymbols > 0 {
                if weights.count < numSymbols && offset < compData.count {
                    let b = compData[compData.startIndex + offset]
                    offset += 1
                    weights.append(Int(b >> 4))
                }
            }
        }

        guard !weights.isEmpty else { return Data(count: regeneratedSize) }

        // Build Huffman decode table from weights
        let maxWeight = weights.max() ?? 0
        guard maxWeight > 0 else { return Data(count: regeneratedSize) }

        // Compute the number of bits for the table
        var weightSum = 0
        for w in weights where w > 0 {
            weightSum += (1 << (w - 1))
        }

        // maxBits = ceil(log2(weightSum)) but must be at least maxWeight
        var maxBits = maxWeight
        var nextPow2 = 1
        while nextPow2 < weightSum { nextPow2 <<= 1; maxBits = max(maxBits, maxWeight) }
        maxBits = 0
        var tmp = nextPow2
        while tmp > 1 { maxBits += 1; tmp >>= 1 }

        // The last implicit symbol fills the remaining space
        let lastSymbolWeight: Int
        let remaining = nextPow2 - weightSum
        if remaining > 0 {
            var lw = 0
            var r = remaining
            while r > 1 { lw += 1; r >>= 1 }
            lastSymbolWeight = lw + 1
        } else {
            lastSymbolWeight = 0
        }

        // Build (symbol, numBits) pairs
        struct HuffSymbol {
            let symbol: UInt8
            let numBits: Int
        }
        var symbols = [HuffSymbol]()
        for (i, w) in weights.enumerated() where w > 0 {
            symbols.append(HuffSymbol(symbol: UInt8(i), numBits: maxBits + 1 - w))
        }
        if lastSymbolWeight > 0 && weights.count < 256 {
            symbols.append(HuffSymbol(symbol: UInt8(weights.count), numBits: maxBits + 1 - lastSymbolWeight))
        }

        guard !symbols.isEmpty && maxBits > 0 && maxBits <= 12 else {
            return Data(count: regeneratedSize)
        }

        // Build prefix lookup table: 2^maxBits entries
        let tableSize = 1 << maxBits
        var lookupSym = [UInt8](repeating: 0, count: tableSize)
        var lookupBits = [UInt8](repeating: 0, count: tableSize)

        for sym in symbols {
            guard sym.numBits <= maxBits && sym.numBits > 0 else { continue }
            let codeCount = 1 << (maxBits - sym.numBits)
            // Assign codes: we need to track which prefix slots are used
            // Use a simple approach: fill unused slots
            var filled = 0
            for code in 0..<tableSize {
                if filled >= codeCount { break }
                // Check if the top sym.numBits bits match an unassigned prefix
                let prefix = code >> (maxBits - sym.numBits)
                let baseCode = prefix << (maxBits - sym.numBits)
                if baseCode == code && lookupBits[code] == 0 {
                    for ext in 0..<(1 << (maxBits - sym.numBits)) {
                        let idx = baseCode + ext
                        if idx < tableSize && lookupBits[idx] == 0 {
                            lookupSym[idx] = sym.symbol
                            lookupBits[idx] = UInt8(sym.numBits)
                        }
                    }
                    filled += 1
                }
            }
        }

        // Fill any remaining empty slots with a default
        for i in 0..<tableSize where lookupBits[i] == 0 {
            lookupBits[i] = UInt8(maxBits)
        }

        // Decode literals from streams
        let streamData = compData[(compData.startIndex + offset)...]
        if numStreams == 4 && streamData.count >= 6 {
            return try decode4Streams(Data(streamData), regeneratedSize: regeneratedSize,
                                       lookupSym: lookupSym, lookupBits: lookupBits, maxBits: maxBits)
        } else {
            return try decode1Stream(Data(streamData), regeneratedSize: regeneratedSize,
                                      lookupSym: lookupSym, lookupBits: lookupBits, maxBits: maxBits)
        }
    }

    private static func decode1Stream(_ data: Data, regeneratedSize: Int,
                                       lookupSym: [UInt8], lookupBits: [UInt8], maxBits: Int) throws -> Data {
        var result = Data(capacity: regeneratedSize)
        var rbr = ZstdReverseBitReader(data: data)

        while result.count < regeneratedSize && rbr.bitsAvailable > 0 {
            let bits = rbr.peekBits(maxBits)
            let sym = lookupSym[bits]
            let nb = Int(lookupBits[bits])
            rbr.consumeBits(nb)
            result.append(sym)
        }

        while result.count < regeneratedSize { result.append(0) }
        return Data(result.prefix(regeneratedSize))
    }

    private static func decode4Streams(_ data: Data, regeneratedSize: Int,
                                        lookupSym: [UInt8], lookupBits: [UInt8], maxBits: Int) throws -> Data {
        guard data.count >= 6 else {
            return try decode1Stream(data, regeneratedSize: regeneratedSize,
                                      lookupSym: lookupSym, lookupBits: lookupBits, maxBits: maxBits)
        }

        // Jump table: 3 x 2-byte LE sizes for streams 1-3
        let s1 = Int(data[data.startIndex]) | (Int(data[data.startIndex + 1]) << 8)
        let s2 = Int(data[data.startIndex + 2]) | (Int(data[data.startIndex + 3]) << 8)
        let s3 = Int(data[data.startIndex + 4]) | (Int(data[data.startIndex + 5]) << 8)

        let streamStart = 6
        let o1 = streamStart
        let o2 = o1 + s1
        let o3 = o2 + s2
        let o4 = o3 + s3

        let segSize = (regeneratedSize + 3) / 4
        var result = Data(capacity: regeneratedSize)

        let streams: [(Int, Int, Int)] = [
            (o1, o2, segSize),
            (o2, o3, segSize),
            (o3, o4, segSize),
            (o4, data.count, regeneratedSize - 3 * segSize)
        ]

        for (start, end, targetSize) in streams {
            guard start < data.count && end <= data.count && start < end else {
                result.append(Data(count: targetSize))
                continue
            }
            let streamData = Data(data[(data.startIndex + start)..<(data.startIndex + end)])
            let decoded = try decode1Stream(streamData, regeneratedSize: targetSize,
                                             lookupSym: lookupSym, lookupBits: lookupBits, maxBits: maxBits)
            result.append(decoded)
        }

        return Data(result.prefix(regeneratedSize))
    }

    // MARK: - FSE Weight Decoding (for Huffman tree header)

    private static func decodeFSEWeights(_ data: Data) throws -> [Int] {
        guard !data.isEmpty else { return [] }

        var br = ZstdForwardBitReader(data: data)
        let accuracyLog = Int(try br.readBitsForward(4)) + 5

        // Build the FSE table for weight decoding
        let tableSize = 1 << accuracyLog
        var probs = [Int16]()
        var remainingProb = Int16(tableSize) + 1
        var symbol = 0

        while remainingProb > 1 && symbol < 256 {
            let maxBitsNeeded = highBit(UInt32(remainingProb + 1)) + 1
            let smallVal = try br.readBitsForward(maxBitsNeeded - 1)
            let threshold = (1 << maxBitsNeeded) - 1 - Int(remainingProb) + 1

            let value: Int
            if smallVal < threshold {
                value = smallVal
            } else {
                let extra = try br.readBitsForward(1)
                value = (smallVal << 1) + extra - threshold
            }

            let prob = Int16(value) - 1
            probs.append(prob)
            if prob < 0 {
                remainingProb -= 1
            } else if prob > 0 {
                remainingProb -= prob
            } else {
                // Zero probability followed by optional repeat
                var repeat0 = 0
                repeat {
                    let r = try br.readBitsForward(2)
                    repeat0 += r
                    if r < 3 { break }
                } while true
                for _ in 0..<repeat0 {
                    probs.append(0)
                    symbol += 1
                }
            }
            symbol += 1
        }

        // Build FSE decode table
        let table = buildFSEDecodeTable(probs: probs, accuracyLog: accuracyLog, tableSize: tableSize)

        // Decode weights using the FSE table
        var state = try br.readBitsForward(accuracyLog)
        var weights = [Int]()

        while br.bitsAvailable > -(accuracyLog + 1) {
            let entry = table[state % table.count]
            weights.append(entry.symbol)
            if br.bitsAvailable < entry.numBits { break }
            state = entry.baseline + (try br.readBitsForward(entry.numBits))
        }

        return weights
    }

    // MARK: - FSE Table Construction

    struct FSEEntry {
        let symbol: Int
        let numBits: Int
        let baseline: Int
    }

    struct Sequence {
        let litLength: Int
        let matchLength: Int
        let offset: Int
    }

    private static func buildFSEDecodeTable(probs: [Int16], accuracyLog: Int, tableSize: Int) -> [FSEEntry] {
        var table = [FSEEntry](repeating: FSEEntry(symbol: 0, numBits: 0, baseline: 0), count: tableSize)

        // Place symbols with prob == -1 at the end (less-than-one probability)
        var highThreshold = tableSize - 1
        for (sym, prob) in probs.enumerated() {
            if prob == -1 {
                table[highThreshold] = FSEEntry(symbol: sym, numBits: 0, baseline: 0)
                highThreshold -= 1
            }
        }

        // Spread remaining symbols using the Zstd position formula
        let step = (tableSize >> 1) + (tableSize >> 3) + 3
        let tableMask = tableSize - 1
        var position = 0

        for (sym, prob) in probs.enumerated() {
            guard prob > 0 else { continue }
            for _ in 0..<Int(prob) {
                table[position] = FSEEntry(symbol: sym, numBits: 0, baseline: 0)
                position = (position + step) & tableMask
                while position > highThreshold {
                    position = (position + step) & tableMask
                }
            }
        }

        // Compute numBits and baseline for each cell
        var symbolOccurrence = [Int](repeating: 0, count: probs.count + 1)
        for i in 0..<tableSize {
            let sym = table[i].symbol
            let prob = sym < probs.count ? max(Int(probs[sym]), 1) : 1

            let numBits = accuracyLog - highBit(UInt32(prob))
            let baseline = (Int(1 + (1 << numBits)) * prob - tableSize) + symbolOccurrence[sym]

            table[i] = FSEEntry(symbol: sym, numBits: numBits, baseline: baseline)
            symbolOccurrence[sym] += 1
        }

        // Fix less-than-one entries (prob == -1)
        for i in (highThreshold + 1)..<tableSize {
            let sym = table[i].symbol
            table[i] = FSEEntry(symbol: sym, numBits: accuracyLog, baseline: symbolOccurrence[sym])
            symbolOccurrence[sym] += 1
        }

        return table
    }

    private static func readFSETable(_ br: inout ZstdForwardBitReader, mode: Int, defaultTable: [FSEEntry],
                                      prevTable: [FSEEntry]?, maxSymbol: Int, defaultLog: Int) throws -> [FSEEntry] {
        switch mode {
        case 0: return defaultTable
        case 1:
            let sym = Int(try br.readByteDirect())
            return [FSEEntry(symbol: sym, numBits: 0, baseline: 0)]
        case 2:
            return try decodeFSETableFromBitstream(&br, maxSymbol: maxSymbol)
        case 3:
            if let prev = prevTable { return prev }
            return defaultTable
        default:
            return defaultTable
        }
    }

    private static func decodeFSETableFromBitstream(_ br: inout ZstdForwardBitReader, maxSymbol: Int) throws -> [FSEEntry] {
        let accuracyLog = Int(try br.readBitsForward(4)) + 5
        let tableSize = 1 << accuracyLog
        var probs = [Int16]()
        var remainingProb = Int16(tableSize) + 1
        var symbol = 0

        while remainingProb > 1 && symbol <= maxSymbol {
            let maxBitsNeeded = highBit(UInt32(remainingProb + 1)) + 1
            let smallVal = try br.readBitsForward(maxBitsNeeded - 1)
            let threshold = (1 << maxBitsNeeded) - 1 - Int(remainingProb) + 1

            let value: Int
            if smallVal < threshold {
                value = smallVal
            } else {
                let extra = try br.readBitsForward(1)
                value = (smallVal << 1) + extra - threshold
            }

            let prob = Int16(value) - 1
            probs.append(prob)
            if prob < 0 {
                remainingProb -= 1
            } else if prob > 0 {
                remainingProb -= prob
            } else {
                var repeat0 = 0
                repeat {
                    let r = try br.readBitsForward(2)
                    repeat0 += r
                    if r < 3 { break }
                } while true
                for _ in 0..<repeat0 {
                    probs.append(0)
                    symbol += 1
                }
            }
            symbol += 1
        }

        // Pad to maxSymbol+1 with zeros
        while probs.count <= maxSymbol { probs.append(0) }

        br.alignToByte()
        return buildFSEDecodeTable(probs: probs, accuracyLog: accuracyLog, tableSize: tableSize)
    }

    // MARK: - Sequence Decoding

    private static func decodeSequences(_ data: Data, count: Int,
                                         llTable: [FSEEntry], ofTable: [FSEEntry], mlTable: [FSEEntry],
                                         repOffsets: inout [Int]) throws -> [Sequence] {
        guard !data.isEmpty && count > 0 else { return [] }

        var sequences = [Sequence]()
        sequences.reserveCapacity(count)

        var rbr = ZstdReverseBitReader(data: data)

        guard !llTable.isEmpty && !ofTable.isEmpty && !mlTable.isEmpty else { return [] }

        let llLog = llTable.count > 1 ? highBit(UInt32(llTable.count)) : 0
        let ofLog = ofTable.count > 1 ? highBit(UInt32(ofTable.count)) : 0
        let mlLog = mlTable.count > 1 ? highBit(UInt32(mlTable.count)) : 0

        // Init order per spec: OF, then ML, then LL
        var llState = llLog > 0 ? rbr.readBitsSafe(llLog) : 0
        var mlState = mlLog > 0 ? rbr.readBitsSafe(mlLog) : 0
        var ofState = ofLog > 0 ? rbr.readBitsSafe(ofLog) : 0

        for i in 0..<count {
            let ofEntry = ofTable[ofState % ofTable.count]
            let mlEntry = mlTable[mlState % mlTable.count]
            let llEntry = llTable[llState % llTable.count]

            let ofSymbol = ofEntry.symbol
            let mlSymbol = min(mlEntry.symbol, zstdMLBaselines.count - 1)
            let llSymbol = min(llEntry.symbol, zstdLLBaselines.count - 1)

            // Offset: read extra bits
            let ofExtraBits = ofSymbol
            var offset: Int
            if ofExtraBits > 0 {
                let extra = rbr.readBitsSafe(ofExtraBits)
                offset = (1 << ofExtraBits) + extra
            } else {
                offset = 1
            }

            // Handle repeated offsets
            if offset <= 3 {
                let llBase = zstdLLBaselines[llSymbol]
                let llExtra = zstdLLExtraBits[llSymbol]
                let litLength = llBase + (llExtra > 0 ? rbr.readBitsSafe(llExtra) : 0)

                if litLength == 0 {
                    if offset == 3 {
                        offset = repOffsets[0] - 1
                        if offset <= 0 { offset = 1 }
                    }
                    // Shift repeated offsets
                    let tempIdx = offset
                    if tempIdx == 1 {
                        offset = repOffsets[0]
                    } else if tempIdx == 2 {
                        offset = repOffsets[1]
                        repOffsets[1] = repOffsets[0]
                        repOffsets[0] = offset
                    } else {
                        offset = repOffsets[2]
                        repOffsets[2] = repOffsets[1]
                        repOffsets[1] = repOffsets[0]
                        repOffsets[0] = offset
                    }
                } else {
                    // Normal repeated offset
                    let repIdx = offset - 1
                    offset = repOffsets[repIdx]
                    if repIdx > 0 {
                        repOffsets[repIdx] = repOffsets[repIdx - 1]
                        if repIdx > 1 { repOffsets[repIdx - 1] = repOffsets[0] }
                        repOffsets[0] = offset
                    }
                }

                // Match length
                let mlBase = zstdMLBaselines[mlSymbol]
                let mlExtra = zstdMLExtraBits[mlSymbol]
                let matchLength = mlBase + 3 + (mlExtra > 0 ? rbr.readBitsSafe(mlExtra) : 0)

                sequences.append(Sequence(litLength: litLength, matchLength: matchLength, offset: max(offset, 1)))
            } else {
                offset -= 3
                repOffsets[2] = repOffsets[1]
                repOffsets[1] = repOffsets[0]
                repOffsets[0] = offset

                let mlBase = zstdMLBaselines[mlSymbol]
                let mlExtra = zstdMLExtraBits[mlSymbol]
                let matchLength = mlBase + 3 + (mlExtra > 0 ? rbr.readBitsSafe(mlExtra) : 0)

                let llBase = zstdLLBaselines[llSymbol]
                let llExtra = zstdLLExtraBits[llSymbol]
                let litLength = llBase + (llExtra > 0 ? rbr.readBitsSafe(llExtra) : 0)

                sequences.append(Sequence(litLength: litLength, matchLength: matchLength, offset: max(offset, 1)))
            }

            // Update FSE states (order per spec: LL, ML, OF)
            if i < count - 1 {
                if llEntry.numBits > 0 {
                    llState = llEntry.baseline + rbr.readBitsSafe(llEntry.numBits)
                }
                if mlEntry.numBits > 0 {
                    mlState = mlEntry.baseline + rbr.readBitsSafe(mlEntry.numBits)
                }
                if ofEntry.numBits > 0 {
                    ofState = ofEntry.baseline + rbr.readBitsSafe(ofEntry.numBits)
                }
            }
        }

        return sequences
    }

    // Baseline tables are at file scope (shared with compressor)

    // MARK: - Default FSE Tables (RFC 8878 Section 3.1.1.1)

    private static let defaultLLTable: [FSEEntry] = {
        let probs: [Int16] = [
            4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1,
            -1, -1, -1, -1
        ]
        return buildFSEDecodeTable(probs: probs, accuracyLog: 6, tableSize: 64)
    }()

    private static let defaultOFTable: [FSEEntry] = {
        let probs: [Int16] = [
            1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1, -1, -1, -1
        ]
        return buildFSEDecodeTable(probs: probs, accuracyLog: 5, tableSize: 32)
    }()

    private static let defaultMLTable: [FSEEntry] = {
        let probs: [Int16] = [
            1, 4, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1,
            -1, -1, -1, -1, -1
        ]
        return buildFSEDecodeTable(probs: probs, accuracyLog: 6, tableSize: 64)
    }()

    private static func highBit(_ v: UInt32) -> Int {
        guard v > 0 else { return 0 }
        return 31 - v.leadingZeroBitCount
    }
}

// MARK: - Zstd Compressor (raw blocks — valid Zstd frames, no compression)

enum ZstdCompressor {
    static func compress(_ input: Data) throws -> Data {
        guard !input.isEmpty else {
            var frame = Data()
            frame.append(contentsOf: [0x28, 0xB5, 0x2F, 0xFD])
            frame.append(0x20)
            frame.append(0x00)
            frame.append(contentsOf: [0x01, 0x00, 0x00])
            return frame
        }

        var frame = Data()
        frame.append(contentsOf: [0x28, 0xB5, 0x2F, 0xFD])

        if input.count <= 255 {
            frame.append(0x20)
            frame.append(UInt8(input.count))
        } else if input.count <= 65535 + 256 {
            frame.append(0x60)
            let adjusted = UInt16(input.count - 256)
            withUnsafeBytes(of: adjusted.littleEndian) { frame.append(contentsOf: $0) }
        } else {
            frame.append(0xA0)
            withUnsafeBytes(of: UInt32(input.count).littleEndian) { frame.append(contentsOf: $0) }
        }

        var offset = 0
        let maxBlockSize = 1 << 17

        while offset < input.count {
            let remaining = input.count - offset
            let blockSize = min(remaining, maxBlockSize)
            let isLast = (offset + blockSize >= input.count)

            var blockHeader = UInt32(blockSize) << 3
            blockHeader |= 0 << 1 // Raw block type
            if isLast { blockHeader |= 1 }

            frame.append(UInt8(blockHeader & 0xFF))
            frame.append(UInt8((blockHeader >> 8) & 0xFF))
            frame.append(UInt8((blockHeader >> 16) & 0xFF))

            let start = input.startIndex + offset
            frame.append(input[start..<(start + blockSize)])
            offset += blockSize
        }

        return frame
    }
}

// MARK: - Shared Baseline Tables (RFC 8878 Section 3.1.1.3)

private let zstdLLBaselines = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    16, 18, 20, 24, 28, 32, 40, 48, 64, 128, 256, 512,
    1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072
]
private let zstdLLExtraBits = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
]
private let zstdMLBaselines = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    16, 18, 20, 24, 28, 32, 40, 48, 64, 128, 256, 512,
    1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072
]
private let zstdMLExtraBits = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
]

// MARK: - Bit Readers

private struct ZstdByteReader {
    let data: Data
    var offset: Int = 0
    var remaining: Int { data.count - offset }

    init(data: Data) { self.data = data }

    mutating func readByte() throws -> UInt8 {
        guard offset < data.count else { throw ParquetError.unexpectedEOF }
        let b = data[data.startIndex + offset]
        offset += 1
        return b
    }

    mutating func readUInt16LE() throws -> UInt16 {
        UInt16(try readByte()) | (UInt16(try readByte()) << 8)
    }

    mutating func readUInt24LE() throws -> UInt32 {
        UInt32(try readByte()) | (UInt32(try readByte()) << 8) | (UInt32(try readByte()) << 16)
    }

    mutating func readUInt32LE() throws -> UInt32 {
        UInt32(try readByte()) | (UInt32(try readByte()) << 8) |
        (UInt32(try readByte()) << 16) | (UInt32(try readByte()) << 24)
    }

    mutating func readUInt64LE() throws -> UInt64 {
        let lo = UInt64(try readUInt32LE())
        let hi = UInt64(try readUInt32LE())
        return lo | (hi << 32)
    }

    mutating func readData(_ count: Int) throws -> Data {
        guard offset + count <= data.count else { throw ParquetError.unexpectedEOF }
        let start = data.startIndex + offset
        offset += count
        return data[start..<(start + count)]
    }

    func peekUInt32LE() -> UInt32 {
        guard offset + 4 <= data.count else { return 0 }
        let s = data.startIndex + offset
        return UInt32(data[s]) | (UInt32(data[s+1]) << 8) |
               (UInt32(data[s+2]) << 16) | (UInt32(data[s+3]) << 24)
    }

    mutating func skip(_ count: Int) throws {
        guard offset + count <= data.count else { throw ParquetError.unexpectedEOF }
        offset += count
    }
}

/// Forward bit reader: reads bits LSB-first from byte stream
private struct ZstdForwardBitReader {
    private let data: Data
    private var byteOffset: Int = 0
    private var bitOffset: Int = 0 // 0..7 within current byte

    var bitsAvailable: Int { (data.count - byteOffset) * 8 - bitOffset }

    init(data: Data) { self.data = data }

    mutating func readByteDirect() throws -> UInt8 {
        guard bitOffset == 0 || true else { throw ParquetError.unexpectedEOF }
        // Align first
        if bitOffset != 0 {
            byteOffset += 1
            bitOffset = 0
        }
        guard byteOffset < data.count else { throw ParquetError.unexpectedEOF }
        let b = data[data.startIndex + byteOffset]
        byteOffset += 1
        return b
    }

    mutating func readBytesDirect(_ count: Int) throws -> Data {
        if bitOffset != 0 {
            byteOffset += 1
            bitOffset = 0
        }
        guard byteOffset + count <= data.count else { throw ParquetError.unexpectedEOF }
        let start = data.startIndex + byteOffset
        byteOffset += count
        return Data(data[start..<(start + count)])
    }

    mutating func readBitsForward(_ n: Int) throws -> Int {
        guard n > 0 else { return 0 }
        var result = 0
        for i in 0..<n {
            let totalBit = byteOffset * 8 + bitOffset
            guard totalBit < data.count * 8 else { return result }
            let byte = data[data.startIndex + byteOffset]
            let bit = (Int(byte) >> bitOffset) & 1
            result |= bit << i
            bitOffset += 1
            if bitOffset >= 8 {
                bitOffset = 0
                byteOffset += 1
            }
        }
        return result
    }

    mutating func alignToByte() {
        if bitOffset != 0 {
            byteOffset += 1
            bitOffset = 0
        }
    }

    func readRemainingBytes() -> Data {
        let start = bitOffset != 0 ? byteOffset + 1 : byteOffset
        guard start < data.count else { return Data() }
        return Data(data[(data.startIndex + start)...])
    }
}

/// Reverse bit reader: reads bits MSB-first from end of byte stream
private struct ZstdReverseBitReader {
    private let data: Data
    private var bitPos: Int // current bit position (counts down)

    var bitsAvailable: Int { bitPos }

    init(data: Data) {
        self.data = data
        // Find sentinel bit: highest set bit in last byte
        self.bitPos = 0
        guard !data.isEmpty else { return }
        let lastByte = data[data.startIndex + data.count - 1]
        guard lastByte != 0 else {
            self.bitPos = (data.count - 1) * 8
            return
        }
        let highBit = 7 - lastByte.leadingZeroBitCount
        self.bitPos = (data.count - 1) * 8 + highBit // skip sentinel bit itself
    }

    mutating func peekBits(_ n: Int) -> Int {
        guard n > 0 else { return 0 }
        var result = 0
        for i in 0..<n {
            let pos = bitPos - 1 - i
            guard pos >= 0 else { break }
            let byteIdx = pos / 8
            let bitIdx = pos % 8
            guard byteIdx < data.count else { break }
            let bit = (Int(data[data.startIndex + byteIdx]) >> bitIdx) & 1
            result |= bit << (n - 1 - i)
        }
        return result
    }

    mutating func consumeBits(_ n: Int) {
        bitPos -= n
        if bitPos < 0 { bitPos = 0 }
    }

    mutating func readBitsSafe(_ n: Int) -> Int {
        guard n > 0 else { return 0 }
        var result = 0
        for i in 0..<n {
            bitPos -= 1
            guard bitPos >= 0 else { return result }
            let byteIdx = bitPos / 8
            let bitIdx = bitPos % 8
            guard byteIdx < data.count else { return result }
            let bit = (Int(data[data.startIndex + byteIdx]) >> bitIdx) & 1
            result |= bit << (n - 1 - i)
        }
        return result
    }
}
