// Dremel.swift — Dremel algorithm for nested Parquet types (List, Map, Struct)
// Reference: "Dremel: Interactive Analysis of Web-Scale Datasets" (Melnik et al., 2010)
//
// Shredding: converts nested records (ParquetRecord) into flat columns with
//   definition levels (which fields are null/present) and repetition levels
//   (which repeated fields start a new list).
//
// Assembly: reconstructs nested records from flat columns + levels.

import Foundation

// MARK: - Shredded Column

struct ShreddedColumn {
    let path: [String]          // column path in schema
    let physicalType: PhysicalType
    var values: [ParquetRecord]  // non-null leaf values only
    var defLevels: [Int32]       // one per slot (including nulls)
    var repLevels: [Int32]       // one per slot
}

// MARK: - Dremel Shredder

struct DremelShredder {

    /// Shred an array of top-level records into flat columns with def/rep levels.
    /// Each row should be a .struct with fields matching the schema's leaf columns.
    /// Returns one ShreddedColumn per leaf in DFS order.
    static func shred(rows: [ParquetRecord], schema: GroupNode) -> [ShreddedColumn] {
        let leaves = schema.leafColumns
        var columns = leaves.map { leaf in
            ShreddedColumn(
                path: buildPath(leaf),
                physicalType: leaf.physicalType,
                values: [],
                defLevels: [],
                repLevels: []
            )
        }

        for row in rows {
            shredNode(value: row, schemaNode: schema, columns: &columns,
                      leafIndex: 0, currentDef: 0, currentRep: 0)
        }

        return columns
    }

    @discardableResult
    private static func shredNode(
        value: ParquetRecord,
        schemaNode: any SchemaNode,
        columns: inout [ShreddedColumn],
        leafIndex: Int,
        currentDef: Int32,
        currentRep: Int32
    ) -> Int {
        if let prim = schemaNode as? PrimitiveNode {
            return shredPrimitive(value: value, node: prim, columns: &columns,
                                   leafIndex: leafIndex, currentDef: currentDef, currentRep: currentRep)
        }

        guard let group = schemaNode as? GroupNode else { return leafIndex }

        if group.convertedType == .list {
            return shredList(value: value, group: group, columns: &columns,
                             leafIndex: leafIndex, currentDef: currentDef, currentRep: currentRep)
        } else if group.convertedType == .map {
            return shredMap(value: value, group: group, columns: &columns,
                            leafIndex: leafIndex, currentDef: currentDef, currentRep: currentRep)
        } else {
            return shredStruct(value: value, group: group, columns: &columns,
                               leafIndex: leafIndex, currentDef: currentDef, currentRep: currentRep)
        }
    }

    private static func shredPrimitive(
        value: ParquetRecord,
        node: PrimitiveNode,
        columns: inout [ShreddedColumn],
        leafIndex: Int,
        currentDef: Int32,
        currentRep: Int32
    ) -> Int {
        guard leafIndex < columns.count else { return leafIndex + 1 }

        switch value {
        case .null:
            columns[leafIndex].defLevels.append(currentDef)
            columns[leafIndex].repLevels.append(currentRep)
        default:
            let maxDef = Int32(node.maxDefinitionLevel)
            columns[leafIndex].defLevels.append(maxDef)
            columns[leafIndex].repLevels.append(currentRep)
            columns[leafIndex].values.append(value)
        }
        return leafIndex + 1
    }

    private static func shredStruct(
        value: ParquetRecord,
        group: GroupNode,
        columns: inout [ShreddedColumn],
        leafIndex: Int,
        currentDef: Int32,
        currentRep: Int32
    ) -> Int {
        var idx = leafIndex

        // If group is optional, increment def when present
        let defForPresent = group.repetition != .required ? currentDef + 1 : currentDef

        switch value {
        case .null:
            // Emit null for all leaves under this group
            for child in group.children {
                idx = emitNulls(schemaNode: child, columns: &columns,
                                leafIndex: idx, defLevel: currentDef, repLevel: currentRep)
            }
        case .struct(let fields):
            let fieldMap = Dictionary(fields, uniquingKeysWith: { _, b in b })
            for child in group.children {
                let childValue = fieldMap[child.name] ?? .null
                idx = shredNode(value: childValue, schemaNode: child, columns: &columns,
                                leafIndex: idx, currentDef: defForPresent, currentRep: currentRep)
            }
        default:
            // Treat non-struct values as present primitives (for flat schema compatibility)
            for child in group.children {
                idx = shredNode(value: value, schemaNode: child, columns: &columns,
                                leafIndex: idx, currentDef: defForPresent, currentRep: currentRep)
            }
        }
        return idx
    }

    private static func shredList(
        value: ParquetRecord,
        group: GroupNode,
        columns: inout [ShreddedColumn],
        leafIndex: Int,
        currentDef: Int32,
        currentRep: Int32
    ) -> Int {
        // Standard 3-level list: group(LIST) { repeated group list { element } }
        // The outer group may be optional (adds 1 to def)
        // The repeated group adds 1 to def (for "list exists but empty" vs "list has elements")
        // The element may be optional (adds 1 more to def)

        let defForListPresent = group.repetition != .required ? currentDef + 1 : currentDef

        guard let repeatedGroup = group.children.first as? GroupNode else {
            return leafIndex
        }
        let defForNonEmpty = defForListPresent + 1
        let maxRepForList = Int32(repeatedGroup.maxRepetitionLevel)

        switch value {
        case .null:
            // List is null
            var idx = leafIndex
            for child in repeatedGroup.children {
                idx = emitNulls(schemaNode: child, columns: &columns,
                                leafIndex: idx, defLevel: currentDef, repLevel: currentRep)
            }
            return idx

        case .list(let elements):
            if elements.isEmpty {
                // List exists but is empty
                var idx = leafIndex
                for child in repeatedGroup.children {
                    idx = emitNulls(schemaNode: child, columns: &columns,
                                    leafIndex: idx, defLevel: defForListPresent, repLevel: currentRep)
                }
                return idx
            }

            var idx = leafIndex
            for (i, element) in elements.enumerated() {
                let rep = (i == 0) ? currentRep : maxRepForList
                let startIdx = leafIndex
                idx = startIdx
                for child in repeatedGroup.children {
                    idx = shredNode(value: element, schemaNode: child, columns: &columns,
                                    leafIndex: idx, currentDef: defForNonEmpty, currentRep: rep)
                }
            }
            return idx

        default:
            return leafIndex
        }
    }

    private static func shredMap(
        value: ParquetRecord,
        group: GroupNode,
        columns: inout [ShreddedColumn],
        leafIndex: Int,
        currentDef: Int32,
        currentRep: Int32
    ) -> Int {
        // Standard map: group(MAP) { repeated group key_value { key; value } }
        let defForMapPresent = group.repetition != .required ? currentDef + 1 : currentDef

        guard let repeatedGroup = group.children.first as? GroupNode else { return leafIndex }
        let defForNonEmpty = defForMapPresent + 1
        let maxRepForMap = Int32(repeatedGroup.maxRepetitionLevel)

        switch value {
        case .null:
            var idx = leafIndex
            for child in repeatedGroup.children {
                idx = emitNulls(schemaNode: child, columns: &columns,
                                leafIndex: idx, defLevel: currentDef, repLevel: currentRep)
            }
            return idx

        case .map(let entries):
            if entries.isEmpty {
                var idx = leafIndex
                for child in repeatedGroup.children {
                    idx = emitNulls(schemaNode: child, columns: &columns,
                                    leafIndex: idx, defLevel: defForMapPresent, repLevel: currentRep)
                }
                return idx
            }

            var idx = leafIndex
            for (i, entry) in entries.enumerated() {
                let rep = (i == 0) ? currentRep : maxRepForMap
                idx = leafIndex
                // First child is key, second is value
                if repeatedGroup.children.count >= 1 {
                    idx = shredNode(value: entry.key, schemaNode: repeatedGroup.children[0],
                                    columns: &columns, leafIndex: idx,
                                    currentDef: defForNonEmpty, currentRep: rep)
                }
                if repeatedGroup.children.count >= 2 {
                    idx = shredNode(value: entry.value, schemaNode: repeatedGroup.children[1],
                                    columns: &columns, leafIndex: idx,
                                    currentDef: defForNonEmpty, currentRep: rep)
                }
            }
            return idx

        default:
            return leafIndex
        }
    }

    /// Emit null entries for all leaves under a schema node.
    private static func emitNulls(
        schemaNode: any SchemaNode,
        columns: inout [ShreddedColumn],
        leafIndex: Int,
        defLevel: Int32,
        repLevel: Int32
    ) -> Int {
        if schemaNode is PrimitiveNode {
            guard leafIndex < columns.count else { return leafIndex + 1 }
            columns[leafIndex].defLevels.append(defLevel)
            columns[leafIndex].repLevels.append(repLevel)
            return leafIndex + 1
        }
        guard let group = schemaNode as? GroupNode else { return leafIndex }
        var idx = leafIndex
        if group.convertedType == .list || group.convertedType == .map {
            if let repeated = group.children.first as? GroupNode {
                for child in repeated.children {
                    idx = emitNulls(schemaNode: child, columns: &columns,
                                    leafIndex: idx, defLevel: defLevel, repLevel: repLevel)
                }
            }
        } else {
            for child in group.children {
                idx = emitNulls(schemaNode: child, columns: &columns,
                                leafIndex: idx, defLevel: defLevel, repLevel: repLevel)
            }
        }
        return idx
    }

    private static func buildPath(_ node: PrimitiveNode) -> [String] {
        var path = [node.name]
        var current: GroupNode? = node.parent
        while let p = current {
            if p.parent != nil { path.insert(p.name, at: 0) }
            current = p.parent
        }
        return path
    }
}

// MARK: - Dremel Assembler

struct DremelAssembler {

    /// Assemble flat columns with def/rep levels back into nested ParquetRecord rows.
    static func assemble(
        columns: [ShreddedColumn],
        schema: GroupNode,
        numRows: Int
    ) -> [ParquetRecord] {
        guard !columns.isEmpty else { return [] }

        // Find row boundaries using repetition levels of the first column
        let firstCol = columns[0]
        var rowBoundaries = [Int]()
        for (i, rep) in firstCol.repLevels.enumerated() {
            if rep == 0 { rowBoundaries.append(i) }
        }

        var cursors = [Int](repeating: 0, count: columns.count)
        var valueCursors = [Int](repeating: 0, count: columns.count)
        var rows = [ParquetRecord]()

        for rowIdx in 0..<rowBoundaries.count {
            let start = rowBoundaries[rowIdx]
            let end = rowIdx + 1 < rowBoundaries.count ? rowBoundaries[rowIdx + 1] : firstCol.repLevels.count

            let row = assembleGroup(columns: columns, schema: schema,
                                     cursors: &cursors, valueCursors: &valueCursors,
                                     start: start, end: end)
            rows.append(row)
        }

        return rows
    }

    private static func assembleGroup(
        columns: [ShreddedColumn],
        schema: GroupNode,
        cursors: inout [Int],
        valueCursors: inout [Int],
        start: Int,
        end: Int
    ) -> ParquetRecord {
        var fields = [(String, ParquetRecord)]()

        var leafIdx = 0
        for child in schema.children {
            let numLeaves = countLeaves(child)

            if let prim = child as? PrimitiveNode {
                let colIdx = leafIdx
                guard colIdx < columns.count else {
                    leafIdx += numLeaves
                    continue
                }
                let col = columns[colIdx]
                if cursors[colIdx] < col.defLevels.count {
                    let def = col.defLevels[cursors[colIdx]]
                    let maxDef = Int32(prim.maxDefinitionLevel)
                    cursors[colIdx] += 1
                    if def == maxDef {
                        let val = valueCursors[colIdx] < col.values.count ?
                            col.values[valueCursors[colIdx]] : .null
                        valueCursors[colIdx] += 1
                        fields.append((child.name, val))
                    } else {
                        fields.append((child.name, .null))
                    }
                }
            } else if let group = child as? GroupNode {
                if group.convertedType == .list {
                    let list = assembleList(columns: columns, group: group,
                                             cursors: &cursors, valueCursors: &valueCursors,
                                             leafIdx: leafIdx)
                    fields.append((child.name, list))
                } else if group.convertedType == .map {
                    let map = assembleMap(columns: columns, group: group,
                                           cursors: &cursors, valueCursors: &valueCursors,
                                           leafIdx: leafIdx)
                    fields.append((child.name, map))
                } else {
                    // Nested struct
                    let struct_ = assembleGroup(columns: columns, schema: group,
                                                 cursors: &cursors, valueCursors: &valueCursors,
                                                 start: start, end: end)
                    fields.append((child.name, struct_))
                }
            }
            leafIdx += numLeaves
        }

        return .struct(fields)
    }

    private static func assembleList(
        columns: [ShreddedColumn],
        group: GroupNode,
        cursors: inout [Int],
        valueCursors: inout [Int],
        leafIdx: Int
    ) -> ParquetRecord {
        guard let repeatedGroup = group.children.first as? GroupNode,
              let elementNode = repeatedGroup.children.first else {
            return .null
        }

        let colIdx = leafIdx
        guard colIdx < columns.count else { return .null }
        let col = columns[colIdx]
        guard cursors[colIdx] < col.defLevels.count else { return .null }

        let maxRepForList = Int32(repeatedGroup.maxRepetitionLevel)
        let defForListPresent = group.repetition != .required ? Int32(1) : Int32(0)
        let firstDef = col.defLevels[cursors[colIdx]]

        // Null list
        if firstDef < defForListPresent {
            cursors[colIdx] += 1
            return .null
        }

        // Empty list
        let defForNonEmpty = defForListPresent + 1
        if firstDef < defForNonEmpty {
            cursors[colIdx] += 1
            return .list([])
        }

        // Non-empty list: collect elements until rep level drops
        var elements = [ParquetRecord]()
        var isFirst = true

        while cursors[colIdx] < col.defLevels.count {
            let rep = col.repLevels[cursors[colIdx]]
            if !isFirst && rep < maxRepForList { break }
            isFirst = false

            let def = col.defLevels[cursors[colIdx]]
            let maxDef = elementNode is PrimitiveNode ?
                Int32((elementNode as! PrimitiveNode).maxDefinitionLevel) :
                Int32(defForNonEmpty + 1)

            cursors[colIdx] += 1
            if def >= maxDef {
                let val = valueCursors[colIdx] < col.values.count ?
                    col.values[valueCursors[colIdx]] : .null
                valueCursors[colIdx] += 1
                elements.append(val)
            } else {
                elements.append(.null)
            }
        }

        return .list(elements)
    }

    private static func assembleMap(
        columns: [ShreddedColumn],
        group: GroupNode,
        cursors: inout [Int],
        valueCursors: inout [Int],
        leafIdx: Int
    ) -> ParquetRecord {
        guard let repeatedGroup = group.children.first as? GroupNode,
              repeatedGroup.children.count >= 2 else { return .null }

        let keyColIdx = leafIdx
        let valColIdx = leafIdx + 1
        guard keyColIdx < columns.count && valColIdx < columns.count else { return .null }

        let keyCol = columns[keyColIdx]
        guard cursors[keyColIdx] < keyCol.defLevels.count else { return .null }

        let maxRepForMap = Int32(repeatedGroup.maxRepetitionLevel)
        let defForPresent = group.repetition != .required ? Int32(1) : Int32(0)
        let firstDef = keyCol.defLevels[cursors[keyColIdx]]

        if firstDef < defForPresent {
            cursors[keyColIdx] += 1
            cursors[valColIdx] += 1
            return .null
        }

        let defForNonEmpty = defForPresent + 1
        if firstDef < defForNonEmpty {
            cursors[keyColIdx] += 1
            cursors[valColIdx] += 1
            return .map([])
        }

        var entries = [(key: ParquetRecord, value: ParquetRecord)]()
        var isFirst = true

        while cursors[keyColIdx] < keyCol.defLevels.count {
            let rep = keyCol.repLevels[cursors[keyColIdx]]
            if !isFirst && rep < maxRepForMap { break }
            isFirst = false

            // Read key
            let keyDef = keyCol.defLevels[cursors[keyColIdx]]
            cursors[keyColIdx] += 1
            let keyMaxDef = Int32((repeatedGroup.children[0] as! PrimitiveNode).maxDefinitionLevel)
            let key: ParquetRecord
            if keyDef >= keyMaxDef {
                key = valueCursors[keyColIdx] < keyCol.values.count ?
                    keyCol.values[valueCursors[keyColIdx]] : .null
                valueCursors[keyColIdx] += 1
            } else {
                key = .null
            }

            // Read value
            let valCol = columns[valColIdx]
            let valDef = valCol.defLevels[cursors[valColIdx]]
            cursors[valColIdx] += 1
            let valMaxDef = Int32((repeatedGroup.children[1] as! PrimitiveNode).maxDefinitionLevel)
            let value: ParquetRecord
            if valDef >= valMaxDef {
                value = valueCursors[valColIdx] < valCol.values.count ?
                    valCol.values[valueCursors[valColIdx]] : .null
                valueCursors[valColIdx] += 1
            } else {
                value = .null
            }

            entries.append((key: key, value: value))
        }

        return .map(entries)
    }

    private static func countLeaves(_ node: any SchemaNode) -> Int {
        if node is PrimitiveNode { return 1 }
        guard let group = node as? GroupNode else { return 0 }
        if group.convertedType == .list || group.convertedType == .map {
            guard let repeated = group.children.first as? GroupNode else { return 0 }
            return repeated.children.reduce(0) { $0 + countLeaves($1) }
        }
        return group.children.reduce(0) { $0 + countLeaves($1) }
    }
}
