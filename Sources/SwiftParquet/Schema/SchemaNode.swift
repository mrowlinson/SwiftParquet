// SchemaNode.swift — Parquet schema tree: GroupNode and PrimitiveNode
// Port of github.com/apache/arrow-go/parquet/schema/schema.go
//
// The Parquet schema is a tree of SchemaElement nodes flattened to a list via DFS.
// GroupNode is an intermediate node (has children); PrimitiveNode is a leaf (has a PhysicalType).
// For flat (non-nested) schemas, the root is a GroupNode and all children are PrimitiveNodes.

import Foundation

// MARK: - Schema Node Protocol

public protocol SchemaNode: Sendable {
    var name: String { get }
    var repetition: Repetition { get }
    var parent: GroupNode? { get }

    /// Maximum definition level: how many optional/repeated ancestors.
    var maxDefinitionLevel: Int16 { get }

    /// Maximum repetition level: how many repeated ancestors.
    var maxRepetitionLevel: Int16 { get }
}

// MARK: - PrimitiveNode

public final class PrimitiveNode: SchemaNode, @unchecked Sendable {
    public let name: String
    public let repetition: Repetition
    public let physicalType: PhysicalType
    public let convertedType: ConvertedType?
    public let logicalType: LogicalTypeThrift?
    public let typeLength: Int32?  // for FIXED_LEN_BYTE_ARRAY

    public private(set) weak var parent: GroupNode?

    public var maxDefinitionLevel: Int16 {
        var level: Int16 = 0
        var node: SchemaNode = self
        while let p = node.parent {
            if p.repetition != .required { level += 1 }
            node = p
        }
        if repetition != .required { level += 1 }
        return level
    }

    public var maxRepetitionLevel: Int16 {
        var level: Int16 = 0
        var node: SchemaNode = self
        while let p = node.parent {
            if p.repetition == .repeated { level += 1 }
            node = p
        }
        if repetition == .repeated { level += 1 }
        return level
    }

    public init(
        name: String,
        repetition: Repetition = .required,
        physicalType: PhysicalType,
        convertedType: ConvertedType? = nil,
        logicalType: LogicalTypeThrift? = nil,
        typeLength: Int32? = nil
    ) {
        self.name = name
        self.repetition = repetition
        self.physicalType = physicalType
        self.convertedType = convertedType
        self.logicalType = logicalType
        self.typeLength = typeLength
    }

    func setParent(_ parent: GroupNode) {
        self.parent = parent
    }

    /// Convert to a SchemaElement for Thrift serialization.
    func toSchemaElement() -> SchemaElement {
        SchemaElement(
            type: physicalType,
            typeLength: typeLength,
            repetitionType: repetition,
            name: name,
            numChildren: nil,
            convertedType: convertedType,
            logicalType: logicalType
        )
    }
}

// MARK: - GroupNode

public final class GroupNode: SchemaNode, @unchecked Sendable {
    public let name: String
    public let repetition: Repetition
    public private(set) var children: [any SchemaNode]
    public private(set) weak var parent: GroupNode?

    public var maxDefinitionLevel: Int16 {
        var level: Int16 = 0
        var node: SchemaNode = self
        while let p = node.parent {
            if p.repetition != .required { level += 1 }
            node = p
        }
        if repetition != .required { level += 1 }
        return level
    }

    public var maxRepetitionLevel: Int16 {
        var level: Int16 = 0
        var node: SchemaNode = self
        while let p = node.parent {
            if p.repetition == .repeated { level += 1 }
            node = p
        }
        if repetition == .repeated { level += 1 }
        return level
    }

    public init(name: String, repetition: Repetition = .required, children: [any SchemaNode] = []) {
        self.name = name
        self.repetition = repetition
        self.children = []
        self.addChildren(children)
    }

    func addChildren(_ newChildren: [any SchemaNode]) {
        for child in newChildren {
            if let prim = child as? PrimitiveNode {
                prim.setParent(self)
            } else if let grp = child as? GroupNode {
                grp.setParent(self)
            }
            children.append(child)
        }
    }

    func setParent(_ parent: GroupNode) {
        self.parent = parent
    }

    /// Flatten the schema tree to a list of SchemaElements via DFS (pre-order).
    func flattenToElements() -> [SchemaElement] {
        var elements: [SchemaElement] = []
        flattenInto(elements: &elements)
        return elements
    }

    private func flattenInto(elements: inout [SchemaElement]) {
        // Write this group node
        let groupElement = SchemaElement(
            type: nil,
            typeLength: nil,
            repetitionType: parent == nil ? nil : repetition,  // root has no repetition
            name: name,
            numChildren: Int32(children.count),
            convertedType: nil,
            logicalType: nil
        )
        elements.append(groupElement)

        // Recurse into children
        for child in children {
            if let prim = child as? PrimitiveNode {
                elements.append(prim.toSchemaElement())
            } else if let grp = child as? GroupNode {
                grp.flattenInto(elements: &elements)
            }
        }
    }

    /// All leaf (primitive) columns in DFS order — these become the column chunks.
    var leafColumns: [PrimitiveNode] {
        var result: [PrimitiveNode] = []
        for child in children {
            if let prim = child as? PrimitiveNode {
                result.append(prim)
            } else if let grp = child as? GroupNode {
                result.append(contentsOf: grp.leafColumns)
            }
        }
        return result
    }
}

// MARK: - Schema

/// Top-level schema: a GroupNode named "schema" at the root.
public final class ParquetSchema: Sendable {
    public let root: GroupNode

    public init(fields: [any SchemaNode]) {
        self.root = GroupNode(name: "schema", repetition: .required, children: fields)
    }

    /// Flatten to SchemaElement list for FileMetaData.schema.
    func toSchemaElements() -> [SchemaElement] {
        root.flattenToElements()
    }

    /// Leaf columns in DFS order.
    var columns: [PrimitiveNode] {
        root.leafColumns
    }

    var numColumns: Int { columns.count }
}
