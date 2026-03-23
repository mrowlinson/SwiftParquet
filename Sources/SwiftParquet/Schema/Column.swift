// Column.swift — ColumnDescriptor: path, type, def/rep levels for a leaf column
// Port of github.com/apache/arrow-go/parquet/schema/column.go

import Foundation

/// Describes a single leaf column in the Parquet schema.
public struct ColumnDescriptor: Sendable {
    /// The leaf primitive node.
    public let node: PrimitiveNode

    /// Full dotted path from the root, e.g. "address.city"
    public let path: [String]

    /// Maximum definition level for this column (0 = required, ≥1 = optional/repeated).
    public let maxDefinitionLevel: Int16

    /// Maximum repetition level for this column (0 = not repeated).
    public let maxRepetitionLevel: Int16

    public var name: String { node.name }
    public var physicalType: PhysicalType { node.physicalType }

    init(node: PrimitiveNode) {
        self.node = node
        // Build path: walk up the tree collecting names (excluding root "schema")
        var pathComponents: [String] = [node.name]
        var current: GroupNode? = node.parent
        while let p = current {
            if p.parent != nil {  // skip the root GroupNode
                pathComponents.insert(p.name, at: 0)
            }
            current = p.parent
        }
        self.path = pathComponents
        self.maxDefinitionLevel = node.maxDefinitionLevel
        self.maxRepetitionLevel = node.maxRepetitionLevel
    }
}
