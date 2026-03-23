// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "SwiftParquet",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [
        .library(name: "SwiftParquet", targets: ["SwiftParquet"]),
    ],
    targets: [
        .target(
            name: "SwiftParquet",
            path: "Sources/SwiftParquet"
        ),
        .testTarget(
            name: "SwiftParquetTests",
            dependencies: ["SwiftParquet"],
            path: "Tests/SwiftParquetTests",
            resources: [.copy("Resources")]
        ),
    ]
)
