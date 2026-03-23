// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "SwiftParquet",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [
        .library(name: "SwiftParquet", targets: ["SwiftParquet"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-crypto.git", from: "3.0.0"),
    ],
    targets: [
        .target(
            name: "SwiftParquet",
            dependencies: [
                .product(name: "Crypto", package: "swift-crypto",
                         condition: .when(platforms: [.linux])),
            ],
            path: "Sources/SwiftParquet"
        ),
        .systemLibrary(
            name: "CZlib",
            pkgConfig: "zlib",
            providers: [.apt(["zlib1g-dev"]), .brew(["zlib"])]
        ),
        .testTarget(
            name: "SwiftParquetTests",
            dependencies: ["SwiftParquet"],
            path: "Tests/SwiftParquetTests",
            resources: [.copy("Resources")]
        ),
    ]
)
