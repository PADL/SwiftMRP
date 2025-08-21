// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let PlatformPackageDependencies: [Package.Dependency]
let PlatformTargetDependencies: [Target.Dependency]
let PlatformProducts: [Product]
let PlatformTargets: [Target]
var PlatformCSettings: [CSetting] = []
var PlatformSwiftSettings: [SwiftSetting] = []
var PlatformLinkerSettings: [LinkerSetting] = []

PlatformSwiftSettings += [
  .swiftLanguageMode(.v5),
  .enableExperimentalFeature("StrictConcurrency"),
]

#if os(Linux)
PlatformCSettings += [.unsafeFlags(["-I", "/usr/include/libnl3"])]

PlatformPackageDependencies = [
  .package(
    url: "https://github.com/PADL/IORingSwift",
    from: "0.1.2"
  ),
  .package(
    url: "https://github.com/PADL/NetLinkSwift",
    branch: "main"
  ),
  .package(url: "https://github.com/xtremekforever/swift-systemd", from: "0.2.1"),
]

PlatformTargetDependencies = [
  .product(
    name: "NetLink",
    package: "NetLinkSwift"
  ),
  .product(name: "nldump", package: "NetLinkSwift"),
  .product(name: "nlmonitor", package: "NetLinkSwift"),
  .product(name: "nltool", package: "NetLinkSwift"),
  .product(
    name: "IORing",
    package: "IORingSwift"
  ),
  .product(
    name: "IORingUtils",
    package: "IORingSwift"
  ),
]

PlatformProducts = [
  .executable(
    name: "mrpd",
    targets: ["MRPDaemon"]
  ),
  .executable(
    name: "portmon",
    targets: ["portmon"]
  ),
  .executable(
    name: "pmctool",
    targets: ["pmctool"]
  ),
]
PlatformTargets = [
  .executableTarget(
    name: "MRPDaemon",
    dependencies: [
      "MRP",
      .product(name: "ArgumentParser", package: "swift-argument-parser"),
      .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
      .product(name: "Systemd", package: "swift-systemd", condition: .when(platforms: [.linux])),
      .product(
        name: "SystemdLifecycle",
        package: "swift-systemd",
        condition: .when(platforms: [.linux])
      ),
    ],
    cSettings: PlatformCSettings,
    swiftSettings: PlatformSwiftSettings
  ),
  .executableTarget(
    name: "portmon",
    dependencies: ["MRP"],
    path: "Examples/portmon",
    cSettings: PlatformCSettings,
    swiftSettings: PlatformSwiftSettings
  ),
  .executableTarget(
    name: "pmctool",
    dependencies: ["PMC"],
    path: "Examples/pmctool",
    cSettings: PlatformCSettings,
    swiftSettings: PlatformSwiftSettings
  ),
]

#elseif os(macOS) || os(iOS)
PlatformPackageDependencies = []
PlatformTargetDependencies = []
PlatformProducts = []
PlatformTargets = []
#endif

let CommonPackageDependencies: [Package.Dependency] = [
  .package(url: "https://github.com/apple/swift-async-algorithms", from: "1.0.0"),
  .package(url: "https://github.com/apple/swift-log", from: "1.6.2"),
  .package(url: "https://github.com/apple/swift-algorithms", from: "1.2.0"),
  .package(url: "https://github.com/apple/swift-system", from: "1.2.1"),
  .package(url: "https://github.com/apple/swift-argument-parser", from: "1.2.0"),
  .package(url: "https://github.com/PADL/SocketAddress", from: "0.0.1"),
  .package(url: "https://github.com/lhoward/AsyncExtensions", from: "0.9.2"),
  .package(url: "https://github.com/swift-server/swift-service-lifecycle", from: "2.3.0"),
  .package(url: "https://github.com/ChimeHQ/JSONRPC.git", from: "0.9.2"),
]

let CommonProducts: [Product] = [
  .library(
    name: "MRP",
    targets: ["MRP"]
  ),
]

let CommonTargets: [Target] = [
  .target(
    name: "IEEE802",
    dependencies: [
      .product(name: "SystemPackage", package: "swift-system"),
    ]
  ),
  .target(
    name: "PMC",
    dependencies: [
      "IEEE802",
      "AsyncExtensions",
      "SocketAddress",
      .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
      .product(name: "SystemPackage", package: "swift-system"),
    ] + PlatformTargetDependencies,
    cSettings: PlatformCSettings,
    swiftSettings: PlatformSwiftSettings,
    linkerSettings: PlatformLinkerSettings
  ),
  .target(
    name: "MRP",
    dependencies: [
      "IEEE802",
      "AsyncExtensions",
      "SocketAddress",
      "PMC",
      "MRPControlProtocol",
      .product(name: "Algorithms", package: "swift-algorithms"),
      .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
      .product(name: "Logging", package: "swift-log"),
      .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
      .product(name: "SystemPackage", package: "swift-system"),
    ] + PlatformTargetDependencies,
    cSettings: PlatformCSettings,
    swiftSettings: PlatformSwiftSettings,
    linkerSettings: PlatformLinkerSettings
  ),
  .target(
    name: "MRPControlProtocol",
    dependencies: [
      "IEEE802",
      "AsyncExtensions",
      "SocketAddress",
      .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
      .product(name: "SystemPackage", package: "swift-system"),
      "JSONRPC",
    ] + PlatformTargetDependencies,
    cSettings: PlatformCSettings,
    swiftSettings: PlatformSwiftSettings,
    linkerSettings: PlatformLinkerSettings
  ),
  .testTarget(
    name: "MRPTests",
    dependencies: ["MRP"],
    cSettings: PlatformCSettings,
    swiftSettings: PlatformSwiftSettings,
    linkerSettings: PlatformLinkerSettings
  ),
]

let package = Package(
  name: "SwiftMRP",
  platforms: [
    .macOS(.v15),
  ],
  products: CommonProducts + PlatformProducts,
  dependencies: CommonPackageDependencies + PlatformPackageDependencies,
  targets: CommonTargets + PlatformTargets
)
