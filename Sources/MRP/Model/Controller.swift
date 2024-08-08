//
// Copyright (c) 2024 PADL Software Pty Ltd
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// MRP is a simple, fully distributed, many-to-many protocol, that supports
// efficient, reliable, and rapid declaration and registration of attributes by
// multiple participants on shared and virtual shared media. MRP also
// incorporates optimizations to speed attribute declarations and withdrawals
// on point-to-point media. Correctness of MRP operation is independent of the
// relative values of protocol timers, and the protocol design is based
// primarily on the exchange of idempotent protocol state rather than commands.

import AsyncExtensions
import Logging
import ServiceLifecycle

public actor Controller<P: Port>: Service, CustomStringConvertible {
  typealias MAPContextDictionary = [MAPContextIdentifier: MAPContext<P>]

  let bridge: any Bridge<P>
  let logger: Logger
  var ports: Set<P> { Set(_ports.values) }

  private var _applications = [UInt16: any Application<P>]()
  private var _ports = [P.ID: P]()
  private var _periodicTimers = [P.ID: Timer]()
  private var _administrativeControl = AdministrativeControl.normalParticipant
  private var _taskGroup: ThrowingTaskGroup<(), Error>?
  private let _rxPackets: AnyAsyncSequence<(P.ID, IEEE802Packet)>

  private var periodicTransmissionTime: Duration {
    .seconds(1)
  }

  var leaveAllTime: Duration {
    let leaveAllTime = Double.random(in: LeaveAllTime..<(1.5 * LeaveAllTime))
    return Duration.seconds(leaveAllTime)
  }

  public init(bridge: some Bridge<P>, logger: Logger) async throws {
    logger.debug("initializing MRP controller with bridge \(bridge)")
    self.bridge = bridge
    self.logger = logger
    _ports = try await [P.ID: P](uniqueKeysWithValues: bridge.getPorts().map { ($0.id, $0) })
    _rxPackets = try bridge.rxPackets
  }

  public nonisolated var description: String {
    "Controller(bridge: \(bridge))"
  }

  private func _run() async throws {
    logger.info("starting \(self)")
    for port in ports {
      try? await _didAdd(port: port)
    }

    try bridge.willRun(ports: ports)

    try await withThrowingTaskGroup(of: Void.self) { group in
      _taskGroup = group
      group.addTask { @Sendable in try await self._handleBridgeNotifications() }
      group.addTask { @Sendable in try await self._handleRxPackets() }
      for try await _ in group {}
    }
  }

  private func _shutdown() {
    logger.info("shutting down \(self)")
    try? bridge.willShutdown()
    for port in ports {
      try? _didRemove(port: port)
    }
    _taskGroup?.cancelAll()
  }

  public func run() async throws {
    try await cancelWhenGracefulShutdown {
      try await self.run()
    }
    _shutdown()
  }

  var knownContextIdentifiers: Set<MAPContextIdentifier> {
    Set([MAPBaseSpanningTreeContext] + bridge.vlans.map { MAPContextIdentifier(vlan: $0) })
  }

  func context(for contextIdentifier: MAPContextIdentifier) -> MAPContext<P> {
    if contextIdentifier == MAPBaseSpanningTreeContext {
      ports
    } else {
      ports.filter { port in
        port.vlans.contains(VLAN(contextIdentifier: contextIdentifier))
      }
    }
  }

  var knownContexts: MAPContextDictionary {
    MAPContextDictionary(uniqueKeysWithValues: knownContextIdentifiers.map { contextIdentifier in
      (contextIdentifier, context(for: contextIdentifier))
    })
  }

  private func _applyContextIdentifierChanges(
    beforeAddingOrUpdating port: P,
    isNewPort: Bool
  ) async throws {
    let addedContextIdentifiers: Set<MAPContextIdentifier>
    let removedContextIdentifiers: Set<MAPContextIdentifier>
    let updatedContextIdentifiers: Set<MAPContextIdentifier>

    if let existingPort = ports.first(where: { $0.id == port.id }) {
      addedContextIdentifiers = port.contextIdentifiers.subtracting(existingPort.contextIdentifiers)
      removedContextIdentifiers = existingPort.contextIdentifiers
        .subtracting(port.contextIdentifiers)
      updatedContextIdentifiers = existingPort.contextIdentifiers
        .intersection(port.contextIdentifiers)
    } else {
      addedContextIdentifiers = port.contextIdentifiers
      removedContextIdentifiers = []
      updatedContextIdentifiers = []
    }

    precondition(!addedContextIdentifiers.contains(MAPBaseSpanningTreeContext))
    precondition(!removedContextIdentifiers.contains(MAPBaseSpanningTreeContext))

    logger.trace("""
    applying context identifier changes prior to \(isNewPort ? "adding" : "updating") port \(port):
    removed \(removedContextIdentifiers) updated \(updatedContextIdentifiers) added \(
      addedContextIdentifiers
    )
    """)

    for contextIdentifier in removedContextIdentifiers {
      try _didRemove(contextIdentifier: contextIdentifier, with: [port])
    }

    for contextIdentifier in updatedContextIdentifiers
      .union(isNewPort ? [] : [MAPBaseSpanningTreeContext])
    {
      try _didUpdate(contextIdentifier: contextIdentifier, with: [port])
    }

    for contextIdentifier in addedContextIdentifiers
      .union(isNewPort ? [MAPBaseSpanningTreeContext] : [])
    {
      try await _didAdd(contextIdentifier: contextIdentifier, with: [port])
    }
  }

  private func _applyContextIdentifierChanges(beforeRemoving port: P) throws {
    let removedContextIdentifiers: Set<MAPContextIdentifier>

    guard let existingPort = ports.first(where: { $0.id == port.id }) else { return }
    removedContextIdentifiers = existingPort.contextIdentifiers.subtracting(port.contextIdentifiers)

    logger
      .trace(
        "applying context identifier changes prior to removing port \(port): \(removedContextIdentifiers)"
      )

    for contextIdentifier in [MAPBaseSpanningTreeContext] + removedContextIdentifiers {
      try _didRemove(contextIdentifier: contextIdentifier, with: [port])
    }
  }

  private func _didAdd(port: P) async throws {
    logger.debug("added port \(port)")

    _startTx(port: port)

    try await _applyContextIdentifierChanges(beforeAddingOrUpdating: port, isNewPort: true)
    _ports[port.id] = port
  }

  private func _didRemove(port: P) throws {
    logger.debug("removed port \(port)")

    _stopTx(port: port)

    try _applyContextIdentifierChanges(beforeRemoving: port)
    _ports[port.id] = nil
  }

  private func _didUpdate(port: P) async throws {
    logger.debug("updated port \(port)")

    try await _applyContextIdentifierChanges(beforeAddingOrUpdating: port, isNewPort: false)
    _ports[port.id] = port
  }

  private func _handleBridgeNotifications() async throws {
    for try await notification in bridge.notifications {
      do {
        switch notification {
        case let .added(port):
          try await ports.contains(port) ? _didUpdate(port: port) : _didAdd(port: port)
        case let .removed(port):
          try _didRemove(port: port)
        case let .changed(port):
          try await _didUpdate(port: port)
        }
      } catch {
        logger
          .info("failed to handle bridge notification on \(notification.port): \(error)")
      }
    }
  }

  private func _handleRxPackets() async throws {
    for try await (id, packet) in _rxPackets {
      // find the application for this ethertype
      guard let application = _applications[packet.etherType], let port = _ports[id] else {
        logger.info("application \(packet.etherType) on port \(id) not found, skipping")
        continue
      }
      try await application.rx(packet: packet, from: port)
    }
  }

  func periodicEnabled() {
    logger.trace("enabled periodic timer")
    _periodicTimers.forEach { $0.value.start(interval: periodicTransmissionTime) }
  }

  func periodicDisabled() {
    logger.trace("disabled periodic timer")
    _periodicTimers.forEach { $0.value.stop() }
  }

  typealias MADApplyFunction = (any Application<P>) throws -> ()

  private func _apply(_ block: MADApplyFunction) rethrows {
    for application in _applications {
      try block(application.value)
    }
  }

  typealias AsyncMADApplyFunction = (any Application<P>) async throws -> ()

  @Sendable
  private func _apply(_ block: AsyncMADApplyFunction) async rethrows {
    for application in _applications {
      try await block(application.value)
    }
  }

  typealias MADContextSpecificApplyFunction<T> = (any Application<P>) throws -> (T) throws -> ()

  private func _apply<T>(
    with arg: T,
    _ block: MADContextSpecificApplyFunction<T>
  ) rethrows {
    try _apply { application in
      try block(application)(arg)
    }
  }

  typealias AsyncMADContextSpecificApplyFunction<T> = (any Application<P>) throws
    -> (T) async throws -> ()

  private func _apply<T>(
    with arg: T,
    _ block: AsyncMADContextSpecificApplyFunction<T>
  ) async rethrows {
    try await _apply { application in
      try await block(application)(arg)
    }
  }

  deinit {
    _taskGroup?.cancelAll()
  }

  func register(application: some Application<P>) throws {
    guard _applications[application.etherType] == nil
    else { throw MRPError.applicationAlreadyRegistered }
    try bridge.register(groupAddress: application.groupAddress, etherType: application.etherType)
    _applications[application.etherType] = application
    logger.info("registered application \(application)")
  }

  func deregister(application: some Application<P>) throws {
    guard _applications[application.etherType] == nil
    else { throw MRPError.unknownApplication }
    _applications.removeValue(forKey: application.etherType)
    try? bridge.deregister(
      groupAddress: application.groupAddress,
      etherType: application.etherType
    )
    logger.info("deregistered application \(application)")
  }

  private func _didAdd(
    contextIdentifier: MAPContextIdentifier,
    with context: MAPContext<P>
  ) async throws {
    for application in _applications.values {
      logger
        .trace("added MAP context \(contextIdentifier):\(context) for application \(application)")
      try await application.didAdd(contextIdentifier: contextIdentifier, with: context)
    }
  }

  private func _didUpdate(
    contextIdentifier: MAPContextIdentifier,
    with context: MAPContext<P>
  ) throws {
    for application in _applications.values {
      logger
        .trace("added MAP context  \(contextIdentifier):\(context) for application \(application)")
      try application.didUpdate(contextIdentifier: contextIdentifier, with: context)
    }
  }

  private func _didRemove(
    contextIdentifier: MAPContextIdentifier,
    with context: MAPContext<P>
  ) throws {
    for application in _applications.values {
      logger
        .trace(
          "removed MAP context  \(contextIdentifier):\(context) for application \(application)"
        )
      try application.didRemove(contextIdentifier: contextIdentifier, with: context)
    }
  }

  private func _startTx(port: P) {
    logger.debug("controller starting TX on port \(port)")
    var periodicTimer = _periodicTimers[port.id]
    if periodicTimer == nil {
      periodicTimer = Timer {
        try await self._apply { @Sendable application in
          try await application.periodic()
        }
      }
    }
    periodicTimer!.start(interval: periodicTransmissionTime)
  }

  private func _stopTx(port: P) {
    logger.debug("controller stopping TX on port \(port)")
    _periodicTimers[port.id]?.stop()
  }
}
