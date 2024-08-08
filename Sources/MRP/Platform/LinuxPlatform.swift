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

#if os(Linux)

import AsyncAlgorithms
@preconcurrency
import AsyncExtensions
import CLinuxSockAddr
import CNetLink
import Glibc
import IORing
import IORingUtils
import NetLink
import SocketAddress
import SystemPackage

private func _makeLinkLayerAddress(
  family: sa_family_t = sa_family_t(AF_PACKET),
  macAddress: EUI48? = nil,
  etherType: UInt16 = UInt16(ETH_P_ALL),
  packetType: UInt8 = UInt8(PACKET_HOST),
  index: Int? = nil
) -> sockaddr_ll {
  var sll = sockaddr_ll()
  sll.sll_family = UInt16(family)
  sll.sll_protocol = etherType.bigEndian
  sll.sll_ifindex = CInt(index ?? 0)
  sll.sll_pkttype = packetType
  sll.sll_halen = UInt8(ETH_ALEN)
  if let macAddress {
    sll.sll_addr.0 = macAddress.0
    sll.sll_addr.1 = macAddress.1
    sll.sll_addr.2 = macAddress.2
    sll.sll_addr.3 = macAddress.3
    sll.sll_addr.4 = macAddress.4
    sll.sll_addr.5 = macAddress.5
  }
  return sll
}

private func _makeLinkLayerAddressBytes(
  family: sa_family_t = sa_family_t(AF_PACKET),
  macAddress: EUI48? = nil,
  etherType: UInt16 = UInt16(ETH_P_ALL),
  packetType: UInt8 = UInt8(PACKET_HOST),
  index: Int? = nil
) -> [UInt8] {
  var sll = _makeLinkLayerAddress(
    family: family,
    macAddress: macAddress,
    etherType: etherType,
    index: index
  )
  return withUnsafeBytes(of: &sll) {
    Array($0)
  }
}

public struct LinuxPort: Port, Sendable {
  public typealias ID = Int

  public static func == (_ lhs: LinuxPort, _ rhs: LinuxPort) -> Bool {
    lhs.id == rhs.id
  }

  fileprivate let _rtnl: RTNLLink
  fileprivate weak var _bridge: LinuxBridge?

  init(rtnl: RTNLLink, bridge: LinuxBridge) throws {
    _rtnl = rtnl
    _bridge = bridge
  }

  public func hash(into hasher: inout Hasher) {
    id.hash(into: &hasher)
  }

  public var isOperational: Bool {
    _rtnl.flags & IFF_RUNNING != 0
  }

  public var isEnabled: Bool {
    _rtnl.flags & IFF_UP != 0
  }

  public var isPointToPoint: Bool {
    _rtnl.flags & IFF_POINTOPOINT != 0
  }

  public var _isBridgeSelf: Bool {
    _isBridge && _rtnl.master == _rtnl.index
  }

  public var _isBridge: Bool {
    _rtnl is RTNLLinkBridge
  }

  public var _isVLAN: Bool {
    _rtnl is RTNLLinkVLAN
  }

  public var name: String {
    _rtnl.name
  }

  public var id: Int {
    _rtnl.index
  }

  public var macAddress: EUI48 {
    _rtnl.address
  }

  public var pvid: UInt16? {
    _pvid
  }

  public var vlans: Set<VLAN> {
    guard let vlans = _vlans else { return [] }
    return Set(vlans.map { VLAN(id: $0) })
  }
}

private extension LinuxPort {
  var _vlans: Set<UInt16>? {
    (_rtnl as? RTNLLinkBridge)?.bridgeTaggedVLANs
  }

  var _pvid: UInt16? {
    (_rtnl as? RTNLLinkBridge)?.bridgePVID
  }

  func _add(vlans: Set<VLAN>) async throws {
    try await (_rtnl as! RTNLLinkBridge).add(
      vlans: Set(vlans.map(\.vid)),
      socket: _bridge!._nlLinkSocket
    )
  }

  func _remove(vlans: Set<VLAN>) async throws {
    try await (_rtnl as! RTNLLinkBridge).remove(
      vlans: Set(vlans.map(\.vid)),
      socket: _bridge!._nlLinkSocket
    )
  }
}

public extension LinuxPort {
  func add(vlans: Set<VLAN>) async throws {
    try await _add(vlans: vlans)
  }

  func remove(vlans: Set<VLAN>) async throws {
    try await _remove(vlans: vlans)
  }
}

public final class LinuxBridge: Bridge, @unchecked Sendable {
  public typealias Port = LinuxPort

  private let _txSocket: Socket
  fileprivate let _nlLinkSocket: NLSocket
  private let _nlNfLog: NFNLLog
  private let _bridgePort = ManagedCriticalState<Port?>(nil)
  private var _task: Task<(), Error>!
  private let _portNotificationChannel = AsyncChannel<PortNotification<Port>>()
  private let _rxPacketsChannel = AsyncThrowingChannel<(Port.ID, IEEE802Packet), Error>()
  private let _linkLocalRegistrations = ManagedCriticalState<Set<FilterRegistration>>([])
  private let _linkLocalRxTasks = ManagedCriticalState<[LinkLocalRXTaskKey: Task<(), Error>]>([:])

  public init(name: String, netFilterGroup group: Int) async throws {
    _txSocket = try Socket(ring: IORing.shared, domain: sa_family_t(AF_PACKET), type: SOCK_RAW)
    _nlLinkSocket = try NLSocket(protocol: NETLINK_ROUTE)
    _nlNfLog = try NFNLLog(family: sa_family_t(AF_PACKET), group: UInt16(group))
    try _nlLinkSocket.subscribeLinks()
    let bridgePorts = try await _getPorts(family: sa_family_t(AF_BRIDGE))
      .filter { $0._isBridgeSelf && $0.name == name }
    guard bridgePorts.count == 1 else {
      throw MRPError.invalidBridgeIdentity
    }
    _bridgePort.withCriticalRegion { $0 = bridgePorts.first! }
    _task = Task<(), Error> { [self] in
      for try await notification in _nlLinkSocket.notifications {
        do {
          try await _handleLinkNotification(notification as! RTNLLinkMessage)
        } catch Errno.noSuchAddressOrDevice {
          throw Errno.noSuchAddressOrDevice
        } catch {}
      }
    }
  }

  private func _handleLinkNotification(_ linkMessage: RTNLLinkMessage) async throws {
    var portNotification: PortNotification<Port>?
    try _bridgePort.withCriticalRegion { bridgePort in
      let bridgeIndex = bridgePort!._rtnl.index
      let port = try Port(rtnl: linkMessage.link, bridge: self)
      if port._isBridgeSelf, port._rtnl.index == bridgeIndex {
        if case .new = linkMessage {
          bridgePort = port
        } else {
          debugPrint("LinuxBridge: bridge device itself removed")
          throw Errno.noSuchAddressOrDevice
        }
      } else if port._rtnl.master == bridgeIndex {
        if case .new = linkMessage {
          portNotification = .added(port)
          try _addLinkLocalRxTask(port: port)
        } else {
          try _cancelLinkLocalRxTask(port: port)
          portNotification = .removed(port)
        }
      } else {
        debugPrint("LinuxBridge: ignoring port \(port), not a member or us")
      }
    }
    if let portNotification {
      await _portNotificationChannel.send(portNotification)
    }
  }

  public var defaultPVid: UInt16? {
    _bridgePort.criticalState!._pvid
  }

  public var vlans: Set<VLAN> {
    if let vlans = _bridgePort.criticalState!._vlans {
      Set(vlans.map { VLAN(vid: $0) })
    } else {
      Set()
    }
  }

  public var name: String {
    _bridgePort.criticalState!.name
  }

  private func _getPorts(family: sa_family_t = sa_family_t(AF_UNSPEC)) async throws -> Set<Port> {
    try await Set(
      _nlLinkSocket.getLinks(family: family).map { try Port(rtnl: $0, bridge: self) }
        .collect()
    )
  }

  private var _bridgeIndex: Int {
    _bridgePort.criticalState!._rtnl.index
  }

  @_spi(SwiftMRPPrivate)
  public var bridgePort: Port {
    _bridgePort.criticalState!
  }

  private struct LinkLocalRXTaskKey: Hashable {
    let portID: Port.ID
    let filterRegistration: FilterRegistration
  }

  private func _allocateLinkLocalRxTask(
    port: Port,
    filterRegistration: FilterRegistration
  ) -> Task<(), Error> {
    Task {
      for try await packet in try await filterRegistration._rxPackets(port: port) {
        await _rxPacketsChannel.send((port.id, packet))
      }
    }
  }

  private func _addLinkLocalRxTask(port: Port) throws {
    let filterRegistrations = _linkLocalRegistrations.criticalState
    _linkLocalRxTasks.withCriticalRegion { linkLocalRxTasks in
      for filterRegistration in filterRegistrations {
        let key = LinkLocalRXTaskKey(portID: port.id, filterRegistration: filterRegistration)
        linkLocalRxTasks[key] = _allocateLinkLocalRxTask(
          port: port,
          filterRegistration: filterRegistration
        )
        debugPrint(
          "LinuxBridge: started link-local RX task for \(port) filter registration \(filterRegistration)"
        )
      }
    }
  }

  private func _cancelLinkLocalRxTask(port: Port) throws {
    let filterRegistrations = _linkLocalRegistrations.criticalState
    _linkLocalRxTasks.withCriticalRegion { linkLocalRxTasks in
      for filterRegistration in filterRegistrations {
        let key = LinkLocalRXTaskKey(portID: port.id, filterRegistration: filterRegistration)
        guard let index = linkLocalRxTasks.index(forKey: key) else { continue }
        let task = linkLocalRxTasks[index].value
        linkLocalRxTasks.remove(at: index)
        task.cancel()
        debugPrint(
          "LinuxBridge: removed link-local RX task for \(port) filter registration \(filterRegistration)"
        )
      }
    }
  }

  public func register(groupAddress: EUI48, etherType: UInt16) throws {
    guard _isLinkLocal(macAddress: groupAddress) else { return }
    _linkLocalRegistrations.withCriticalRegion {
      $0.insert(FilterRegistration(groupAddress: groupAddress, etherType: etherType))
    }
  }

  public func deregister(groupAddress: EUI48, etherType: UInt16) throws {
    guard _isLinkLocal(macAddress: groupAddress) else { return }
    _linkLocalRegistrations.withCriticalRegion {
      $0.remove(FilterRegistration(groupAddress: groupAddress, etherType: etherType))
    }
  }

  public func getPorts() async throws -> Set<Port> {
    let bridgeIndex = _bridgeIndex
    return try await _getPorts().filter {
      !$0._isBridgeSelf && $0._rtnl.master == bridgeIndex
    }
  }

  public var notifications: AnyAsyncSequence<PortNotification<Port>> {
    _nlLinkSocket.notifications.compactMap { @Sendable notification in
      let link = notification as! RTNLLinkMessage
      switch link {
      case let .new(link):
        return try .added(Port(rtnl: link, bridge: self))
      case let .del(link):
        return try .removed(Port(rtnl: link, bridge: self))
      }
    }.eraseToAnyAsyncSequence()
  }

  public func add(vlans: Set<VLAN>) async throws {
    if let bridgePort = _bridgePort.criticalState {
      try await bridgePort._add(vlans: vlans)
    }
  }

  public func remove(vlans: Set<VLAN>) async throws {
    if let bridgePort = _bridgePort.criticalState {
      try await bridgePort._remove(vlans: vlans)
    }
  }

  public func tx(_ packet: IEEE802Packet, on portID: P.ID) async throws {
    var serializationContext = SerializationContext()
    let packetType = packet.destMacAddress
      .0 & 0x1 != 0 ? UInt8(PACKET_MULTICAST) : UInt8(PACKET_HOST)
    let address = _makeLinkLayerAddressBytes(
      macAddress: packet.destMacAddress,
      packetType: packetType,
      index: portID
    )
    try packet.serialize(into: &serializationContext)
    try await _txSocket.sendMessage(.init(name: address, buffer: serializationContext.bytes))
  }

  public var rxPackets: AnyAsyncSequence<(P.ID, IEEE802Packet)> {
    _rxPacketsChannel.eraseToAnyAsyncSequence()
  }

  private var _nfLogRxPackets: AnyAsyncSequence<(P.ID, IEEE802Packet)> {
    get throws {
      _nlNfLog.logMessages.compactMap { logMessage in
        guard let hwHeader = logMessage.hwHeader, let payload = logMessage.payload else {
          return nil
        }
        let packet = try IEEE802Packet(hwHeader: hwHeader, payload: payload)
        return (logMessage.physicalInputDevice, packet)
      }.eraseToAnyAsyncSequence()
    }
  }
}

fileprivate final class FilterRegistration: Equatable, Hashable, Sendable {
  static func == (_ lhs: FilterRegistration, _ rhs: FilterRegistration) -> Bool {
    _isEqualMacAddress(lhs._groupAddress, rhs._groupAddress) && lhs._etherType == rhs._etherType
  }

  let _groupAddress: EUI48
  let _etherType: UInt16

  init(groupAddress: EUI48, etherType: UInt16) {
    _groupAddress = groupAddress
    _etherType = etherType
  }

  func hash(into hasher: inout Hasher) {
    _hashMacAddress(_groupAddress, into: &hasher)
    _etherType.hash(into: &hasher)
  }

  func _rxPackets(port: LinuxPort) async throws -> AnyAsyncSequence<IEEE802Packet> {
    precondition(_isLinkLocal(macAddress: _groupAddress))
    let socket = try Socket(
      ring: IORing.shared,
      domain: sa_family_t(AF_PACKET),
      type: SOCK_RAW,
      protocol: CInt(_etherType.bigEndian)
    )
    try socket.bind(to: _makeLinkLayerAddress(
      macAddress: port.macAddress,
      etherType: _etherType,
      packetType: UInt8(PACKET_MULTICAST),
      index: port.id
    ))
    try socket.addMulticastMembership(for: _makeLinkLayerAddress(
      macAddress: _groupAddress,
      index: port.id
    ))

    // TODO: caller needs to handle EINTR, make sure it does
    return try await socket.receiveMessages(count: port._rtnl.mtu).compactMap { message in
      var deserializationContext = DeserializationContext(message.buffer)
      return try? IEEE802Packet(deserializationContext: &deserializationContext)
    }.eraseToAnyAsyncSequence()
  }

  deinit {}
}

#endif
