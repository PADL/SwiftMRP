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
import IEEE802
import IORing
import IORingUtils
import NetLink
import SocketAddress
import SystemPackage

private func _mapUPToSRClassPriority(_ up: UInt8) -> SRclassPriority {
  guard let srClassPriority = SRclassPriority(rawValue: up) else {
    return SRclassPriority.BE // best effort
  }
  return srClassPriority
}

private func _mapSRClassPriorityToUP(_ srClassPriority: SRclassPriority) -> UInt8 {
  srClassPriority.rawValue
}

private func _mapTCToSRClassID(_ tc: UInt8) -> SRclassID? {
  guard tc <= SRclassID.A.rawValue else {
    return nil
  }

  return SRclassID(rawValue: SRclassID.A.rawValue - tc)
}

private func _mapSRClassIDToTC(_ srClassID: SRclassID) -> UInt8 {
  SRclassID.A.rawValue - srClassID.rawValue
}

private func _makeLinkLayerAddress(
  family: sa_family_t = sa_family_t(AF_PACKET),
  macAddress: EUI48? = nil,
  etherType: UInt16 = UInt16(ETH_P_ALL),
  packetType: UInt8 = 0,
  index: Int? = nil
) -> sockaddr_ll {
  var sll = sockaddr_ll()
  sll.sll_family = UInt16(family)
  sll.sll_protocol = etherType.bigEndian
  sll.sll_ifindex = CInt(index ?? 0)
  sll.sll_pkttype = packetType
  if let macAddress {
    sll.sll_halen = UInt8(ETH_ALEN)
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
  packetType: UInt8 = 0,
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

private func _getEthChannelCount(name: String) throws -> (Int, Int) {
  let fileHandle = try FileHandle(
    fileDescriptor: socket(CInt(AF_PACKET), Int32(SOCK_DGRAM.rawValue), 0),
    closeOnDealloc: true
  )

  var channels = ethtool_channels()
  channels.cmd = UInt32(ETHTOOL_GCHANNELS)
  try withUnsafeMutablePointer(to: &channels) {
    try $0.withMemoryRebound(to: CChar.self, capacity: MemoryLayout<ethtool_channels>.size) {
      var ifr = ifreq()
      ifr.ifr_ifru.ifru_data = UnsafeMutablePointer($0)
      // note: we are trusting here that the string is ASCII and not UTF-8
      try withUnsafePointer(to: ifr.ifr_ifrn.ifrn_name) {
        let baseAddress = $0.propertyBasePointer(to: \.0)!
        guard name.count < Int(IFNAMSIZ) else { throw Errno.outOfRange }
        memcpy(UnsafeMutableRawPointer(mutating: baseAddress), name, name.count + 1)
      }

      if ioctl(fileHandle.fileDescriptor, UInt(SIOCETHTOOL), &ifr) < 0 {
        throw Errno(rawValue: errno)
      }
    }
  }

  return (Int(channels.rx_count), Int(channels.tx_count))
}

public struct LinuxPort: Port, Sendable, CustomStringConvertible {
  public static func timeSinceEpoch() throws -> UInt32 {
    var tv = timeval()
    guard gettimeofday(&tv, nil) == 0 else {
      throw Errno(rawValue: errno)
    }
    return UInt32(tv.tv_sec)
  }

  public typealias ID = Int

  public static func == (_ lhs: LinuxPort, _ rhs: LinuxPort) -> Bool {
    lhs.id == rhs.id
  }

  fileprivate let _rtnl: RTNLLink
  fileprivate let _txSocket: Socket
  fileprivate weak var _bridge: LinuxBridge?

  init(rtnl: RTNLLink, bridge: LinuxBridge) throws {
    _rtnl = rtnl
    _bridge = bridge
    _txSocket = try Socket(ring: IORing.shared, domain: sa_family_t(AF_PACKET), type: SOCK_RAW)
    // shouldn't need to join multicast group if we are only sending packets
  }

  public var description: String {
    "LinuxPort(name: \(name), id: \(id))"
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

  public var mtu: UInt {
    _rtnl.mtu
  }

  public var linkSpeed: UInt {
    1_000_000
  }
}

extension LinuxPort: AVBPort {
  public var isAvbCapable: Bool {
    // TODO: may need to revisit this
    guard let channelCount = try? _getEthChannelCount(name: name) else { return false }
    return channelCount.1 > 2
  }

  public func getPortTcMaxLatency(for: SRclassPriority) -> Int {
    500
  }
}

private extension LinuxPort {
  var _vlans: Set<UInt16>? {
    (_rtnl as? RTNLLinkBridge)?.bridgeTaggedVLANs
  }

  var _untaggedVlans: Set<UInt16>? {
    (_rtnl as? RTNLLinkBridge)?.bridgeUntaggedVLANs
  }

  var _pvid: UInt16? {
    (_rtnl as? RTNLLinkBridge)?.bridgePVID
  }

  func _add(vlan: VLAN) async throws {
    guard let rtnl = _rtnl as? RTNLLinkBridge else { throw Errno.noSuchAddressOrDevice }

    var flags = Int32(0)

    if _untaggedVlans?.contains(vlan.vid) ?? false {
      // preserve untagging status, this may not be on spec but saves blowing away management
      // interface
      flags |= BRIDGE_VLAN_INFO_UNTAGGED
    }
    if _pvid == vlan.vid {
      flags |= BRIDGE_VLAN_INFO_PVID
    }

    try await rtnl.add(vlans: Set([vlan.vid]), flags: UInt16(flags), socket: _bridge!._nlLinkSocket)
  }

  func _remove(vlan: VLAN) async throws {
    guard let rtnl = _rtnl as? RTNLLinkBridge else { throw Errno.noSuchAddressOrDevice }

    try await rtnl.remove(vlans: Set([vlan.vid]), socket: _bridge!._nlLinkSocket)
  }
}

public final class LinuxBridge: Bridge, CustomStringConvertible, @unchecked
Sendable {
  public typealias Port = LinuxPort

  fileprivate let _nlLinkSocket: NLSocket
  private let _nlNfLog: NFNLLog
  private let _nlQDiscHandle: Int?
  private var _nlNfLogMonitorTask: Task<(), Error>!
  private var _nlLinkMonitorTask: Task<(), Error>!
  private let _bridgeName: String
  private var _bridgeIndex: Int = 0
  private var _bridgePort: Port?
  private let _portNotificationChannel = AsyncChannel<PortNotification<Port>>()
  private let _rxPacketsChannel = AsyncThrowingChannel<(Port.ID, IEEE802Packet), Error>()
  private var _linkLocalRegistrations = Set<FilterRegistration>()
  private var _linkLocalRxTasks = [LinkLocalRXTaskKey: Task<(), Error>]()
  private let _srClassPriorityMapNotificationChannel =
    AsyncChannel<SRClassPriorityMapNotification<Port>>()

  public init(name: String, netFilterGroup group: Int, qDiscHandle: Int? = nil) throws {
    _bridgeName = name
    _nlLinkSocket = try NLSocket(protocol: NETLINK_ROUTE)
    _nlNfLog = try NFNLLog(group: UInt16(group))
    _nlQDiscHandle = qDiscHandle
  }

  deinit {
    try? _shutdown()
  }

  public nonisolated var description: String {
    "LinuxBridge(name: \(_bridgeName))"
  }

  private func _handleLinkNotification(_ linkMessage: RTNLLinkMessage) throws {
    var portNotification: PortNotification<Port>?
    let port = try Port(rtnl: linkMessage.link, bridge: self)
    if port._isBridgeSelf, port._rtnl.index == _bridgeIndex {
      if case .new = linkMessage {
        _bridgePort = port
      } else {
        debugPrint("LinuxBridge: bridge device itself removed")
        throw Errno.noSuchAddressOrDevice
      }
    } else if port._rtnl.master == _bridgeIndex {
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
    if let portNotification {
      Task { await _portNotificationChannel.send(portNotification) }
    }
  }

  public var notifications: AnyAsyncSequence<PortNotification<Port>> {
    _portNotificationChannel.eraseToAnyAsyncSequence()
  }

  private func _handleTCNotification(_ tcMessage: RTNLTCMessage) throws {
    // all we are really interested is in SR class remappings
    guard let qdisc = tcMessage.tc as? RTNLMQPrioQDisc,
          let _nlQDiscHandle,
          qdisc.parent == _nlQDiscHandle,
          let srClassPriorityMap = qdisc.srClassPriorityMap else { return }
    let tcNotification: SRClassPriorityMapNotification<Port> = if case .new = tcMessage {
      .added(srClassPriorityMap)
    } else {
      .removed(srClassPriorityMap)
    }
    Task { await _srClassPriorityMapNotificationChannel.send(tcNotification) }
  }

  public var defaultPVid: UInt16? {
    _bridgePort?._pvid
  }

  public func getVlans(controller: isolated MRPController<Port>) async -> Set<VLAN> {
    if let vlans = _bridgePort?._vlans {
      Set(vlans.map { VLAN(vid: $0) })
    } else {
      Set()
    }
  }

  public var name: String {
    _bridgePort!.name
  }

  private func _getPorts(family: sa_family_t) async throws -> Set<Port> {
    try await Set(
      _nlLinkSocket.getLinks(family: family).map { try Port(rtnl: $0, bridge: self) }
        .collect()
    )
  }

  private func _getMemberPorts() async throws -> Set<Port> {
    try await _getPorts(family: sa_family_t(AF_BRIDGE)).filter {
      !$0._isBridgeSelf && $0._rtnl.master == _bridgeIndex
    }
  }

  private func _getBridgePort(name: String) async throws -> Port {
    let bridgePorts = try await _getPorts(family: sa_family_t(AF_BRIDGE))
      .filter { $0._isBridgeSelf && $0.name == name }
    guard bridgePorts.count == 1 else {
      throw MRPError.invalidBridgeIdentity
    }
    return bridgePorts.first!
  }

  @_spi(SwiftMRPPrivate)
  public var bridgePort: Port {
    _bridgePort!
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
      repeat {
        do {
          for try await packet in try await filterRegistration._rxPackets(port: port) {
            await _rxPacketsChannel.send((port.id, packet))
          }
        } catch Errno.interrupted {} // restart on interrupted system call
      } while !Task.isCancelled
    }
  }

  private func _addLinkLocalRxTask(port: Port) throws {
    precondition(!port._isBridgeSelf)
    for filterRegistration in _linkLocalRegistrations {
      let key = LinkLocalRXTaskKey(portID: port.id, filterRegistration: filterRegistration)
      _linkLocalRxTasks[key] = _allocateLinkLocalRxTask(
        port: port,
        filterRegistration: filterRegistration
      )
      debugPrint(
        "LinuxBridge: started link-local RX task for \(port) filter registration \(filterRegistration)"
      )
    }
  }

  private func _cancelLinkLocalRxTask(port: Port) throws {
    precondition(!port._isBridgeSelf)
    try _cancelLinkLocalRxTask(portID: port.id)
  }

  private func _cancelLinkLocalRxTask(portID: Port.ID) throws {
    for filterRegistration in _linkLocalRegistrations {
      let key = LinkLocalRXTaskKey(portID: portID, filterRegistration: filterRegistration)
      guard let index = _linkLocalRxTasks.index(forKey: key) else { continue }
      let task = _linkLocalRxTasks[index].value
      _linkLocalRxTasks.remove(at: index)
      task.cancel()
      debugPrint(
        "LinuxBridge: removed link-local RX task for \(portID) filter registration \(filterRegistration)"
      )
    }
  }

  public func register(
    groupAddress: EUI48,
    etherType: UInt16,
    controller: isolated MRPController<Port>
  ) async throws {
    guard _isLinkLocal(macAddress: groupAddress) else { return }
    _linkLocalRegistrations.insert(FilterRegistration(
      groupAddress: groupAddress,
      etherType: etherType
    ))
  }

  public func deregister(
    groupAddress: EUI48,
    etherType: UInt16,
    controller: isolated MRPController<Port>
  ) async throws {
    guard _isLinkLocal(macAddress: groupAddress) else { return }
    _linkLocalRegistrations.remove(FilterRegistration(
      groupAddress: groupAddress,
      etherType: etherType
    ))
  }

  public func run(controller: isolated MRPController<Port>) async throws {
    _bridgePort = try await _getBridgePort(name: _bridgeName)
    _bridgeIndex = _bridgePort!._rtnl.index

    try _nlLinkSocket.subscribeLinks()
    try _nlLinkSocket.subscribeTC()

    _nlLinkMonitorTask = Task<(), Error> { [self] in
      for try await notification in _nlLinkSocket.notifications {
        do {
          switch notification {
          case let linkNotification as RTNLLinkMessage:
            try _handleLinkNotification(linkNotification)
          case let tcNotification as RTNLTCMessage:
            try _handleTCNotification(tcNotification)
          default:
            break
          }
        } catch Errno.noSuchAddressOrDevice {
          throw Errno.noSuchAddressOrDevice
        } catch {}
      }
    }
    _nlNfLogMonitorTask = Task<(), Error> { [self] in
      for try await packet in _nfNlLogRxPackets {
        await _rxPacketsChannel.send(packet)
      }
    }

    let ports = try await _getMemberPorts()
    for port in ports {
      await _portNotificationChannel.send(.added(port))
      try _addLinkLocalRxTask(port: port)
    }
  }

  private func _shutdown() throws {
    let portIDs = _linkLocalRxTasks.keys.map(\.portID)
    for portID in portIDs {
      try? _cancelLinkLocalRxTask(portID: portID)
    }

    _nlNfLogMonitorTask?.cancel()
    _nlLinkMonitorTask?.cancel()

    try? _nlLinkSocket.unsubscribeTC()
    try? _nlLinkSocket.unsubscribeLinks()

    _bridgePort = nil
    _bridgeIndex = 0
  }

  public func shutdown(controller: isolated MRPController<Port>) async throws {
    try _shutdown()
  }

  private func _add(vlan: VLAN) async throws {
    guard let _bridgePort else { throw MRPError.internalError }
    try await _bridgePort._add(vlan: vlan)
  }

  private func _remove(vlan: VLAN) async throws {
    guard let _bridgePort else { throw MRPError.internalError }
    try await _bridgePort._remove(vlan: vlan)
  }

  public func tx(
    _ packet: IEEE802Packet,
    on port: P,
    controller: isolated MRPController<Port>
  ) async throws {
    let address = _makeLinkLayerAddressBytes(
      macAddress: packet.destMacAddress,
      etherType: packet.etherType,
      index: port.id
    )

    var serializationContext = SerializationContext()
    try packet.serialize(into: &serializationContext)
    try await port._txSocket.sendMessage(.init(
      name: address,
      buffer: serializationContext.bytes
    ))
  }

  public var rxPackets: AnyAsyncSequence<(P.ID, IEEE802Packet)> {
    _rxPacketsChannel.eraseToAnyAsyncSequence()
  }

  private var _nfNlLogRxPackets: AnyAsyncSequence<(P.ID, IEEE802Packet)> {
    _nlNfLog.logMessages.compactMap { logMessage in
      guard let hwHeader = logMessage.hwHeader, let payload = logMessage.payload,
            let packet = try? IEEE802Packet(hwHeader: hwHeader, payload: payload)
      else {
        return nil
      }
      return (logMessage.physicalInputDevice, packet)
    }.eraseToAnyAsyncSequence()
  }
}

fileprivate final class FilterRegistration: Equatable, Hashable, Sendable, CustomStringConvertible {
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

  var description: String {
    "FilterRegistration(_groupAddress: \(_macAddressToString(_groupAddress)), _etherType: \(String(format: "0x%04x", _etherType)))"
  }

  func _rxPackets(port: LinuxPort) async throws -> AnyAsyncSequence<IEEE802Packet> {
    precondition(_isLinkLocal(macAddress: _groupAddress))
    let rxSocket = try Socket(
      ring: IORing.shared,
      domain: sa_family_t(AF_PACKET),
      type: SOCK_RAW,
      protocol: CInt(_etherType.bigEndian)
    )
    try rxSocket.bind(to: _makeLinkLayerAddress(
      macAddress: port.macAddress,
      etherType: _etherType,
      packetType: UInt8(PACKET_MULTICAST),
      index: port.id
    ))
    try rxSocket.addMulticastMembership(for: _makeLinkLayerAddress(
      macAddress: _groupAddress,
      index: port.id
    ))

    return try await rxSocket.receiveMessages(count: Int(port._rtnl.mtu)).compactMap { message in
      var deserializationContext = DeserializationContext(message.buffer)
      return try? IEEE802Packet(deserializationContext: &deserializationContext)
    }.eraseToAnyAsyncSequence()
  }
}

extension LinuxBridge: MMRPAwareBridge {
  func register(groupAddress: EUI48, vlan: VLAN?, on ports: Set<P>) async throws {
    guard let rtnl = bridgePort._rtnl as? RTNLLinkBridge else { throw Errno.noSuchAddressOrDevice }

    for port in ports {
      try await rtnl.add(
        link: port._rtnl,
        groupAddresses: [groupAddress],
        vlanID: vlan?.vid,
        socket: _nlLinkSocket
      )
    }
  }

  func deregister(groupAddress: EUI48, vlan: VLAN?, from ports: Set<P>) async throws {
    guard let rtnl = bridgePort._rtnl as? RTNLLinkBridge else { throw Errno.noSuchAddressOrDevice }

    for port in ports {
      try await rtnl.remove(
        link: port._rtnl,
        groupAddresses: [groupAddress],
        vlanID: vlan?.vid,
        socket: _nlLinkSocket
      )
    }
  }

  func register(
    serviceRequirement requirementSpecification: MMRPServiceRequirementValue,
    on ports: Set<P>
  ) async throws {}

  func deregister(
    serviceRequirement requirementSpecification: MMRPServiceRequirementValue,
    from ports: Set<P>
  ) async throws {}
}

extension LinuxBridge: MVRPAwareBridge {
  func register(vlan: VLAN, on ports: Set<P>) async throws {
    try await _add(vlan: vlan)
    for port in ports {
      try await port._add(vlan: vlan)
    }
  }

  func deregister(vlan: VLAN, from ports: Set<P>) async throws {
    for port in ports {
      try await port._remove(vlan: vlan)
    }
    try await _remove(vlan: vlan)
  }
}

extension LinuxBridge: MSRPAwareBridge {
  func adjustCreditBasedShaper(
    port: Port,
    srClass: SRclassID,
    idleSlope: Int,
    sendSlope: Int,
    hiCredit: Int,
    loCredit: Int
  ) async throws {
    guard let parent = _nlQDiscHandle else {
      throw MSRPFailure(systemID: port.systemID, failureCode: .egressPortIsNotAvbCapable)
    }
    try await port._rtnl.add(
      handle: 1 + UInt32(_mapSRClassIDToTC(srClass)),
      parent: UInt32(parent),
      hiCredit: Int32(hiCredit),
      loCredit: Int32(loCredit),
      idleSlope: Int32(idleSlope),
      sendSlope: Int32(sendSlope),
      socket: _nlLinkSocket
    )
  }

  func getSRClassPriorityMap(port: P) async throws -> SRClassPriorityMap? {
    let qDiscs = try await _nlLinkSocket.getQDiscs(
      family: sa_family_t(AF_UNSPEC),
      interfaceIndex: port.id
    )
    guard let qDisc = try await qDiscs.collect().compactMap({ $0 as? RTNLMQPrioQDisc }).first else {
      return nil
    }
    guard let parent = _nlQDiscHandle, qDisc.parent == parent else {
      return nil
    }
    return qDisc.srClassPriorityMap?.1
  }

  var srClassPriorityMapNotifications: AnyAsyncSequence<SRClassPriorityMapNotification<Port>> {
    _srClassPriorityMapNotificationChannel.eraseToAnyAsyncSequence()
  }
}

fileprivate extension RTNLMQPrioQDisc {
  var srClassPriorityMap: (LinuxPort.ID, SRClassPriorityMap)? {
    guard let priorityMap else { return nil }
    return (index, SRClassPriorityMap(uniqueKeysWithValues: priorityMap.compactMap { up, tc in
      guard let srClassID = _mapTCToSRClassID(tc) else { return nil }
      let srClassPriority = _mapUPToSRClassPriority(up)
      return (srClassID, srClassPriority)
    }))
  }
}

fileprivate extension UnsafePointer {
  func propertyBasePointer<Property>(to property: KeyPath<Pointee, Property>)
    -> UnsafePointer<Property>?
  {
    guard let offset = MemoryLayout<Pointee>.offset(of: property) else { return nil }
    return (UnsafeRawPointer(self) + offset).assumingMemoryBound(to: Property.self)
  }
}
#endif
