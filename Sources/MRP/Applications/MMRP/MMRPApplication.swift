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

import Logging

protocol MMRPAwareBridge<P>: Bridge where P: Port {
  func registerMulticastAddress(_ macAddress: EUI48, on ports: Set<P>) async throws
  func deregisterMulticastAddress(_ macAddress: EUI48, from ports: Set<P>) async throws
}

public final class MMRPApplication<P: Port>: BaseApplication, BaseApplicationDelegate,
  Sendable where P == P
{
  var _delegate: (any BaseApplicationDelegate<P>)? { self }

  // for now, we only operate in the Base Spanning Tree Context
  var _contextsSupported: Bool { false }

  func set(logger: Logger) {
    _logger.withCriticalRegion { $0 = logger }
  }

  public var validAttributeTypes: ClosedRange<AttributeType> {
    MMRPAttributeType.validAttributeTypes
  }

  // 10.12.1.3 MMRP application address
  public var groupAddress: EUI48 { CustomerBridgeMRPGroupAddress }

  // 10.12.1.4 MMRP application EtherType
  public var etherType: UInt16 { 0x88F6 }

  // 10.12.1.5 MMRP ProtocolVersion
  public var protocolVersion: ProtocolVersion { 0 }

  let _mad: Weak<Controller<P>>

  public var mad: Controller<P>? { _mad.object }

  let _participants =
    ManagedCriticalState<[MAPContextIdentifier: Set<Participant<MMRPApplication<P>>>]>([:])
  let _logger = ManagedCriticalState<Logger?>(nil)

  init(owner: Controller<P>) {
    _mad = Weak(owner)
  }

  public func deserialize(
    attributeOfType attributeType: AttributeType,
    from deserializationContext: inout DeserializationContext
  ) throws -> any Value {
    guard let attributeType = MMRPAttributeType(rawValue: attributeType)
    else { throw MRPError.unknownAttributeType }
    switch attributeType {
    case .macVector:
      return try MMRPMACVectorValue(deserializationContext: &deserializationContext)
    case .serviceRequirementVector:
      return try MMRPServiceRequirementValue(deserializationContext: &deserializationContext)
    }
  }

  public func makeValue(for attributeType: AttributeType, at index: Int) throws -> any Value {
    guard let attributeType = MMRPAttributeType(rawValue: attributeType)
    else { throw MRPError.unknownAttributeType }
    switch attributeType {
    case .macVector:
      return try MMRPMACVectorValue(index: index)
    case .serviceRequirementVector:
      return try MMRPServiceRequirementValue(index: index)
    }
  }

  public func packedEventsType(for attributeType: AttributeType) throws -> PackedEventsType {
    .threePackedType
  }

  public func administrativeControl(for attributeType: AttributeType) throws
    -> AdministrativeControl
  {
    .normalParticipant
  }

  public func register(macAddress: EUI48) async throws {
    try await join(
      attributeType: MMRPAttributeType.macVector.rawValue,
      attributeValue: MMRPMACVectorValue(macAddress: macAddress),
      isNew: true,
      for: MAPBaseSpanningTreeContext
    )
  }

  public func deregister(macAddress: EUI48) async throws {
    try await leave(
      attributeType: MMRPAttributeType.macVector.rawValue,
      attributeValue: MMRPMACVectorValue(macAddress: macAddress),
      for: MAPBaseSpanningTreeContext
    )
  }

  public func register(
    serviceRequirement requirementSpecification: MMRPServiceRequirementValue
  ) async throws {
    try await join(
      attributeType: MMRPAttributeType.serviceRequirementVector.rawValue,
      attributeValue: requirementSpecification,
      isNew: true,
      for: MAPBaseSpanningTreeContext
    )
  }

  public func deregister(
    serviceRequirement requirementSpecification: MMRPServiceRequirementValue
  ) async throws {
    try await leave(
      attributeType: MMRPAttributeType.serviceRequirementVector.rawValue,
      attributeValue: requirementSpecification,
      for: MAPBaseSpanningTreeContext
    )
  }
}

extension MMRPApplication {
  // these are not called because only the base spanning tree context is supported
  // at present
  func onContextAdded(
    contextIdentifier: MAPContextIdentifier,
    with context: MAPContext<P>
  ) throws {}

  func onContextUpdated(
    contextIdentifier: MAPContextIdentifier,
    with context: MAPContext<P>
  ) throws {}

  func onContextRemoved(
    contextIdentifier: MAPContextIdentifier,
    with context: MAPContext<P>
  ) throws {}

  // On receipt of a MAD_Join.indication, the MMRP application element
  // specifies the Port associated with the MMRP Participant as Forwarding in
  // the Port Map field of the MAC Address Registration Entry (8.8.4) for the
  // MAC address specification carried in the attribute_value parameter and the
  // VID associated with the MAP Context. If such a MAC Address Registration
  // Entry does not exist in the FDB, a new MAC Address Registration Entry is
  // created.
  func onJoinIndication(
    contextIdentifier: MAPContextIdentifier,
    attributeType: AttributeType,
    attributeValue: some Value,
    isNew: Bool
  ) async throws {
    guard let mad else { throw MRPError.internalError }
    guard let bridge = mad.bridge as? any MMRPAwareBridge<P> else { return }
    let ports = await mad.context(for: contextIdentifier)
    guard let attributeType = MMRPAttributeType(rawValue: attributeType)
    else { throw MRPError.unknownAttributeType }
    switch attributeType {
    case .macVector:
      let macAddress = (attributeValue as! MMRPMACVectorValue).macAddress
      guard _isMulticast(macAddress: macAddress) else { throw MRPError.invalidAttributeValue }
      try await bridge.registerMulticastAddress(macAddress, on: ports)
    case .serviceRequirementVector:
      break
    }
  }

  // On receipt of a MAD_Leave.indication, the MMRP application element
  // specifies the Port associated with the MMRP Participant as Filtering in
  // the Port Map field of the MAC Address Registration Entry (8.8.4) for the
  // MAC address specification carried in the attribute_value parameter and the
  // VID associated with the MAP Context. If such an FDB entry does not exist
  // in the FDB, then the indication is ignored. If setting that Port to
  // Filtering results in there being no Ports in the Port Map specified as
  // Forwarding (i.e., all MMRP members are deregistered), then that MAC
  // Address Registration Entry is removed from the FDB.
  func onLeaveIndication(
    contextIdentifier: MAPContextIdentifier,
    attributeType: AttributeType,
    attributeValue: some Value
  ) async throws {
    guard let mad else { throw MRPError.internalError }
    guard let bridge = mad.bridge as? any MMRPAwareBridge<P> else { return }
    let ports = await mad.context(for: contextIdentifier)
    guard let attributeType = MMRPAttributeType(rawValue: attributeType)
    else { throw MRPError.unknownAttributeType }
    switch attributeType {
    case .macVector:
      let macAddress = (attributeValue as! MMRPMACVectorValue).macAddress
      guard _isMulticast(macAddress: macAddress) else { throw MRPError.invalidAttributeValue }
      try await bridge.deregisterMulticastAddress(macAddress, from: ports)
    case .serviceRequirementVector:
      break
    }
  }
}
