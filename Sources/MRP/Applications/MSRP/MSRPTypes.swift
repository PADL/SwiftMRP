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

enum MSRPPortMediaType {
  case accessControlPort
  case nonDMNSharedMediumPort
}

enum MSRPDirection {
  case talker
  case listener
}

enum MSRPDeclarationType {
  case talkerAdvertise
  case talkerFailed
  case listenerAskingFailed
  case listenerReady
  case listenerReadyFailed
}

typealias MSRPTrafficClass = Int
typealias MSRPPortLatency = Int
public typealias MSRPSRClass = UInt8

enum MSRPProtocolVersion: ProtocolVersion {
  case v0 = 0
  case v1 = 1
}

struct MSRPPortParameters {
  let enabled: Bool
  let tcMaxLatency: [MSRPTrafficClass: MSRPPortLatency]
  let streamAge: UInt32
  let srpDomainBoundaryPort: [MSRPSRClass: Bool]
  let neighborProtocolVersion: MSRPProtocolVersion
  let talkerPruning: Bool
  let talkerVlanPruning: Bool
}

enum TSNFailureCode: UInt8, Error, SerDes, Equatable {
  case insufficientBandwidth = 1
  case insufficientBridgeResources = 2
  case insufficientBandwidthForTrafficClass = 3
  case streamIDAlreadyInUse = 4
  case streamDestinationAddressAlreadyInUse = 5
  case streamPreemptedByHigherRank = 6
  case reportedLatencyHasChanged = 7
  case egressPortIsNotAvbCapable = 8
  case useDifferentDestinationAddress = 9
  case outOfMSRPResources = 10
  case outOfMMRPResources = 11
  case cannotStoreDestinationAddress = 12
  case requestedPriorityIsNotAnSRClassPriority = 13
  case maxFrameSizeTooLargeForMedia = 14
  case fanInPortLimitReached = 15
  case changeInFirstValueForRegisteredStreamID = 16
  case vlanBlockedOnEgressPort = 17
  case vlanTaggingDisabledOnEgressPort = 18
  case srClassPriorityMismatch = 19
  case enhancedFeatureCannotBePropagated = 20
  case maxLatencyExceeded = 21
  case nearestBridgeNetworkIdentificationFailed = 22
  case streamTransformationNotSupported = 23
  case streamIDTypeNotSupportedForTransformation = 24
  case enhancedFeatureRequiresCNC = 25

  func serialize(into serializationContext: inout SerializationContext) throws {
    serializationContext.serialize(uint8: rawValue)
  }

  init(deserializationContext: inout DeserializationContext) throws {
    guard let value = Self(rawValue: try deserializationContext.deserialize()) else {
      throw MRPError.invalidFailureCode
    }
    self = value
  }
}

struct TSpec: SerDes, Equatable {
  let maxFrameSize: UInt16
  let maxIntervalFrames: UInt16

  init() {
    self.init(maxFrameSize: 0, maxIntervalFrames: 0)
  }

  init(maxFrameSize: UInt16, maxIntervalFrames: UInt16) {
    self.maxFrameSize = maxFrameSize
    self.maxIntervalFrames = maxIntervalFrames
  }

  func serialize(into serializationContext: inout SerializationContext) throws {
    serializationContext.serialize(uint16: maxFrameSize)
    serializationContext.serialize(uint16: maxIntervalFrames)
  }

  init(deserializationContext: inout DeserializationContext) throws {
    try self.init(
      maxFrameSize: deserializationContext.deserialize(),
      maxIntervalFrames: deserializationContext.deserialize()
    )
  }
}

struct DataFrameParameters: Value, Equatable {
  let destinationAddress: EUI48
  let vlanIdentifier: VLAN

  private var _value: UInt64 {
    UInt64(destinationAddress.0 << 56) |
      UInt64(destinationAddress.1 << 48) |
      UInt64(destinationAddress.2 << 40) |
      UInt64(destinationAddress.3 << 32) |
      UInt64(destinationAddress.4 << 24) |
      UInt64(destinationAddress.5 << 16)
  }

  var index: Int {
    // FIXME: broken on 32-bit systems
    Int(bitPattern: UInt(_value))
  }

  static func == (lhs: DataFrameParameters, rhs: DataFrameParameters) -> Bool {
    _isEqualMacAddress(lhs.destinationAddress, rhs.destinationAddress) && lhs.vlanIdentifier == rhs
      .vlanIdentifier
  }

  func serialize(into serializationContext: inout SerializationContext) throws {
    serializationContext.serialize([
      destinationAddress.0,
      destinationAddress.1,
      destinationAddress.2,
      destinationAddress.3,
      destinationAddress.4,
      destinationAddress.5,
    ])
    try vlanIdentifier.serialize(into: &serializationContext)
  }

  init(destinationAddress: EUI48, vlanIdentifier: VLAN) {
    self.destinationAddress = destinationAddress
    self.vlanIdentifier = vlanIdentifier
  }

  init(deserializationContext: inout DeserializationContext) throws {
    destinationAddress.0 = try deserializationContext.deserialize()
    destinationAddress.1 = try deserializationContext.deserialize()
    destinationAddress.2 = try deserializationContext.deserialize()
    destinationAddress.3 = try deserializationContext.deserialize()
    destinationAddress.4 = try deserializationContext.deserialize()
    destinationAddress.5 = try deserializationContext.deserialize()
    vlanIdentifier = try VLAN(deserializationContext: &deserializationContext)
  }

  private init(_ value: UInt64) {
    destinationAddress = (
      UInt8((value >> 56) & 0xFF),
      UInt8((value >> 48) & 0xFF),
      UInt8((value >> 40) & 0xFF),
      UInt8((value >> 32) & 0xFF),
      UInt8((value >> 24) & 0xFF),
      UInt8((value >> 16) & 0xFF)
    )
    vlanIdentifier = VLAN(vid: UInt16(value & 0x1FFF))
  }

  init() {
    self.init(0)
  }

  init(firstValue: Self?, index: Int) throws {
    let value: UInt64
    if let firstValue {
      value = firstValue._value + UInt64(index)
    } else {
      value = UInt64(index)
    }
    self.init(value)
  }
}

struct PriorityAndRank: SerDes, Equatable {
  let value: UInt8

  init(_ value: UInt8 = 0) {
    self.value = value
  }

  init(dataFramePriority: SRclassPriority, rank: Bool) {
    value = dataFramePriority.rawValue << 5 | (rank ? 0x10 : 0x00)
  }

  init(deserializationContext: inout DeserializationContext) throws {
    value = try deserializationContext.deserialize()
  }

  func serialize(into serializationContext: inout SerializationContext) throws {
    serializationContext.serialize(uint8: value)
  }

  var dataFramePriority: SRclassPriority {
    IEEE802Packet.TCI.PCP(rawValue: UInt8((value & 0xE0) >> 5))!
  }

  var rank: Bool {
    value & 0x10 != 0
  }
}

enum SRclassID: UInt8 {
  case A = 6
  case B = 5
  case C = 4
  case D = 3
  case E = 2
  case F = 1
  case G = 0
}

typealias SRclassPriority = IEEE802Packet.TCI.PCP

typealias SRclassVID = VLAN.ID
