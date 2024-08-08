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

import Algorithms

typealias ProtocolVersion = UInt8

// used by MSRP (not forwarded by bridges)
let CustomerBridgeMVRPGroupAddress: EUI48 = (0x01, 0x80, 0xC2, 0x00, 0x00, 0x0E)

// used by MVRP and MMRP (forwarded by bridges that do not support application protocol)
let IndividualLANScopeGroupAddress: EUI48 = (0x01, 0x80, 0xC2, 0x00, 0x00, 0x21)

struct ThreePackedEvents {
  let value: UInt8

  init(_ value: UInt8) {
    self.value = value
  }

  init(_ tuple: (UInt8, UInt8, UInt8)) {
    value = ((tuple.0 * 6) + tuple.1) * 6 + tuple.2
  }

  var tuple: (UInt8, UInt8, UInt8) {
    var value = value
    var r: (UInt8, UInt8, UInt8)
    r.0 = value / (6 * 6)
    value -= r.0 * (6 * 6)
    r.1 = value / 6
    value -= r.1 * 6
    r.2 = value
    return r
  }

  static func chunked(_ values: [UInt8]) -> [ThreePackedEvents] {
    let values = values.chunks(ofCount: 3)
    return values.map {
      ThreePackedEvents(($0[0], $0[safe: 1] ?? 0, $0[safe: 2] ?? 0))
    }
  }
}

struct FourPackedEvents {
  let value: UInt8

  init(_ value: UInt8) {
    self.value = value
  }

  init(_ tuple: (UInt8, UInt8, UInt8, UInt8)) {
    value = tuple.0 * 64 + tuple.1 * 16 + tuple.2 * 4 + tuple.3
  }

  var tuple: (UInt8, UInt8, UInt8, UInt8) {
    var value = value
    var r: (UInt8, UInt8, UInt8, UInt8)
    r.0 = value / 64
    value -= r.0 * 64
    r.1 = value / 16
    value -= r.1 * 16
    r.2 = value / 4
    value -= r.2 * 4
    r.3 = value
    return r
  }

  static func chunked(_ values: [UInt8]) -> [FourPackedEvents] {
    let values = values.chunks(ofCount: 4)
    return values.map {
      FourPackedEvents(($0[0], $0[safe: 1] ?? 0, $0[safe: 2] ?? 0, $0[safe: 3] ?? 0))
    }
  }
}

typealias NumberOfValues = UInt16 // Number of events encoded in the vector

let EndMark: UInt16 = 0

enum PackedEventsType {
  case threePackedType
  case fourPackedType
}

enum LeaveAllEvent: UInt8 {
  case NullLeaveAllEvent = 0
  case LeaveAll = 1
}

struct VectorHeader: Sendable, Equatable, Hashable, SerDes {
  let leaveAllEvent: LeaveAllEvent
  let numberOfValues: NumberOfValues

  init(leaveAllEvent: LeaveAllEvent, numberOfValues: NumberOfValues) {
    self.leaveAllEvent = leaveAllEvent
    self.numberOfValues = numberOfValues
  }

  init(deserializationContext: inout DeserializationContext) throws {
    let value: UInt16 = try deserializationContext.deserialize()
    guard let leaveAllEvent = LeaveAllEvent(rawValue: UInt8(value >> 13)) else {
      throw MRPError.invalidEnumerationCase
    }
    self.leaveAllEvent = leaveAllEvent
    numberOfValues = value & 0x1FFF
  }

  func serialize(into serializationContext: inout SerializationContext) throws {
    let value = UInt16(leaveAllEvent.rawValue << 13) | UInt16(numberOfValues & 0x1FFF)
    serializationContext.serialize(uint16: value)
  }
}

typealias AttributeType = UInt8
typealias AttributeLength = UInt8
typealias AttributeListLength = UInt16

struct VectorAttribute<V: Value>: Sendable, Equatable {
  static func == (lhs: VectorAttribute<V>, rhs: VectorAttribute<V>) -> Bool {
    guard lhs.vectorHeader == rhs.vectorHeader && lhs.vector == rhs.vector else {
      return false
    }

    return AnyValue(lhs.firstValue) == AnyValue(rhs.firstValue)
  }

  private let vectorHeader: VectorHeader

  // f) If the number of AttributeEvent values is zero, FirstValue is ignored
  // and the value is skipped. However, FirstValue is still present, and of
  // the correct length, in octets, as specified by the AttributeLength field
  // (10.8.2.3) for the Attribute to which the message applies.

  let firstValue: V
  let vector: [UInt8] // packed events

  var leaveAllEvent: LeaveAllEvent { vectorHeader.leaveAllEvent }
  var numberOfValues: NumberOfValues { vectorHeader.numberOfValues }

  init(vectorHeader: VectorHeader, firstValue: V, vector: [UInt8]) {
    self.vectorHeader = vectorHeader
    self.firstValue = firstValue
    self.vector = vector
  }

  init(
    leaveAllEvent: LeaveAllEvent,
    numberOfValues: NumberOfValues,
    firstValue: V,
    vector: [UInt8]
  ) {
    self.init(
      vectorHeader: VectorHeader(
        leaveAllEvent: leaveAllEvent,
        numberOfValues: numberOfValues
      ),
      firstValue: firstValue,
      vector: vector
    )
  }

  init(
    leaveAllEvent: LeaveAllEvent,
    numberOfValues: NumberOfValues,
    firstValue: V,
    vector: [ThreePackedEvents]
  ) {
    self.init(
      leaveAllEvent: leaveAllEvent,
      numberOfValues: numberOfValues,
      firstValue: firstValue,
      vector: vector.map(\.value)
    )
  }

  init(
    leaveAllEvent: LeaveAllEvent,
    numberOfValues: NumberOfValues,
    firstValue: V,
    vector: [FourPackedEvents]
  ) {
    self.init(
      leaveAllEvent: leaveAllEvent,
      numberOfValues: numberOfValues,
      firstValue: firstValue,
      vector: vector.map(\.value)
    )
  }

  init(
    attributeType: AttributeType,
    attributeLength: AttributeLength,
    deserializationContext: inout DeserializationContext,
    application: some Application
  ) throws where V == AnyValue {
    vectorHeader = try VectorHeader(deserializationContext: &deserializationContext)
    try deserializationContext.assertRemainingLength(isAtLeast: Int(attributeLength))
    firstValue = try AnyValue(application.deserialize(
      attributeOfType: attributeType,
      from: &deserializationContext
    ))
    vector = try Array(
      deserializationContext
        .deserialize(count: Int(vectorHeader.numberOfValues))
    )
  }

  func eraseToAny() -> VectorAttribute<AnyValue> {
    let anyFirstValue = AnyValue(firstValue)
    return VectorAttribute<AnyValue>(
      vectorHeader: vectorHeader,
      firstValue: anyFirstValue,
      vector: vector
    )
  }

  var threePackedEvents: [UInt8] {
    vector.flatMap {
      let tuple = ThreePackedEvents($0).tuple
      return [tuple.0, tuple.1, tuple.2]
    }
  }

  var fourPackedEvents: [UInt8] {
    vector.flatMap {
      let tuple = FourPackedEvents($0).tuple
      return [tuple.0, tuple.1, tuple.2, tuple.3]
    }
  }

  func serialize(into serializationContext: inout SerializationContext) throws
    -> AttributeLength
  {
    try vectorHeader.serialize(into: &serializationContext)
    let attributeLength: AttributeLength
    let oldPosition = serializationContext.position
    try firstValue.serialize(into: &serializationContext)
    attributeLength = AttributeLength(serializationContext.position - oldPosition)
    serializationContext.serialize(vector)
    return attributeLength
  }
}

struct Message: Serializable {
  typealias V = AnyValue

  let attributeType: AttributeType
  let attributeList: [VectorAttribute<V>]

  init(attributeType: AttributeType, attributeList: [VectorAttribute<V>]) {
    self.attributeType = attributeType
    self.attributeList = attributeList
  }

  init(
    deserializationContext: inout DeserializationContext,
    application: some Application
  ) throws {
    attributeType = try deserializationContext.deserialize()
    let attributeLength: AttributeLength = try deserializationContext.deserialize()
    let attributeListLength: AttributeListLength = try deserializationContext.deserialize()
    var attributeList = [VectorAttribute<V>]()
    for _ in 0..<attributeListLength {
      try attributeList.append(VectorAttribute<V>(
        attributeType: attributeType,
        attributeLength: attributeLength,
        deserializationContext: &deserializationContext,
        application: application
      ))
    }
    self.attributeList = attributeList
  }

  func serialize(into serializationContext: inout SerializationContext) throws {
    serializationContext.serialize(uint8: attributeType)
    let attributeLengthPosition = serializationContext.position
    serializationContext.serialize(uint8: AttributeLength(0))
    var attributeLength: AttributeLength = 0
    for attribute in attributeList {
      try attributeLength = attribute.serialize(into: &serializationContext)
    }
    serializationContext.serialize(uint8: attributeLength, at: attributeLengthPosition)
  }
}

struct MRPDU: Serializable {
  let protocolVersion: ProtocolVersion
  let messages: [Message]

  init(protocolVersion: ProtocolVersion, messages: [Message]) {
    self.protocolVersion = protocolVersion
    self.messages = messages
  }

  init(
    deserializationContext: inout DeserializationContext,
    application: some Application
  ) throws {
    protocolVersion = try deserializationContext.deserialize()
    var messages = [Message]()
    repeat {
      try messages.append(Message(
        deserializationContext: &deserializationContext,
        application: application
      ))
    } while deserializationContext.position < deserializationContext.count
    self.messages = messages
  }

  func serialize(into serializationContext: inout SerializationContext) throws {
    serializationContext.serialize(uint8: protocolVersion)
    for message in messages {
      try message.serialize(into: &serializationContext)
    }
    serializationContext.serialize(uint16: EndMark)
  }
}
