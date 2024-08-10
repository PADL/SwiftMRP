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

// one Participant per MRP application per Port
//
// a) Participants can issue declarations for MRP application attributes (10.2,
//    10.3, 10.7.3, and 10.7.7).
// b) Participants can withdraw declarations for attributes (10.2, 10.3,
//    10.7.3, and 10.7.7).
// c) Each Bridge propagates declarations to MRP Participants (10.3).
// d) MRP Participants can track the current state of declaration and
//    registration of attributes on each Port of the participant device (10.7.7 and
//    10.7.8).
// e) MRP Participants can remove state information relating to attributes that
//    are no longer active within part or all of the network, e.g., as a result of
//   the failure of a participant (10.7.8 and 10.7.9).
// ...
//
// For a given Port of an MRP-aware Bridge and MRP application supported by that
// Bridge, an instance of an MRP Participant can exist for each MRP Attribute
// Propagation Context (MAP Context) understood by the Bridge. A MAP Context
// identifies the set of Bridge Ports that form the applicable active topology
// (8.4).

import Logging

enum ParticipantType {
  case full
  case pointToPoint
  case newOnly
  case applicantOnly
}

public enum ParticipantEventSource: Sendable {
  case timer
  case local
  case peer
  case administrativeControl
  case map
}

public final actor Participant<A: Application>: Equatable, Hashable {
  public static func == (lhs: Participant<A>, rhs: Participant<A>) -> Bool {
    lhs.application == rhs.application && lhs.port == rhs.port && lhs.contextIdentifier == rhs
      .contextIdentifier
  }

  public nonisolated func hash(into hasher: inout Hasher) {
    application?.hash(into: &hasher)
    port.hash(into: &hasher)
    contextIdentifier.hash(into: &hasher)
  }

  private enum EnqueuedEvent {
    struct AttributeEvent {
      let attributeEvent: MRP.AttributeEvent
      let attributeValue: _AttributeValueState<A>
    }

    case attributeEvent(AttributeEvent)
    case leaveAllEvent(AttributeType)

    var attributeType: AttributeType {
      switch self {
      case let .attributeEvent(attributeEvent):
        attributeEvent.attributeValue.attributeType
      case let .leaveAllEvent(attributeType):
        attributeType
      }
    }

    var isLeaveAll: Bool {
      switch self {
      case .attributeEvent:
        false
      case .leaveAllEvent:
        true
      }
    }

    var unsafeAttributeEvent: AttributeEvent {
      switch self {
      case let .attributeEvent(attributeEvent):
        attributeEvent
      case .leaveAllEvent:
        fatalError("attemped to unsafely unwrap LeaveAll event")
      }
    }
  }

  private typealias EnqueuedEvents = [AttributeType: [EnqueuedEvent]]

  private var _attributes = [AttributeType: Set<_AttributeValueState<A>>]()
  private var _enqueuedEvents = EnqueuedEvents()
  private var _leaveAll: LeaveAll!
  private var _jointimer: Timer!
  private nonisolated let _controller: Weak<MRPController<A.P>>
  private nonisolated let _application: Weak<A>

  fileprivate let _logger: Logger
  fileprivate let _type: ParticipantType
  fileprivate nonisolated var controller: MRPController<A.P>? { _controller.object }
  fileprivate nonisolated var application: A? { _application.object }

  nonisolated let port: A.P
  nonisolated let contextIdentifier: MAPContextIdentifier

  init(
    controller: MRPController<A.P>,
    application: A,
    port: A.P,
    contextIdentifier: MAPContextIdentifier,
    type: ParticipantType? = nil
  ) async {
    _controller = Weak(controller)
    _application = Weak(application)
    self.contextIdentifier = contextIdentifier

    self.port = port

    if let type {
      _type = type
    } else if port.isPointToPoint {
      _type = .pointToPoint
    } else {
      _type = .full
    }

    _logger = controller.logger

    // The Join Period Timer, jointimer, controls the interval between transmit
    // opportunities that are applied to the Applicant state machine. An
    // instance of this timer is required on a per-Port, per-MRP Participant
    // basis. The value of JoinTime used to initialize this timer is determined
    // in accordance with 10.7.11.
    _jointimer = Timer(onExpiry: _onJoinTimerExpired)
    _jointimer.start(interval: JoinTime)

    // The Leave All Period Timer, leavealltimer, controls the frequency with
    // which the LeaveAll state machine generates LeaveAll PDUs. The timer is
    // required on a per-Port, per-MRP Participant basis. If LeaveAllTime is
    // zero, the Leave All Period Timer is not started; otherwise, the Leave
    // All Period Timer is set to a random value, T, in the range LeaveAllTime
    // < T < 1.5 × LeaveAllTime when it is started. LeaveAllTime is defined in
    // Table 10-7.
    _leaveAll = await LeaveAll(
      interval: controller.leaveAllTime,
      onLeaveAllTimerExpired: _onLeaveAllTimerExpired
    )
  }

  deinit {
    _jointimer.stop()
    _leaveAll.stopLeaveAllTimer()
  }

  @Sendable
  private func _onJoinTimerExpired() async throws {
    // this will send a .tx/.txLA event to all attributes which will then make
    // the appropriate state transitions, potentially triggering the encoding
    // of a vector
    switch _leaveAll.state {
    case .Active:
      try await _handleLeaveAll(
        event: .tx,
        eventSource: .timer
      ) // sets LeaveAll to passive and emits sLA action
      try await _apply(event: .txLA, eventSource: .timer)
    case .Passive:
      try await _apply(event: .tx, eventSource: .timer)
    }
  }

  @Sendable
  private func _onLeaveAllTimerExpired() async throws {
    try await _handleLeaveAll(event: .leavealltimer, eventSource: .timer)
  }

  private func _packMessages(with events: EnqueuedEvents) throws -> [Message] {
    guard let application else { throw MRPError.internalError }

    var messages = [Message]()

    for event in events {
      let leaveAll = event.value.contains(where: \.isLeaveAll)
      let attributeEvents = event.value.filter { !$0.isLeaveAll }.map(\.unsafeAttributeEvent)
        .sorted(by: {
          $0.attributeValue.index < $1.attributeValue.index
        })
      let valueIndexGroups = attributeEvents.map(\.attributeValue.index).consecutivelyGrouped

      var vectorAttributes: [VectorAttribute<AnyValue>] = try valueIndexGroups
        .map { valueIndexGroup in
          let firstIndex = attributeEvents
            .firstIndex(where: { $0.attributeValue.index == valueIndexGroup[0] })!
          let attributeEvents = attributeEvents[firstIndex..<(firstIndex + valueIndexGroups.count)]

          return try VectorAttribute<AnyValue>(
            leaveAllEvent: leaveAll ? .LeaveAll : .NullLeaveAllEvent,
            firstValue: attributeEvents[firstIndex].attributeValue.value,
            attributeEvents: Array(attributeEvents.map(\.attributeEvent)),
            attributeType: event.key,
            application: application
          )
        }

      if vectorAttributes.count == 0, leaveAll {
        let vectorAttribute = try VectorAttribute<AnyValue>(
          leaveAllEvent: .LeaveAll,
          numberOfValues: 0,
          firstValue: AnyValue(application.makeValue(for: event.key)),
          vector: [UInt8]()
        )
        vectorAttributes.append(vectorAttribute)
      }

      messages.append(Message(attributeType: event.key, attributeList: vectorAttributes))
    }

    _logger.trace("coalesced events \(events) into \(messages)")

    return messages
  }

  private func _txEnqueue(_ event: EnqueuedEvent) {
    _logger.trace("enqueing event \(event)")
    if let index = _enqueuedEvents.index(forKey: event.attributeType) {
      _enqueuedEvents.values[index].append(event)
    } else {
      _enqueuedEvents[event.attributeType] = [event]
    }
  }

  fileprivate func _txEnqueue(
    attributeEvent: AttributeEvent,
    attributeValue: _AttributeValueState<A>
  ) {
    let event = EnqueuedEvent.AttributeEvent(
      attributeEvent: attributeEvent,
      attributeValue: attributeValue
    )
    _txEnqueue(.attributeEvent(event))
  }

  private func _txEnqueueLeaveAllEvents() throws {
    guard let application else { throw MRPError.internalError }
    for attributeType in application.validAttributeTypes {
      _txEnqueue(.leaveAllEvent(attributeType))
    }
  }

  // handle an event in the LeaveAll state machine (10.5)
  private func _handleLeaveAll(
    event: ProtocolEvent,
    eventSource: ParticipantEventSource
  ) async throws {
    let action = _leaveAll.action(for: event)

    if action == .leavealltimer {
      _leaveAll.startLeaveAllTimer()
    } else if action == .sLA {
      // a) The LeaveAll state machine associated with that instance of the
      // Applicant or Registrar state machine performs the sLA action
      // (10.7.6.6); or a MRPDU is received with a LeaveAll
      try await _apply(event: .rLA, eventSource: eventSource)
      try _txEnqueueLeaveAllEvents()
    }
  }

  private func _apply(event: ProtocolEvent, eventSource: ParticipantEventSource) async throws {
    try await _apply { attributeValue in
      try await attributeValue.handle(event: event, eventSource: eventSource)
    }
  }

  private func _txDequeue() async throws -> MRPDU {
    guard let application else { throw MRPError.internalError }
    let enqueuedMessages = try _packMessages(with: _enqueuedEvents)
    let pdu = MRPDU(
      protocolVersion: application.protocolVersion,
      messages: enqueuedMessages
    )

    _enqueuedEvents.removeAll()

    return pdu
  }

  private func _apply(
    attributeType: AttributeType? = nil,
    _ block: ParticipantApplyFunction<A>
  ) async rethrows {
    if let attributeType {
      guard let attributeValues = _attributes[attributeType] else { return }
      for attributeValue in attributeValues {
        try await block(attributeValue)
      }
    } else {
      for attribute in _attributes {
        for attributeValue in attribute.value {
          try await block(attributeValue)
        }
      }
    }
  }

  private func _handle(
    attributeEvent: AttributeEvent,
    with attributeValue: _AttributeValueState<A>,
    eventSource: ParticipantEventSource
  ) async throws {
    let protocolEvent = attributeEvent.protocolEvent
    try await attributeValue.handle(event: protocolEvent, eventSource: eventSource)
  }

  // TODO: use a more efficient representation such as a bitmask
  private func _findAttributeValueState(
    attributeType: AttributeType,
    index: Int,
    isNew: Bool
  ) throws -> _AttributeValueState<A> {
    guard let application else { throw MRPError.internalError }
    let absoluteValue = try AnyValue(application.makeValue(for: attributeType, at: index))

    if let attributeValue = _attributes[attributeType]?.first(where: {
      $0.value == absoluteValue
    }) {
      return attributeValue
    }

    let attributeValue = _AttributeValueState(
      participant: self,
      type: attributeType,
      value: absoluteValue
    )
    if let index = _attributes.index(forKey: attributeType) {
      _attributes.values[index].insert(attributeValue)
    } else {
      _attributes[attributeType] = [attributeValue]
    }
    return attributeValue
  }

  private func _getAttributeEvents(
    attributeType: AttributeType,
    vectorAttribute: VectorAttribute<some Value>
  ) throws
    -> [AttributeEvent]
  {
    guard let application else { throw MRPError.internalError }
    switch try application.packedEventsType(for: attributeType) {
    case .threePackedType:
      return vectorAttribute.threePackedEvents.compactMap { AttributeEvent(rawValue: $0) }
    case .fourPackedType:
      return vectorAttribute.fourPackedEvents.compactMap { AttributeEvent(rawValue: $0) }
    }
  }

  func rx(message: Message, sourceMacAddress: EUI48) async throws {
    let eventSource: ParticipantEventSource = _isEqualMacAddress(
      sourceMacAddress,
      port.macAddress
    ) ?
      .local : .peer
    for vectorAttribute in message.attributeList {
      let packedEvents = try _getAttributeEvents(
        attributeType: message.attributeType,
        vectorAttribute: vectorAttribute
      )
      guard packedEvents.count >= vectorAttribute.numberOfValues else {
        throw MRPError.badVectorAttribute
      }
      for i in 0..<Int(vectorAttribute.numberOfValues) {
        // TODO: what is the correct policy for unknown attributes
        guard let attribute = try? _findAttributeValueState(
          attributeType: message.attributeType,
          index: vectorAttribute.firstValue.index + i,
          isNew: packedEvents[i] == .New
        ) else { continue }
        try await _handle(
          attributeEvent: packedEvents[i],
          with: attribute,
          eventSource: eventSource
        )
      }

      if vectorAttribute.leaveAllEvent == .LeaveAll {
        try await _handleLeaveAll(event: .rLA, eventSource: eventSource)
      }
    }
  }

  fileprivate func _getSmFlags(for attributeType: AttributeType) throws
    -> StateMachineHandlerFlags
  {
    guard let application else { throw MRPError.internalError }
    var flags: StateMachineHandlerFlags = []
    if _type == .pointToPoint { flags.insert(.operPointToPointMAC) }
    let administrativeControl = try application.administrativeControl(for: attributeType)
    switch administrativeControl {
    case .normalParticipant:
      break
    case .newOnlyParticipant:
      flags.insert(.registrationFixedNewPropagated)
    case .nonParticipant:
      flags.insert(.registrationFixedNewIgnored)
    }
    return flags
  }

  func tx() async throws {
    guard let application, let controller else { throw MRPError.internalError }
    try await controller.bridge.tx(
      pdu: _txDequeue(),
      for: application,
      contextIdentifier: contextIdentifier,
      on: port,
      controller: controller
    )
  }

  // A Flush! event signals to the Registrar state machine that there is a
  // need to rapidly deregister information on the Port associated with the
  // state machine as a result of a topology change that has occurred in the
  // network topology that supports the propagation of MRP information. If
  // the network topology is maintained by means of the spanning tree
  // protocol state machines, then, for the set of Registrar state machines
  // associated with a given Port and spanning tree instance, this event is
  // generated when the Port Role changes from either Root Port or Alternate
  // Port to Designated Port.
  func flush() async throws {
    try await _apply(event: .Flush, eventSource: .administrativeControl)
    try await _handleLeaveAll(event: .Flush, eventSource: .administrativeControl)
  }

  // A Re-declare! event signals to the Applicant and Registrar state machines
  // that there is a need to rapidly redeclare registered information on the
  // Port associated with the state machines as a result of a topology change
  // that has occurred in the network topology that supports the propagation
  // of MRP information. If the network topology is maintained by means of the
  // spanning tree protocol state machines, then, for the set of Applicant and
  // Registrar state machines associated with a given Port and spanning tree
  // instance, this event is generated when the Port Role changes from
  // Designated Port to either Root Port or Alternate Port.
  func redeclare() async throws {
    try await _apply(event: .ReDeclare, eventSource: .administrativeControl)
  }

  func join(
    attributeType: AttributeType,
    attributeValue: some Value,
    isNew: Bool,
    eventSource: ParticipantEventSource
  ) async throws {
    let attribute = try _findAttributeValueState(
      attributeType: attributeType,
      index: attributeValue.index,
      isNew: isNew
    )
    try await _handle(
      attributeEvent: isNew ? .New : .JoinMt,
      with: attribute,
      eventSource: .administrativeControl
    )
  }

  func leave(
    attributeType: AttributeType,
    attributeValue: some Value,
    eventSource: ParticipantEventSource
  ) async throws {
    let attribute = try _findAttributeValueState(
      attributeType: attributeType,
      index: attributeValue.index,
      isNew: false
    )
    try await _handle(attributeEvent: .Lv, with: attribute, eventSource: eventSource)
  }
}

private typealias ParticipantApplyFunction<A: Application> =
  @Sendable (_AttributeValueState<A>) async throws -> ()

private final class _AttributeValueState<A: Application>: @unchecked Sendable, Hashable,
  Equatable
{
  typealias P = Participant<A>
  static func == (lhs: _AttributeValueState<A>, rhs: _AttributeValueState<A>) -> Bool {
    lhs.attributeType == rhs.attributeType && lhs.value == rhs.value
  }

  private let _participant: Weak<P>
  private let applicant = Applicant() // A per-Attribute Applicant state machine (10.7.7)
  private var registrar: Registrar? // A per-Attribute Registrar state machine (10.7.8)
  let attributeType: AttributeType
  let value: AnyValue
  var index: Int { value.index }

  var participant: P? { _participant.object }

  init(participant: P, type: AttributeType, value: any Value) {
    _participant = Weak(participant)
    attributeType = type
    self.value = AnyValue(value)
    if participant._type != .applicantOnly {
      registrar = Registrar(onLeaveTimerExpired: {
        try await self.handle(event: .leavetimer, eventSource: .timer)
      })
    }
  }

  deinit {
    registrar?.stopLeaveTimer()
  }

  func hash(into hasher: inout Hasher) {
    attributeType.hash(into: &hasher)
    if let serialized = try? value.serialized() {
      serialized.hash(into: &hasher)
    }
  }

  func handle(event: ProtocolEvent, eventSource: ParticipantEventSource) async throws {
    guard let participant else { throw MRPError.internalError }
    let smFlags = try await participant._getSmFlags(for: attributeType)

    participant._logger
      .trace(
        "handling protocol event \(event) for attribute \(attributeType) value \(value) flags \(smFlags) state A \(applicant) R \(registrar?.description ?? "-")"
      )

    let applicantAction = applicant.action(for: event, flags: smFlags)
    if let applicantAction {
      participant._logger.trace("applicant action for event \(event): \(applicantAction)")
      try await handle(applicantAction: applicantAction, eventSource: eventSource)
    } else {
      participant._logger.trace("no applicant action for event \(event), skipping")
    }

    if let registrarAction = registrar?.action(for: event, flags: smFlags) {
      participant._logger.trace("registrar action for event \(event): \(registrarAction)")
      try await handle(registrarAction: registrarAction, eventSource: eventSource)
    } else {
      participant._logger.trace("no registrar action for event \(event), skipping")
    }
  }

  private func handle(
    applicantAction action: Applicant.Action,
    eventSource: ParticipantEventSource
  ) async throws {
    guard let participant else { throw MRPError.internalError }
    switch action {
    case .sN:
      // The AttributeEvent value New is encoded in the Vector as specified in
      // 10.7.6.1.
      await participant._txEnqueue(attributeEvent: .New, attributeValue: self)
    case .sJ:
      fallthrough
    case .sJ_:
      // The [sJ] variant indicates that the action is only necessary in cases
      // where transmitting the value, rather than terminating a vector and
      // starting a new one, makes for more optimal/ encoding; i.e.,
      // transmitting the value is not necessary for correct/ protocol
      // operation.
      guard let registrar else { break }
      if registrar.state == .IN {
        await participant._txEnqueue(attributeEvent: .JoinIn, attributeValue: self)
      } else if registrar.state == .MT || registrar.state == .LV {
        await participant._txEnqueue(attributeEvent: .JoinMt, attributeValue: self)
      }
    case .sL:
      fallthrough
    case .sL_:
      await participant._txEnqueue(attributeEvent: .Lv, attributeValue: self)
    case .s:
      fallthrough
    case .s_:
      guard let registrar else { break }
      if registrar.state == .IN {
        await participant._txEnqueue(attributeEvent: .In, attributeValue: self)
      } else if registrar.state == .MT || registrar.state == .LV {
        await participant._txEnqueue(attributeEvent: .Mt, attributeValue: self)
      }
    }
  }

  private func handle(
    registrarAction action: Registrar.Action,
    eventSource: ParticipantEventSource
  ) async throws {
    guard let participant else { throw MRPError.internalError }
    switch action {
    case .New:
      try await participant.application?.joinIndicated(
        contextIdentifier: participant.contextIdentifier,
        port: participant.port,
        attributeType: attributeType,
        attributeValue: value,
        isNew: true,
        eventSource: eventSource
      )
    case .Join:
      try await participant.application?.joinIndicated(
        contextIdentifier: participant.contextIdentifier,
        port: participant.port,
        attributeType: attributeType,
        attributeValue: value,
        isNew: false,
        eventSource: eventSource
      )
    case .Lv:
      try await participant.application?.leaveIndicated(
        contextIdentifier: participant.contextIdentifier,
        port: participant.port,
        attributeType: attributeType,
        attributeValue: value,
        eventSource: eventSource
      )
    }
  }
}
