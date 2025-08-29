//
// Copyright (c) 2024-2025 PADL Software Pty Ltd
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

import IEEE802
import Logging
import Synchronization

enum ParticipantType {
  case full
  case pointToPoint
  case newOnly
  case applicantOnly
}

private enum EnqueuedEvent<A: Application>: Equatable, CustomStringConvertible {
  struct AttributeEvent: Equatable, CustomStringConvertible {
    let attributeEvent: MRP.AttributeEvent
    let attributeValue: _AttributeValue<A>
    let encodingOptional: Bool

    var description: String {
      "attributeEvent: \(attributeEvent), attributeValue: \(attributeValue), encodingOptional: \(encodingOptional)"
    }
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

  var attributeEvent: AttributeEvent? {
    switch self {
    case let .attributeEvent(attributeEvent):
      attributeEvent
    case .leaveAllEvent:
      nil
    }
  }

  var unsafeAttributeEvent: AttributeEvent {
    attributeEvent!
  }

  var description: String {
    if isLeaveAll {
      "EnqueuedEvent(LA, attributeType: \(attributeType))"
    } else {
      "EnqueuedEvent(\(unsafeAttributeEvent))"
    }
  }

  func canBeReplacedBy(_ newEvent: EnqueuedEvent<A>) -> Bool {
    if self == newEvent {
      // if the event is identical, replace it (effectively, a no-op)
      true
    } else if let existingAttributeEvent = attributeEvent,
              let newAttributeEvent = newEvent.attributeEvent
    {
      // clause 10.6 suggests the message actually transmitted is "that
      // appropriate to the state of the machine when the opportunity is
      // presented". I believe this means we can replace any event with the
      // same attribute value. We could probably simplify this by just
      // transmitting all current attribute values at TX opportunities.
      existingAttributeEvent.attributeValue == newAttributeEvent.attributeValue
    } else {
      false
    }
  }
}

public final actor Participant<A: Application>: Equatable, Hashable, CustomStringConvertible {
  public static func == (lhs: Participant<A>, rhs: Participant<A>) -> Bool {
    lhs.application == rhs.application && lhs.port == rhs.port && lhs.contextIdentifier == rhs
      .contextIdentifier
  }

  public nonisolated func hash(into hasher: inout Hasher) {
    application?.hash(into: &hasher)
    port.hash(into: &hasher)
    contextIdentifier.hash(into: &hasher)
  }

  private typealias EnqueuedEvents = [AttributeType: [EnqueuedEvent<A>]]

  private var _attributes = [AttributeType: Set<_AttributeValue<A>>]()
  private var _enqueuedEvents = EnqueuedEvents()
  private var _leaveAll: LeaveAll!
  private var _jointimer: Timer?
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
    await _initTimers()
  }

  public nonisolated var description: String {
    "\(application!.name)@\(port.name)"
  }

  private func _initTimers() async {
    // The Join Period Timer, jointimer, controls the interval between transmit
    // opportunities that are applied to the Applicant state machine. An
    // instance of this timer is required on a per-Port, per-MRP Participant
    // basis. The value of JoinTime used to initialize this timer is determined
    // in accordance with 10.7.11.

    // only required for shared media, in the point-to-point case packets
    // are transmitted immediately
    let jointimer = Timer(label: "jointimer", onExpiry: _onJoinTimerExpired)
    jointimer.start(interval: JoinTime)
    _jointimer = jointimer

    // The Leave All Period Timer, leavealltimer, controls the frequency with
    // which the LeaveAll state machine generates LeaveAll PDUs. The timer is
    // required on a per-Port, per-MRP Participant basis. If LeaveAllTime is
    // zero, the Leave All Period Timer is not started; otherwise, the Leave
    // All Period Timer is set to a random value, T, in the range LeaveAllTime
    // < T < 1.5 × LeaveAllTime when it is started. LeaveAllTime is defined in
    // Table 10-7.
    _leaveAll = await LeaveAll(
      interval: controller!.leaveAllTime,
      onLeaveAllTimerExpired: _onLeaveAllTimerExpired
    )
  }

  deinit {
    _jointimer?.stop()
    _leaveAll?.stopLeaveAllTimer()
  }

  @Sendable
  private func _onJoinTimerExpired() async throws {
    try await _txOpportunity(eventSource: .joinTimer)
  }

  @Sendable
  private func _onLeaveAllTimerExpired() async throws {
    try await _handleLeaveAll(event: .leavealltimer, eventSource: .leaveAllTimer)
  }

  private func _apply(
    attributeType: AttributeType? = nil,
    matching filter: AttributeValueFilter? = nil,
    _ block: AsyncParticipantApplyFunction<A>
  ) async rethrows {
    for attribute in _attributes {
      for attributeValue in attribute.value {
        if let filter,
           !attributeValue.matches(attributeType: attributeType, matching: filter) { continue }
        try await block(attributeValue)
      }
    }
  }

  private func _apply(
    event: ProtocolEvent,
    eventSource: EventSource
  ) async throws {
    try await _apply { attributeValue in
      try await attributeValue.handle(
        event: event,
        eventSource: eventSource
      )
    }
  }

  private func _applyDeferredRegistrarChanges(
    event: ProtocolEvent,
    eventSource: EventSource
  ) async throws {
    try await _apply { attributeValue in
      try await attributeValue.handleDeferredRegistrarChanges(
        event: event,
        eventSource: eventSource
      )
    }
  }

  private func _txOpportunity(eventSource: EventSource) async throws {
    // this will send a .tx/.txLA event to all attributes which will then make
    // the appropriate state transitions, potentially triggering the encoding
    // of a vector
    switch _leaveAll.state {
    case .Active:
      // encode attributes first with current registrar states, then process LeaveAll
      try await _apply(event: .txLA, eventSource: eventSource)
      // now process deferred registrar state changes for txLA
      try await _applyDeferredRegistrarChanges(event: .txLA, eventSource: eventSource)
      // sets LeaveAll to passive and emits sLA action
      try await _handleLeaveAll(event: .tx, eventSource: eventSource)
    case .Passive:
      try await _apply(event: .tx, eventSource: eventSource)
    }
  }

  private func _findOrCreateAttribute(
    attributeType: AttributeType,
    attributeSubtype: AttributeSubtype?,
    matching filter: AttributeValueFilter,
    createIfMissing: Bool
  ) throws -> _AttributeValue<A> {
    if let attributeValue = _attributes[attributeType]?
      .first(where: { $0.matches(attributeType: attributeType, matching: filter) })
    {
      return attributeValue
    }

    guard createIfMissing else {
      throw MRPError.invalidAttributeValue
    }

    guard let filterValue = try filter._value else {
      throw MRPError.internalError
    }

    let attributeValue = _AttributeValue(
      participant: self,
      type: attributeType,
      subtype: attributeSubtype,
      value: filterValue
    )
    if let index = _attributes.index(forKey: attributeType) {
      _attributes.values[index].insert(attributeValue)
    } else {
      _attributes[attributeType] = [attributeValue]
    }
    return attributeValue
  }

  public func findAttribute(
    attributeType: AttributeType,
    matching filter: AttributeValueFilter
  ) -> (AttributeSubtype?, any Value)? {
    let attributeValue = try? _findOrCreateAttribute(
      attributeType: attributeType,
      attributeSubtype: nil,
      matching: filter,
      createIfMissing: false
    )
    // we allow attributes that are in the leaving state to be "found", because
    // they haven't yet been timed out yet (and a leave indication issued)
    guard let attributeValue, attributeValue.registrarState != .MT else {
      _logger.trace("\(self): could not find attribute type \(attributeType) matching \(filter)")
      return nil
    }
    return (attributeValue.attributeSubtype, attributeValue.unwrappedValue)
  }

  public func findAttributes(
    attributeType: AttributeType,
    matching filter: AttributeValueFilter
  ) -> [(AttributeSubtype?, any Value)] {
    var attributeValues = [(AttributeSubtype?, any Value)]()

    for attributeValue in _attributes[attributeType] ?? [] {
      guard attributeValue.matches(attributeType: attributeType, matching: filter) else {
        continue
      }
      guard attributeValue.registrarState != .MT else {
        continue
      }
      attributeValues.append((
        attributeValue.attributeSubtype,
        attributeValue.unwrappedValue
      ))
    }

    return attributeValues
  }

  public func leaveNow(
    _ isIncluded: @Sendable (AttributeType, AttributeSubtype?, any Value)
      -> Bool
  ) async throws {
    try await _leave(eventSource: .application, isLeaveAll: false, isIncluded)
  }

  private func _leave(
    eventSource: EventSource,
    isLeaveAll: Bool,
    _ isIncluded: @Sendable (AttributeType, AttributeSubtype?, any Value) -> Bool
  ) async throws {
    try await _apply { attributeValue in
      guard isIncluded(
        attributeValue.attributeType,
        attributeValue.attributeSubtype,
        attributeValue.unwrappedValue
      ) else {
        return
      }

      try await attributeValue.handle(event: isLeaveAll ? .rLA : .rLv, eventSource: eventSource)
    }
  }

  private func _leaveAll(
    eventSource: EventSource,
    attributeType leaveAllAttributeType: AttributeType
  ) async throws {
    try await _leave(eventSource: eventSource, isLeaveAll: true) { attributeType, _, _ in
      attributeType == leaveAllAttributeType
    }
  }

  private func _chunkAttributeEvents(_ attributeEvents: [EnqueuedEvent<A>.AttributeEvent])
    -> [[EnqueuedEvent<A>.AttributeEvent]]
  {
    guard !attributeEvents.isEmpty else { return [] }

    var chunks: [[EnqueuedEvent<A>.AttributeEvent]] = []
    var currentChunk: [EnqueuedEvent<A>.AttributeEvent] = []
    var expectedIndex = attributeEvents[0].attributeValue.index

    for attributeEvent in attributeEvents {
      if attributeEvent.attributeValue.index == expectedIndex {
        currentChunk.append(attributeEvent)
        expectedIndex += 1
      } else {
        if !currentChunk.isEmpty {
          chunks.append(currentChunk)
        }
        currentChunk = [attributeEvent]
        expectedIndex = attributeEvent.attributeValue.index + 1
      }
    }

    if !currentChunk.isEmpty {
      chunks.append(currentChunk)
    }

    return chunks
  }

  private func _packMessages(with events: EnqueuedEvents) throws -> [Message] {
    guard let application else { throw MRPError.internalError }

    var messages = [Message]()

    for (attributeType, eventValue) in events {
      let leaveAll = eventValue.contains(where: \.isLeaveAll)
      let attributeEvents = eventValue.filter { !$0.isLeaveAll }.map(\.unsafeAttributeEvent)
        .sorted(by: {
          $0.attributeValue.index < $1.attributeValue.index
        })
      let attributeEventChunks = _chunkAttributeEvents(attributeEvents)

      var vectorAttributes: [VectorAttribute<AnyValue>] = attributeEventChunks
        .compactMap { attributeEventChunk in
          guard !attributeEventChunk.isEmpty else { return nil }

          let attributeSubtypes: [AttributeSubtype]? = if application
            .hasAttributeSubtype(for: attributeType)
          {
            Array(attributeEventChunk.map { $0.attributeValue.attributeSubtype! })
          } else {
            nil
          }

          return VectorAttribute<AnyValue>(
            leaveAllEvent: leaveAll ? .LeaveAll : .NullLeaveAllEvent,
            firstValue: attributeEventChunk.first!.attributeValue.value,
            attributeEvents: Array(attributeEventChunk.map(\.attributeEvent)),
            applicationEvents: attributeSubtypes
          )
        }

      if vectorAttributes.isEmpty, leaveAll {
        let vectorAttribute = try VectorAttribute<AnyValue>(
          leaveAllEvent: .LeaveAll,
          firstValue: AnyValue(application.makeNullValue(for: attributeType)),
          attributeEvents: [],
          applicationEvents: nil
        )
        vectorAttributes = [vectorAttribute]
      }
      if !vectorAttributes.isEmpty {
        messages.append(Message(attributeType: attributeType, attributeList: vectorAttributes))
      }
    }

    return messages
  }

  private func _txEnqueue(_ event: EnqueuedEvent<A>) {
    if let index = _enqueuedEvents.index(forKey: event.attributeType) {
      if let eventIndex = _enqueuedEvents.values[index]
        .firstIndex(where: { $0.canBeReplacedBy(event) })
      {
        _enqueuedEvents.values[index][eventIndex] = event
      } else {
        _enqueuedEvents.values[index].append(event)
      }
    } else {
      _enqueuedEvents[event.attributeType] = [event]
    }
  }

  fileprivate func _txEnqueue(
    attributeEvent: AttributeEvent,
    attributeValue: _AttributeValue<A>,
    encodingOptional: Bool
  ) {
    let event = EnqueuedEvent<A>.AttributeEvent(
      attributeEvent: attributeEvent,
      attributeValue: attributeValue,
      encodingOptional: encodingOptional
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
    eventSource: EventSource
  ) async throws {
    switch _leaveAll.action(for: event) {
    case .leavealltimer:
      _leaveAll.startLeaveAllTimer()
    case .sLA:
      // a) The LeaveAll state machine associated with that instance of the
      // Applicant or Registrar state machine performs the sLA action
      // (10.7.6.6); or a MRPDU is received with a LeaveAll
      _logger.debug("\(self): sending leave all events, source \(eventSource)")
      try await _apply(event: .rLA, eventSource: eventSource)
      try _txEnqueueLeaveAllEvents()
    default:
      break
    }
  }

  private func _txDequeue() async throws -> MRPDU? {
    guard let application else { throw MRPError.internalError }
    let enqueuedMessages = try _packMessages(with: _enqueuedEvents)
    _enqueuedEvents.removeAll()

    guard !enqueuedMessages.isEmpty else { return nil }

    let pdu = MRPDU(
      protocolVersion: application.protocolVersion,
      messages: enqueuedMessages
    )

    return pdu
  }

  private func _handle(
    attributeEvent: AttributeEvent,
    with attributeValue: _AttributeValue<A>,
    eventSource: EventSource
  ) async throws {
    try await attributeValue.handle(
      event: attributeEvent.protocolEvent,
      eventSource: eventSource
    )
  }

  func rx(message: Message, sourceMacAddress: EUI48) async throws {
    _debugLogMessage(message, direction: .rx)
    let eventSource: EventSource = _isEqualMacAddress(
      sourceMacAddress,
      port.macAddress
    ) ? .local : .peer
    for vectorAttribute in message.attributeList {
      // 10.6 Protocol operation: process LeaveAll first.
      if vectorAttribute.leaveAllEvent == .LeaveAll {
        try await _leaveAll(eventSource: eventSource, attributeType: message.attributeType)
      }

      let packedEvents = try vectorAttribute.attributeEvents
      guard packedEvents.count >= vectorAttribute.numberOfValues else {
        throw MRPError.badVectorAttribute
      }
      for i in 0..<Int(vectorAttribute.numberOfValues) {
        let attributeEvent = packedEvents[i]
        let attributeSubtype = vectorAttribute.applicationEvents?[i]

        guard let attribute = try? _findOrCreateAttribute(
          attributeType: message.attributeType,
          attributeSubtype: attributeSubtype,
          matching: .matchRelative((vectorAttribute.firstValue.value, UInt64(i))),
          createIfMissing: true
        ) else { continue }

        if let attributeSubtype, attributeEvent == .JoinIn || attributeEvent == .JoinMt {
          // fast path for MSRP pre-applicant event handler: silently replace attribute
          // subtypes as if the Listener declaration had been withdrawn and
          // replaced by the updated Listener declaration (35.2.6)
          attribute.attributeSubtype = attributeSubtype
        }

        try await _handle(
          attributeEvent: attributeEvent,
          with: attribute,
          eventSource: eventSource
        )
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
    // TODO: add flags for when attribute is empty, so Applicant State Machine can
    // ignore transition to LO from VO/AO/QO when receiving rLA!, txLA!, or txLAF!
    return flags
  }

  private enum _Direction: CustomStringConvertible {
    case tx
    case rx

    var description: String {
      switch self {
      case .tx: "TX"
      case .rx: "RX"
      }
    }
  }

  private func _debugLogAttribute(
    attributeType: AttributeType,
    _ attribute: VectorAttribute<AnyValue>,
    direction: _Direction
  ) {
    let threePackedEventsString = try! attribute.attributeEvents
      .compactMap { String(describing: $0) }.joined(separator: ", ")
    let fourPackedEventsString = attribute.applicationEvents?
      .prefix(Int(attribute.numberOfValues))
      .compactMap { String(describing: MSRPAttributeSubtype(rawValue: $0)!) }
      .joined(separator: ", ")
    let firstValueString = attribute
      .numberOfValues > 0 ? String(describing: attribute.firstValue) : "--"

    if let fourPackedEventsString {
      _logger
        .debug(
          "\(self): \(direction): AT \(attributeType) \(attribute.leaveAllEvent == .LeaveAll ? "LA" : "--") AV \(firstValueString) AE [\(threePackedEventsString)] AS [\(fourPackedEventsString)]"
        )
    } else {
      _logger
        .debug(
          "\(self): \(direction): AT \(attributeType) \(attribute.leaveAllEvent == .LeaveAll ? "LA" : "--") AV \(firstValueString) AE [\(threePackedEventsString)]"
        )
    }
  }

  private func _debugLogMessage(_ message: Message, direction: _Direction) {
    for attribute in message.attributeList {
      _debugLogAttribute(attributeType: message.attributeType, attribute, direction: direction)
    }
  }

  private func _debugLogPdu(_ pdu: MRPDU, direction: _Direction) {
    _logger
      .debug("\(self): \(direction): -------------------------------------------------------------")
    for message in pdu.messages {
      _debugLogMessage(message, direction: direction)
    }
    _logger
      .debug("\(self): \(direction): -------------------------------------------------------------")
  }

  func tx() async throws {
    guard let application, let controller else { throw MRPError.internalError }
    guard let pdu = try await _txDequeue() else { return }
    _debugLogPdu(pdu, direction: .tx)
    try await controller.bridge.tx(
      pdu: pdu,
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
    try await _apply(event: .Flush, eventSource: .internal)
    try await _handleLeaveAll(event: .Flush, eventSource: .internal)
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
    try await _apply(event: .ReDeclare, eventSource: .internal)
  }

  func join(
    attributeType: AttributeType,
    attributeSubtype: AttributeSubtype? = nil,
    attributeValue: some Value,
    isNew: Bool,
    eventSource: EventSource
  ) async throws {
    let attribute = try _findOrCreateAttribute(
      attributeType: attributeType,
      attributeSubtype: attributeSubtype,
      matching: .matchEqual(attributeValue), // don't match on subtype, we want to replace it
      createIfMissing: true
    )

    if let attributeSubtype { attribute.attributeSubtype = attributeSubtype }
    try await attribute.handle(event: isNew ? .New : .Join, eventSource: eventSource)
  }

  func leave(
    attributeType: AttributeType,
    attributeSubtype: AttributeSubtype? = nil,
    attributeValue: some Value,
    eventSource: EventSource
  ) async throws {
    let attribute = try _findOrCreateAttribute(
      attributeType: attributeType,
      attributeSubtype: attributeSubtype,
      matching: .matchEqual(attributeValue), // don't match on subtype, we want to replace it
      createIfMissing: false
    )

    if let attributeSubtype { attribute.attributeSubtype = attributeSubtype }
    try await attribute.handle(event: .Lv, eventSource: eventSource)
  }
}

private typealias AsyncParticipantApplyFunction<A: Application> =
  @Sendable (_AttributeValue<A>) async throws -> ()

private typealias ParticipantApplyFunction<A: Application> =
  @Sendable (_AttributeValue<A>) throws -> ()

private final class _AttributeValue<A: Application>: @unchecked
Sendable, Hashable, Equatable,
  CustomStringConvertible
{
  typealias P = Participant<A>

  // don't match subtype on comparison, we don't want to emit PDUs with
  // multiple subtype values (at least, for MSRP). We only match the index
  // because we can only have one value for each (attributeType, index).
  static func == (lhs: _AttributeValue<A>, rhs: _AttributeValue<A>) -> Bool {
    lhs.matches(
      attributeType: rhs.attributeType,
      matching: .matchIndex(rhs.unwrappedValue)
    )
  }

  private let _participant: Weak<P>
  private let _attributeSubtype: Mutex<AttributeSubtype?>

  private let applicant = Applicant() // A per-Attribute Applicant state machine (10.7.7)
  // note registrar is not mutated outside init() so it does not need a mutex
  private var registrar: Registrar? // A per-Attribute Registrar state machine (10.7.8)

  let attributeType: AttributeType
  let value: AnyValue

  let counters = Mutex(EventCounters<A>())

  var index: UInt64 { value.index }
  var participant: P? { _participant.object }
  var unwrappedValue: any Value { value.value }

  var attributeSubtype: AttributeSubtype? {
    get {
      _attributeSubtype.withLock { $0 }
    }

    set {
      _attributeSubtype.withLock { $0 = newValue }
    }
  }

  var registrarState: Registrar.State? {
    registrar?.state
  }

  nonisolated var description: String {
    if let attributeSubtype {
      "_AttributeValue(attributeType: \(attributeType), attributeSubtype: \(attributeSubtype), attributeValue: \(value), A \(applicant) R \(registrar?.description ?? "-"))"
    } else {
      "_AttributeValue(attributeType: \(attributeType), attributeValue: \(value), A \(applicant) R \(registrar?.description ?? "-"))"
    }
  }

  private init(
    participant: Weak<P>,
    type: AttributeType,
    subtype: AttributeSubtype?,
    value: AnyValue
  ) {
    precondition(!(value.value is AnyValue))
    _participant = participant
    registrar = nil
    attributeType = type
    _attributeSubtype = .init(subtype)
    self.value = value
  }

  init(participant: P, type: AttributeType, subtype: AttributeSubtype?, value: some Value) {
    precondition(!(value is AnyValue))
    _participant = Weak(participant)
    attributeType = type
    _attributeSubtype = .init(subtype)
    self.value = AnyValue(value)
    if participant._type != .applicantOnly {
      registrar = Registrar(onLeaveTimerExpired: _onLeaveTimerExpired)
    }
  }

  deinit {
    registrar?.stopLeaveTimer()
  }

  @Sendable
  private func _onLeaveTimerExpired() async throws {
    try await handle(
      event: .leavetimer,
      eventSource: .leaveTimer
    )
  }

  func hash(into hasher: inout Hasher) {
    attributeType.hash(into: &hasher)
    value.index.hash(into: &hasher)
  }

  func matches(
    attributeType filterAttributeType: AttributeType?,
    matching filter: AttributeValueFilter
  ) -> Bool {
    if let filterAttributeType {
      guard attributeType == filterAttributeType else { return false }
    }

    do {
      switch filter {
      case .matchAny:
        return true
      case let .matchAnyIndex(index):
        return self.index == index
      case let .matchIndex(value):
        return index == value.index
      case let .matchEqual(value):
        return self.value == value.eraseToAny()
      case .matchRelative(let (value, offset)):
        return try self.value == value.makeValue(relativeTo: offset).eraseToAny()
      }
    } catch {
      return false
    }
  }

  func handle(
    event: ProtocolEvent,
    eventSource: EventSource
  ) async throws {
    guard let participant else { throw MRPError.internalError }

    let context = try await EventContext(
      participant: participant,
      event: event,
      eventSource: eventSource,
      attributeType: attributeType,
      attributeSubtype: attributeSubtype,
      attributeValue: unwrappedValue,
      smFlags: participant._getSmFlags(for: attributeType),
      applicant: applicant,
      registrar: registrar
    )

    precondition(!(unwrappedValue is AnyValue))
    participant._logger.trace("\(participant): handling \(context)")
    try await _handleApplicant(context: context) // attribute subtype can be adjusted by hook
    // for txLA events, defer registrar state changes until after applicant encoding
    if event != .txLA {
      try await _handleRegistrar(context: context)
    }
  }

  func handleDeferredRegistrarChanges(
    event: ProtocolEvent,
    eventSource: EventSource
  ) async throws {
    guard let participant else { throw MRPError.internalError }

    let context = try await EventContext(
      participant: participant,
      event: event,
      eventSource: eventSource,
      attributeType: attributeType,
      attributeSubtype: attributeSubtype,
      attributeValue: unwrappedValue,
      smFlags: participant._getSmFlags(for: attributeType),
      applicant: applicant,
      registrar: registrar
    )

    try await _handleRegistrar(context: context)
  }

  private func _handleApplicant(context: EventContext<A>) async throws {
    let applicantAction = applicant.action(for: context.event, flags: context.smFlags)

    if let applicantAction {
      context.participant._logger
        .trace(
          "\(context.participant): applicant action for event \(context.event): \(applicantAction)"
        )
      let applicationEventHandler = context.participant
        .application as? any ApplicationEventHandler<A>
      try await applicationEventHandler?.preApplicantEventHandler(context: context)
      let attributeEvent = try await _handle(applicantAction: applicantAction, context: context)
      applicationEventHandler?.postApplicantEventHandler(context: context)
      counters.withLock { $0.count(context: context, attributeEvent: attributeEvent) }
    }
  }

  private func _handle(
    applicantAction action: Applicant.Action,
    context: EventContext<A>
  ) async throws -> AttributeEvent? {
    var attributeEvent: AttributeEvent?

    switch action {
    case .sN:
      // The AttributeEvent value New is encoded in the Vector as specified in
      // 10.7.6.1.
      attributeEvent = .New
    case .sJ:
      fallthrough
    case .sJ_:
      // The [sJ] variant indicates that the action is only necessary in cases
      // where transmitting the value, rather than terminating a vector and
      // starting a new one, makes for more optimal encoding; i.e.,
      // transmitting the value is not necessary for correct protocol
      // operation.
      // TODO: the text is difficult to parse, are we implementing correctly?
      guard let registrar else { break }
      attributeEvent = (registrar.state == .IN) ? .JoinIn : .JoinMt
    case .sL:
      fallthrough
    case .sL_:
      attributeEvent = .Lv
    case .s:
      fallthrough
    case .s_:
      guard let registrar else { break }
      attributeEvent = (registrar.state == .IN) ? .In : .Mt
    }

    if let attributeEvent {
      await context.participant._txEnqueue(
        attributeEvent: attributeEvent,
        attributeValue: self,
        encodingOptional: action.encodingOptional
      )
    }

    return attributeEvent
  }

  private func _handleRegistrar(context: EventContext<A>) async throws {
    if let registrarAction = context.registrar?.action(for: context.event, flags: context.smFlags) {
      context.participant._logger
        .trace(
          "\(context.participant): registrar action for event \(context.event): \(registrarAction)"
        )
      try await _handle(
        registrarAction: registrarAction,
        context: context
      )
    }
  }

  private func _handle(
    registrarAction action: Registrar.Action,
    context: EventContext<A>
  ) async throws {
    guard let application = context.participant.application else { throw MRPError.internalError }
    switch action {
    case .New:
      fallthrough
    case .Join:
      try await application.joinIndicated(
        contextIdentifier: context.participant.contextIdentifier,
        port: context.participant.port,
        attributeType: context.attributeType,
        attributeSubtype: context.attributeSubtype,
        attributeValue: context.attributeValue,
        isNew: action == .New,
        eventSource: context.eventSource
      )
    case .Lv:
      try await application.leaveIndicated(
        contextIdentifier: context.participant.contextIdentifier,
        port: context.participant.port,
        attributeType: context.attributeType,
        attributeSubtype: context.attributeSubtype,
        attributeValue: context.attributeValue,
        eventSource: context.eventSource
      )
    }
  }
}

private extension AttributeValueFilter {
  var _value: (any Value)? {
    get throws {
      switch self {
      case .matchAny:
        fallthrough
      case .matchAnyIndex:
        return nil
      case let .matchIndex(value):
        fallthrough
      case let .matchEqual(value):
        return value
      case .matchRelative(let (value, index)):
        return try value.makeValue(relativeTo: index)
      }
    }
  }
}
