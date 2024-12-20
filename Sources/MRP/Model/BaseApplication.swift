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
import Synchronization

protocol BaseApplication: Application where P == P {
  typealias MAPParticipantDictionary = [MAPContextIdentifier: Set<Participant<Self>>]

  var _controller: Weak<MRPController<P>> { get }
  var _participants: Mutex<MAPParticipantDictionary> { get }
}

protocol BaseApplicationContextObserver<P>: BaseApplication {
  func onContextAdded(
    contextIdentifier: MAPContextIdentifier,
    with context: MAPContext<P>
  ) async throws
  func onContextUpdated(contextIdentifier: MAPContextIdentifier, with context: MAPContext<P>) throws
  func onContextRemoved(contextIdentifier: MAPContextIdentifier, with context: MAPContext<P>) throws
}

protocol BaseApplicationEventObserver<P>: BaseApplication {
  func onJoinIndication(
    contextIdentifier: MAPContextIdentifier,
    port: P,
    attributeType: AttributeType,
    attributeSubtype: AttributeSubtype?,
    attributeValue: some Value,
    isNew: Bool,
    eventSource: EventSource
  ) async throws
  func onLeaveIndication(
    contextIdentifier: MAPContextIdentifier,
    port: P,
    attributeType: AttributeType,
    attributeSubtype: AttributeSubtype?,
    attributeValue: some Value,
    eventSource: EventSource
  ) async throws
}

extension BaseApplication {
  var controller: MRPController<P>? { _controller.object }

  public func add(participant: Participant<Self>) throws {
    precondition(
      nonBaseContextsSupported || participant
        .contextIdentifier == MAPBaseSpanningTreeContext
    )
    _participants.withLock {
      if let index = $0.index(forKey: participant.contextIdentifier) {
        $0.values[index].insert(participant)
      } else {
        $0[participant.contextIdentifier] = Set([participant])
      }
    }
  }

  public func remove(
    participant: Participant<Self>
  ) throws {
    precondition(
      nonBaseContextsSupported || participant
        .contextIdentifier == MAPBaseSpanningTreeContext
    )
    _ = _participants.withLock {
      $0[participant.contextIdentifier]?.remove(participant)
    }
  }

  @discardableResult
  public func apply<T>(
    for contextIdentifier: MAPContextIdentifier? = nil,
    _ block: AsyncApplyFunction<T>
  ) async rethrows -> [T] {
    var participants: Set<Participant<Self>>?
    _participants.withLock {
      if let contextIdentifier {
        participants = $0[contextIdentifier]
      } else {
        participants = Set($0.flatMap { Array($1) })
      }
    }
    var ret = [T]()
    if let participants {
      for participant in participants {
        try await ret.append(block(participant))
      }
    }
    return ret
  }

  @discardableResult
  public func apply<T>(
    for contextIdentifier: MAPContextIdentifier? = nil,
    _ block: ApplyFunction<T>
  ) rethrows -> [T] {
    var participants: Set<Participant<Self>>?
    _participants.withLock {
      if let contextIdentifier {
        participants = $0[contextIdentifier]
      } else {
        participants = Set($0.flatMap { Array($1) })
      }
    }
    var ret = [T]()
    if let participants {
      for participant in participants {
        try ret.append(block(participant))
      }
    }
    return ret
  }

  // applications that support MAP contexts other than the base context will have
  // participants allocated for each context
  private func _isParticipantValid(contextIdentifier: MAPContextIdentifier) -> Bool {
    nonBaseContextsSupported || contextIdentifier == MAPBaseSpanningTreeContext
  }

  public func didAdd(
    contextIdentifier: MAPContextIdentifier,
    with context: MAPContext<P>
  ) async throws {
    if _isParticipantValid(contextIdentifier: contextIdentifier) {
      for port in context {
        guard (try? findParticipant(for: contextIdentifier, port: port)) == nil
        else {
          throw MRPError.portAlreadyExists
        }
        guard let controller else { throw MRPError.internalError }
        let participant = await Participant<Self>(
          controller: controller,
          application: self,
          port: port,
          contextIdentifier: contextIdentifier
        )
        try add(participant: participant)
      }
    }
    // ensure participants are initialized before calling observer
    // also call this regardless of the value of nonBaseContextsSupported, so that
    // MVRP can be advised of VLAN changes on a port
    if let observer = self as? any BaseApplicationContextObserver<P> {
      try await observer.onContextAdded(contextIdentifier: contextIdentifier, with: context)
    }
  }

  public func didUpdate(
    contextIdentifier: MAPContextIdentifier,
    with context: MAPContext<P>
  ) throws {
    if _isParticipantValid(contextIdentifier: contextIdentifier) {
      for port in context {
        let participant = try findParticipant(
          for: contextIdentifier,
          port: port
        )
        Task { try await participant.redeclare() }
      }
    }
    // also call this regardless of the value of nonBaseContextsSupported, so that
    // MVRP can be advised of VLAN changes on a port
    if let observer = self as? any BaseApplicationContextObserver<P> {
      try observer.onContextUpdated(contextIdentifier: contextIdentifier, with: context)
    }
  }

  public func didRemove(
    contextIdentifier: MAPContextIdentifier,
    with context: MAPContext<P>
  ) throws {
    // call observer _before_ removing participants so it can do any other cleanup
    // also call this regardless of the value of nonBaseContextsSupported, so that
    // MVRP can be advised of VLAN changes on a port
    if let observer = self as? any BaseApplicationContextObserver<P> {
      try observer.onContextRemoved(contextIdentifier: contextIdentifier, with: context)
    }
    if _isParticipantValid(contextIdentifier: contextIdentifier) {
      for port in context {
        let participant = try findParticipant(
          for: contextIdentifier,
          port: port
        )
        Task { try await participant.flush() }
        try remove(participant: participant)
      }
    }
  }

  public func shouldPropagate(eventSource: EventSource) -> Bool {
    switch eventSource {
    case .joinTimer:
      fallthrough
    case .local:
      fallthrough
    case .peer:
      fallthrough
    case .application:
      return true // FIXME: check whether we should propagate application withdrawals?
    case .internal:
      fallthrough // don't need to propagate this because application calls all participants
    case .map:
      fallthrough
    case .leaveTimer:
      fallthrough
    case .leaveAllTimer:
      return false // don't recursively call ourselves, and let each participant handle leave timers
    }
  }

  private func _propagateJoinIndicated(
    contextIdentifier: MAPContextIdentifier,
    port: P,
    attributeType: AttributeType,
    attributeSubtype: AttributeSubtype?,
    attributeValue: some Value,
    isNew: Bool,
    eventSource: EventSource
  ) async throws {
    guard shouldPropagate(eventSource: eventSource) else { return }
    try await apply(for: contextIdentifier) { participant in
      guard participant.port != port else { return }
      try await participant.join(
        attributeType: attributeType,
        attributeSubtype: attributeSubtype,
        attributeValue: attributeValue,
        isNew: isNew,
        eventSource: .map
      )
    }
  }

  public func joinIndicated(
    contextIdentifier: MAPContextIdentifier,
    port: P,
    attributeType: AttributeType,
    attributeSubtype: AttributeSubtype?,
    attributeValue: some Value,
    isNew: Bool,
    eventSource: EventSource
  ) async throws {
    precondition(!(attributeValue is AnyValue))
    do {
      if let observer = self as? any BaseApplicationEventObserver<P> {
        try await observer.onJoinIndication(
          contextIdentifier: contextIdentifier,
          port: port,
          attributeType: attributeType,
          attributeSubtype: attributeSubtype,
          attributeValue: attributeValue,
          isNew: isNew,
          eventSource: eventSource
        )
      }
    } catch MRPError.doNotPropagateAttribute {
      return
    }
    try await _propagateJoinIndicated(
      contextIdentifier: contextIdentifier,
      port: port,
      attributeType: attributeType,
      attributeSubtype: attributeSubtype,
      attributeValue: attributeValue,
      isNew: isNew,
      eventSource: eventSource
    )
  }

  private func _propagateLeaveIndicated(
    contextIdentifier: MAPContextIdentifier,
    port: P,
    attributeType: AttributeType,
    attributeSubtype: AttributeSubtype?,
    attributeValue: some Value,
    eventSource: EventSource
  ) async throws {
    guard shouldPropagate(eventSource: eventSource) else { return }
    try await apply(for: contextIdentifier) { participant in
      guard participant.port != port else { return }
      try await participant.leave(
        attributeType: attributeType,
        attributeSubtype: attributeSubtype,
        attributeValue: attributeValue,
        eventSource: .map
      )
    }
  }

  public func leaveIndicated(
    contextIdentifier: MAPContextIdentifier,
    port: P,
    attributeType: AttributeType,
    attributeSubtype: AttributeSubtype?,
    attributeValue: some Value,
    eventSource: EventSource
  ) async throws {
    precondition(!(attributeValue is AnyValue))
    do {
      if let observer = self as? any BaseApplicationEventObserver<P> {
        try await observer.onLeaveIndication(
          contextIdentifier: contextIdentifier,
          port: port,
          attributeType: attributeType,
          attributeSubtype: attributeSubtype,
          attributeValue: attributeValue,
          eventSource: eventSource
        )
      }
    } catch MRPError.doNotPropagateAttribute {
      return
    }
    try await _propagateLeaveIndicated(
      contextIdentifier: contextIdentifier,
      port: port,
      attributeType: attributeType,
      attributeSubtype: attributeSubtype,
      attributeValue: attributeValue,
      eventSource: eventSource
    )
  }
}
