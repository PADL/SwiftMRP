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

import AsyncExtensions

public protocol Port: Hashable, Sendable, Identifiable where ID: Hashable & Sendable {
  static func timeSinceEpoch() throws -> UInt32

  var isOperational: Bool { get }
  var isEnabled: Bool { get }
  var isPointToPoint: Bool { get }

  var name: String { get }
  var id: ID { get }
  var pvid: UInt16? { get }
  var vlans: Set<VLAN> { get }
  var mtu: Int { get }
  var latency: Int { get }
  var linkSpeed: Int { get }
  var isAvbCapable: Bool { get }

  var macAddress: EUI48 { get }
}

public enum PortNotification<P: Port>: Sendable {
  case added(P)
  case removed(P)
  case changed(P)

  var port: P {
    switch self {
    case let .added(port):
      port
    case let .removed(port):
      port
    case let .changed(port):
      port
    }
  }
}

extension Port {
  var contextIdentifiers: Set<MAPContextIdentifier> {
    Set(vlans.map { MAPContextIdentifier(vlan: $0) })
  }
}
