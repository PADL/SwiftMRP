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

import AsyncAlgorithms
import AsyncExtensions
import CNetLink
import Dispatch
import Glibc
import SystemPackage

public final class NLObject: @unchecked Sendable, Equatable, Hashable, CustomStringConvertible {
  public static func == (_ lhs: NLObject, _ rhs: NLObject) -> Bool {
    lhs._obj.withCriticalRegion { lhs in
      rhs._obj.withCriticalRegion { rhs in
        nl_object_identical(lhs, rhs) != 0
      }
    }
  }

  let _obj: ManagedCriticalState<OpaquePointer>

  convenience init(msg: OpaquePointer) throws {
    var obj: OpaquePointer! = nil

    try withUnsafeMutablePointer(to: &obj) { objRef in
      _ = try throwingErrno {
        nl_msg_parse(msg, { obj, objRef in
          nl_object_get(obj)
          objRef!
            .withMemoryRebound(
              to: OpaquePointer.self,
              capacity: 1
            ) { objRef in
              objRef.pointee = obj!
            }
        }, objRef)
      }
    }

    self.init(obj: obj)
    nl_object_put(obj)
  }

  public init(obj: OpaquePointer!) {
    nl_object_get(obj)
    _obj = ManagedCriticalState(obj)
  }

  deinit {
    _obj.withCriticalRegion { nl_object_put($0) }
  }

  public var description: String {
    var buffer = [CChar](repeating: 0, count: 1024)
    _obj.withCriticalRegion { obj in
      buffer.withUnsafeMutableBufferPointer {
        nl_object_dump_buf(obj, $0.baseAddress!, $0.count)
      }
    }
    return String(cString: buffer)
  }

  public var isMarked: Bool {
    get {
      nl_object_is_marked(_obj.criticalState) != 0
    }
    set {
      _obj.withCriticalRegion { obj in
        newValue ? nl_object_mark(obj) : nl_object_unmark(obj)
      }
    }
  }

  public func hash(into hasher: inout Hasher) {
    var hashkey: UInt32 = 0
    _obj.withCriticalRegion { obj in
      nl_object_keygen(obj, &hashkey, UInt32(MemoryLayout<UInt32>.size))
    }
    hasher.combine(hashkey)
  }

  public var typeString: String {
    String(cString: nl_object_get_type(_obj.criticalState))
  }

  public var messageType: Int {
    Int(nl_object_get_msgtype(_obj.criticalState))
  }

  public var isAttributeMask: UInt32 {
    nl_object_get_id_attrs(_obj.criticalState)
  }

  public func apply<T>(_ block: (OpaquePointer) throws -> T) rethrows -> T {
    try _obj.withCriticalRegion { obj in
      try block(obj)
    }
  }
}

private func NLSocket_CB(
  _ msg: OpaquePointer!,
  _ arg: UnsafeMutableRawPointer!
) -> CInt {
  let nlSocket = Unmanaged<NLSocket>.fromOpaque(arg).takeUnretainedValue()
  let hdr = nlmsg_hdr(msg)!
  do {
    let object = try NLObject(msg: msg)
    nlSocket.resume(sequence: hdr.pointee.nlmsg_seq, with: Result.success(object))
  } catch {
    nlSocket.resume(sequence: hdr.pointee.nlmsg_seq, with: Result.failure(error))
  }
  return 0
}

private func NLSocket_ErrCB(
  _ nla: UnsafeMutablePointer<sockaddr_nl>?,
  _ err: UnsafeMutablePointer<nlmsgerr>?,
  _ arg: UnsafeMutableRawPointer?
) -> CInt {
  debugPrint("NLSocket_ErrCB")
  return 0
}

public final class NLSocket {
  private typealias Continuation = CheckedContinuation<NLObject, Error>
  private typealias Channel = AsyncThrowingChannel<NLObject, Error>

  private enum _Request {
    case response(Continuation)
    case event(Channel)
  }

  fileprivate let _sk: OpaquePointer!
  private let _readSource: any DispatchSourceRead
  private let _lastError = ManagedCriticalState<CInt>(0)
  private let _requests = ManagedCriticalState<[UInt32: _Request]>([:])

  public init(protocol: Int32) throws {
    guard let sk = nl_socket_alloc() else { throw Errno.noMemory }
    _sk = sk

    try throwingErrno {
      nl_connect(sk, `protocol`)
    }
    nl_socket_set_nonblocking(sk)

    let fd = nl_socket_get_fd(sk)
    precondition(fd >= 0)

    _readSource = DispatchSource.makeReadSource(fileDescriptor: fd, queue: .main)
    _readSource.setEventHandler(handler: onReadReady)

    nl_socket_modify_cb(
      sk,
      NL_CB_VALID,
      NL_CB_CUSTOM,
      NLSocket_CB,
      Unmanaged.passUnretained(self).toOpaque()
    )
    nl_socket_modify_err_cb(
      sk,
      NL_CB_CUSTOM,
      NLSocket_ErrCB,
      Unmanaged.passUnretained(self).toOpaque()
    )

    _readSource.resume()
  }

  deinit {
    _readSource.cancel()
    nl_socket_free(_sk)
  }

  public func connect(proto: CInt) throws {
    try throwingErrno { nl_connect(_sk, proto) }
  }

  public func addMembership(group: CInt) throws {
    try throwingErrno { nl_socket_add_membership(_sk, group) }
  }

  public func dropMembership(group: CInt) throws {
    try throwingErrno { nl_socket_drop_membership(_sk, group) }
  }

  public func setPassCred(_ value: Bool) throws {
    try throwingErrno { nl_socket_set_passcred(_sk, value ? 1 : 0) }
  }

  public var messageBufferSize: Int {
    get {
      nl_socket_get_msg_buf_size(_sk)
    }
    set {
      nl_socket_set_msg_buf_size(_sk, newValue)
    }
  }

  public func setAutoAck(_ enabled: Bool) {
    enabled ? nl_socket_enable_auto_ack(_sk) : nl_socket_disable_auto_ack(_sk)
  }

  private func onReadReady() {
    let r = nl_recvmsgs_default(_sk)
    guard r >= 0 else {
      _lastError.withCriticalRegion { $0 = -r }
      return
    }
  }

  public func useNextSequenceNumber() -> UInt32 {
    nl_socket_use_seq(_sk)
  }

  fileprivate func resume(
    sequence: UInt32,
    with result: Result<NLObject, Error>
  ) {
    var request: _Request!

    _requests.withCriticalRegion {
      request = $0[sequence]
      if case .response = request {
        $0.removeValue(forKey: sequence)
      }
    }

    switch request! {
    case .response(let continuation):
      continuation.resume(with: result)
    case .event(let channel):
      switch result {
      case .success(let value):
        Task { await channel.send(value) }
      case .failure(let error):
        channel.fail(error)
      }
    }
  }

  func request_N(
    message: consuming NLMessage
  ) throws -> AnyAsyncSequence<NLObject> {
    let sequence = message.sequence
    var channel: Channel!
    _requests.withCriticalRegion {
      let _channel = Channel()
      $0[sequence] = .event(_channel)
      channel = _channel
    }
    try message.send(on: self)
    return channel.eraseToAnyAsyncSequence()
  }

  func request_N(
    message: consuming NLMessage
  ) async throws -> NLObject {
    let sequence = message.sequence
    return try await withTaskCancellationHandler(operation: {
      try await withCheckedThrowingContinuation { continuation in
        _requests.withCriticalRegion { $0[sequence] = .response(continuation) }
        do {
          try message.send(on: self)
        } catch {
          resume(sequence: sequence, with: .failure(error))
        }
      }
    }, onCancel: {
      resume(sequence: sequence, with: .failure(CancellationError()))
    })
  }
}

@discardableResult
func throwingErrno(_ body: () -> CInt) throws -> CInt {
  let r = body()
  guard r >= 0 else {
    throw Errno(rawValue: -r)
  }
  return r
}

public protocol NLObjectConstructible {
  init(object: NLObject) throws
}

struct NLAttribute {
  var _nla: UnsafePointer<nlattr>

  var type: CInt {
    nla_type(_nla)
  }

  var length: CInt {
    nla_len(_nla)
  }

  func get(attrtype: CInt) -> UInt32 {
    nla_get_u32(_nla)
  }
}

struct NLMessage: ~Copyable {
  var _msg: OpaquePointer!

  init(type: CInt, flags: CInt) throws {
    _msg = nlmsg_alloc_simple(type, flags)
    guard _msg != nil else { throw Errno.noMemory }
  }

  init(hdr: UnsafeMutablePointer<nlmsghdr>) throws {
    _msg = nlmsg_convert(hdr)
    guard _msg != nil else { throw Errno.noMemory }
  }

  init(
    socket: NLSocket,
    type: Int,
    flags: Int32 = 0
  ) throws {
    try self.init(seq: socket.useNextSequenceNumber(), type: type, flags: flags)
  }

  init(
    pid: UInt32 = UInt32(NL_AUTO_PID),
    seq: UInt32 = UInt32(NL_AUTO_SEQ),
    type: Int,
    flags: Int32 = 0
  ) throws {
    var nlh = nlmsghdr()
    nlh.nlmsg_type = UInt16(type)
    nlh.nlmsg_flags = UInt16(flags)
    nlh.nlmsg_seq = seq
    nlh.nlmsg_pid = pid

    _msg = nlmsg_inherit(&nlh)
  }

  func append(_ data: [UInt8], pad: CInt = CInt(NLMSG_ALIGNTO)) throws {
    var data = data
    try throwingErrno {
      data.withUnsafeMutableBufferPointer {
        nlmsg_append(_msg, $0.baseAddress, $0.count, pad)
      }
    }
  }

  func expand(to newlen: Int) throws {
    try throwingErrno { nlmsg_expand(_msg, newlen) }
  }

  func reserve(length: Int, pad: CInt = CInt(NLMSG_ALIGNTO)) throws -> UnsafeMutableRawPointer {
    let ptr = nlmsg_reserve(_msg, length, pad)
    guard let ptr else { throw Errno.noMemory }
    return ptr
  }

  func put(
    pid: UInt32 = UInt32(NL_AUTO_PID),
    seq: UInt32 = UInt32(NL_AUTO_SEQ),
    type: CInt,
    payload: CInt,
    flags: CInt = 0
  ) throws -> UnsafeMutablePointer<nlmsghdr> {
    guard let msghdr = nlmsg_put(_msg, pid, seq, type, payload, flags) else {
      throw Errno.noMemory
    }
    return msghdr
  }

  func put(u8 value: UInt8, for attrtype: CInt) throws {
    try throwingErrno { nla_put_u8(_msg, attrtype, value) }
  }

  func put(u16 value: UInt16, for attrtype: CInt) throws {
    try throwingErrno { nla_put_u16(_msg, attrtype, value) }
  }

  func put(u32 value: UInt32, for attrtype: CInt) throws {
    try throwingErrno { nla_put_u32(_msg, attrtype, value) }
  }

  func put(u64 value: UInt64, for attrtype: CInt) throws {
    try throwingErrno { nla_put_u64(_msg, attrtype, value) }
  }

  var sequence: UInt32 {
    let hdr = nlmsg_hdr(_msg)!
    return hdr.pointee.nlmsg_seq
  }

  func send(on socket: NLSocket) throws {
    try throwingErrno { nl_send_auto(socket._sk, _msg) }
  }

  deinit {
    nlmsg_free(_msg)
  }
}
