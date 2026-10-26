// Copyright 2025 Lukas Lalinsky
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const net = Io.net;

pub const ConnectError = net.HostName.ValidateError || net.HostName.ConnectError || Io.ConcurrentError;
pub const SetKeepAliveError = std.posix.SetSockOptError;

const connect_options: net.IpAddress.ConnectOptions = .{ .mode = .stream, .protocol = .tcp };

/// How long an attempt gets before the next address is dialled beside it (RFC 8305's
/// "Connection Attempt Delay"; Go's dialer uses 300 ms).
const attempt_delay_ms = 250;
const max_addresses = 16;

/// Open a TCP connection to `host:port`. `host` may be an IP literal or a host name.
///
/// A host name's addresses are raced, Happy Eyeballs style (RFC 8305, as Go's dialer
/// does): resolved first, families interleaved (IPv6, IPv4, IPv6, …), the first dialled
/// at once and each next one `attempt_delay_ms` later — or at once when the previous
/// fails — while the earlier ones keep trying. The first to connect wins; the others are
/// cancelled, and a late success is closed. Before (NATS_ZIG_NOTES §34) they were dialled
/// one at a time with no limit: on a network that silently drops IPv6, a dual-stack host
/// connected over IPv4 only after the OS's TCP timeout (75 s on macOS) — past the
/// connection's 5 s handshake limit, so never. Zig 0.16 cannot bound a single connect
/// (`ConnectOptions.timeout` panics "TODO"), hence the race.
///
/// `std.Io.net.HostName.connect` is not used: it dials every address at once, with no
/// preference and no delay (and, in Zig 0.16.0, prints a stray debug line per attempt).
pub fn tcpConnectToHost(io: Io, host: []const u8, port: u16) ConnectError!net.Stream {
    if (net.IpAddress.parse(host, port)) |address| {
        return address.connect(io, connect_options);
    } else |_| {}

    var resolved: [max_addresses]net.IpAddress = undefined;
    const addresses = try resolveAll(io, host, port, &resolved);
    var ordered_buf: [max_addresses]net.IpAddress = undefined;
    const ordered = interleaveFamilies(addresses, &ordered_buf);
    return race(io, ordered);
}

/// Every address of `host`, in resolver order (at most `max_addresses`).
fn resolveAll(io: Io, host: []const u8, port: u16, out: *[max_addresses]net.IpAddress) ConnectError![]net.IpAddress {
    const host_name = try net.HostName.init(host);

    var canonical_name_buffer: [net.HostName.max_len]u8 = undefined;
    var lookup_buffer: [16]net.HostName.LookupResult = undefined;
    var lookup_queue: Io.Queue(net.HostName.LookupResult) = .init(&lookup_buffer);

    // The queue is consumed while the lookup fills it, so the lookup must run
    // concurrently; `lookup` guarantees it will not outgrow a queue with
    // capacity of at least 16 as long as someone keeps consuming it.
    var lookup_task = try io.concurrent(net.HostName.lookup, .{ host_name, io, &lookup_queue, .{
        .port = port,
        .canonical_name_buffer = &canonical_name_buffer,
    } });
    defer lookup_task.cancel(io) catch {};

    var n: usize = 0;
    while (lookup_queue.getOne(io)) |result| switch (result) {
        .address => |address| {
            if (n < out.len) {
                out[n] = address;
                n += 1;
            }
        },
        .canonical_name => continue,
    } else |err| switch (err) {
        error.Canceled => |e| return e,
        error.Closed => {},
    }
    // No address: report the lookup's own failure, else that there was none.
    if (n == 0) {
        try lookup_task.await(io);
        return error.UnknownHostName;
    }
    return out[0..n];
}

/// The addresses with the families alternating, starting with the resolver's first
/// choice and keeping its order within each family (RFC 8305 §4).
fn interleaveFamilies(addresses: []const net.IpAddress, out: *[max_addresses]net.IpAddress) []net.IpAddress {
    if (addresses.len == 0) return out[0..0];
    const first_is_ip6 = addresses[0] == .ip6;
    var a: usize = 0; // next of the first family
    var b: usize = 0; // next of the other
    var n: usize = 0;
    var take_first = true;
    while (n < addresses.len) {
        const want_ip6 = if (take_first) first_is_ip6 else !first_is_ip6;
        var idx = if (take_first) a else b;
        while (idx < addresses.len and (addresses[idx] == .ip6) != want_ip6) idx += 1;
        if (idx < addresses.len) {
            out[n] = addresses[idx];
            n += 1;
            if (take_first) a = idx + 1 else b = idx + 1;
        } else if (take_first) {
            a = addresses.len;
        } else {
            b = addresses.len;
        }
        take_first = !take_first;
        if (a >= addresses.len and b >= addresses.len) break;
    }
    return out[0..n];
}

const Dial = struct {
    result: net.IpAddress.ConnectError!net.Stream,
};

const RaceEvent = union(enum) {
    dial: Dial,
    delay: Io.Cancelable!void,
};

fn dialOne(io: Io, address: net.IpAddress) Dial {
    return .{ .result = address.connect(io, connect_options) };
}

fn attemptDelay(io: Io) Io.Cancelable!void {
    const d: Io.Clock.Duration = .{ .raw = .fromMilliseconds(attempt_delay_ms), .clock = .awake };
    return d.sleep(io);
}

/// The race itself: see tcpConnectToHost.
fn race(io: Io, addresses: []const net.IpAddress) ConnectError!net.Stream {
    // Room for every result a cancel may still have to collect: one dial and one delay
    // per address at most.
    var buffer: [2 * max_addresses]RaceEvent = undefined;
    var select: Io.Select(RaceEvent) = .init(io, &buffer);

    var next: usize = 0;
    var dialling: usize = 0;
    var last_err: ?net.IpAddress.ConnectError = null;

    const Start = struct {
        fn next_attempt(s: *Io.Select(RaceEvent), i: Io, addrs: []const net.IpAddress, nxt: *usize, dl: *usize) Io.ConcurrentError!void {
            try s.concurrent(.dial, dialOne, .{ i, addrs[nxt.*] });
            nxt.* += 1;
            dl.* += 1;
            if (nxt.* < addrs.len) try s.concurrent(.delay, attemptDelay, .{i});
        }
        /// Cancel what is left; a dial that connected meanwhile is closed.
        fn drain(s: *Io.Select(RaceEvent), i: Io) void {
            while (s.cancel()) |rest| switch (rest) {
                .dial => |d| if (d.result) |stream| stream.close(i) else |_| {},
                .delay => {},
            };
        }
    };

    Start.next_attempt(&select, io, addresses, &next, &dialling) catch |err| {
        Start.drain(&select, io);
        return err;
    };
    while (true) {
        const event = select.await() catch |err| {
            Start.drain(&select, io);
            return err;
        };
        switch (event) {
            .dial => |d| {
                dialling -= 1;
                if (d.result) |stream| {
                    Start.drain(&select, io);
                    return stream;
                } else |err| {
                    if (err == error.Canceled) {
                        Start.drain(&select, io);
                        return err;
                    }
                    last_err = err;
                    if (next < addresses.len) {
                        // A failure does not wait for the delay: the next address now.
                        Start.next_attempt(&select, io, addresses, &next, &dialling) catch |e| {
                            Start.drain(&select, io);
                            return e;
                        };
                    } else if (dialling == 0) {
                        Start.drain(&select, io);
                        return last_err.?;
                    }
                }
            },
            .delay => {
                if (next < addresses.len) {
                    Start.next_attempt(&select, io, addresses, &next, &dialling) catch |e| {
                        Start.drain(&select, io);
                        return e;
                    };
                }
            },
        }
    }
}

test "interleaveFamilies alternates, starting with the resolver's first family" {
    const v6a: net.IpAddress = .{ .ip6 = .loopback(1) };
    const v6b: net.IpAddress = .{ .ip6 = .loopback(2) };
    const v4a: net.IpAddress = .{ .ip4 = .loopback(3) };
    const v4b: net.IpAddress = .{ .ip4 = .loopback(4) };
    var out: [max_addresses]net.IpAddress = undefined;
    const got = interleaveFamilies(&.{ v6a, v6b, v4a, v4b }, &out);
    try std.testing.expectEqual(@as(usize, 4), got.len);
    try std.testing.expectEqual(@as(u16, 1), got[0].getPort());
    try std.testing.expectEqual(@as(u16, 3), got[1].getPort());
    try std.testing.expectEqual(@as(u16, 2), got[2].getPort());
    try std.testing.expectEqual(@as(u16, 4), got[3].getPort());
    // one family only: unchanged
    const one = interleaveFamilies(&.{ v4a, v4b }, &out);
    try std.testing.expectEqual(@as(usize, 2), one.len);
    try std.testing.expectEqual(@as(u16, 3), one[0].getPort());
}

test "tcpConnectToHost: a host name with one dead address still connects through another" {
    const io = std.testing.io;
    const listen_address: net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try listen_address.listen(io, .{});
    defer server.deinit(io);
    // "localhost" resolves to ::1 and 127.0.0.1; only the IPv4 one listens here.
    const stream = try tcpConnectToHost(io, "localhost", server.socket.address.getPort());
    defer stream.close(io);
    try std.testing.expect(stream.socket.address == .ip4);
}

/// Vectored write straight through the `Io` vtable. `net.Stream` only
/// writes through the `Io.Writer` interface, which erases the real error
/// behind `error.WriteFailed` and stashes it on the writer; the vtable
/// call keeps the typed error set - including cancellation - intact.
/// Returns the number of bytes written; short writes are possible.
pub fn writeVec(io: Io, stream: net.Stream, data: []const []const u8) net.Stream.Writer.Error!usize {
    return io.vtable.netWrite(io.userdata, stream.socket.handle, &.{}, data, 1);
}

/// Write all of `bytes` to the stream, looping over short writes.
pub fn writeAll(io: Io, stream: net.Stream, bytes: []const u8) net.Stream.Writer.Error!void {
    var pos: usize = 0;
    while (pos < bytes.len) {
        pos += try writeVec(io, stream, &.{bytes[pos..]});
    }
}

/// Enable or disable SO_KEEPALIVE. There is no socket-options API in std.Io,
/// so this goes through the raw socket handle.
pub fn setKeepAlive(socket: net.Socket, enabled: bool) SetKeepAliveError!void {
    if (builtin.os.tag == .windows) return;

    const value: c_int = @intFromBool(enabled);
    try std.posix.setsockopt(socket.handle, std.posix.SOL.SOCKET, std.posix.SO.KEEPALIVE, std.mem.asBytes(&value));
}

/// Turn off Nagle's algorithm: a small write leaves at once instead of waiting for the
/// previous segment's acknowledgement. NATS writes many small frames (a request, a pull,
/// a PONG); with Nagle on, one held behind a delayed ACK cost a request ~200 ms (measured
/// 2026-10-04 through a leaf over IPv6, NATS_ZIG_NOTES §32). The Go and JS clients set
/// it by default.
pub fn setNoDelay(socket: net.Socket, enabled: bool) SetKeepAliveError!void {
    if (builtin.os.tag == .windows) return;

    const value: c_int = @intFromBool(enabled);
    try std.posix.setsockopt(socket.handle, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&value));
}

test "tcpConnectToHost with an IP literal" {
    const io = std.testing.io;

    const listen_address: net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try listen_address.listen(io, .{});
    defer server.deinit(io);

    const stream = try tcpConnectToHost(io, "127.0.0.1", server.socket.address.getPort());
    defer stream.close(io);

    try setKeepAlive(stream.socket, true);
    try setNoDelay(stream.socket, true);
}

test "tcpConnectToHost with a host name" {
    const io = std.testing.io;

    const listen_address: net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try listen_address.listen(io, .{});
    defer server.deinit(io);

    const stream = try tcpConnectToHost(io, "localhost", server.socket.address.getPort());
    defer stream.close(io);
}

test "tcpConnectToHost with an invalid host name" {
    const io = std.testing.io;

    try std.testing.expectError(error.InvalidHostName, tcpConnectToHost(io, "invalid_host!", 4222));
}

test "tcpConnectToHost with nothing listening" {
    const io = std.testing.io;

    // Grab an ephemeral port and close the listener so the port is dead.
    const listen_address: net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try listen_address.listen(io, .{});
    const port = server.socket.address.getPort();
    server.deinit(io);

    try std.testing.expectError(error.ConnectionRefused, tcpConnectToHost(io, "127.0.0.1", port));
}
