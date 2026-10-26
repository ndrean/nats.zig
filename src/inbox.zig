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
const nuid = @import("nuid.zig");

/// Default inbox prefix used by NATS clients, without its trailing dot.
pub const default_prefix = "_INBOX";

/// Longest accepted prefix. The response manager holds its reply prefix in an
/// inline buffer sized from this, so a custom prefix stays bounded instead of
/// turning that buffer into an allocation.
pub const max_prefix_len = 64;

/// Default inbox prefix used by NATS clients
const INBOX_PREFIX = default_prefix ++ ".";

/// A prefix has to work as the leading tokens of a subject: non-empty, no
/// wildcard, no whitespace, no trailing dot (the dot is added here), and short
/// enough for the inline buffer. The rules nats.go's CustomInboxPrefix applies,
/// plus the length bound this implementation needs.
pub fn validatePrefix(prefix: []const u8) error{InvalidInboxPrefix}!void {
    if (prefix.len == 0 or prefix.len > max_prefix_len) return error.InvalidInboxPrefix;
    if (prefix[prefix.len - 1] == '.') return error.InvalidInboxPrefix;
    for (prefix) |c| switch (c) {
        '*', '>', ' ', '\t', '\r', '\n' => return error.InvalidInboxPrefix,
        else => {},
    };
}

/// Generate a unique inbox subject for request/reply pattern
/// Format: _INBOX.<22-char-nuid>
pub fn newInbox(allocator: std.mem.Allocator) ![]u8 {
    return newInboxWithPrefix(allocator, default_prefix);
}

/// The same under a caller-chosen prefix: `<prefix>.<22-char-nuid>`. A
/// connection's own prefix is reached through `Connection.newInbox`, which is
/// what everything inside this library uses — a client granted only
/// `_INBOX.<name>.>` must not generate a reply subject outside it.
pub fn newInboxWithPrefix(allocator: std.mem.Allocator, prefix: []const u8) ![]u8 {
    var nuid_buf: [nuid.NUID_TOTAL_LEN]u8 = undefined;
    nuid.next(&nuid_buf);
    return try std.fmt.allocPrint(allocator, "{s}.{s}", .{ prefix, nuid_buf });
}

test "a custom prefix replaces the default, and the default is unchanged" {
    const allocator = std.testing.allocator;

    const default_inbox = try newInbox(allocator);
    defer allocator.free(default_inbox);
    try std.testing.expect(std.mem.startsWith(u8, default_inbox, "_INBOX."));

    const scoped = try newInboxWithPrefix(allocator, "_INBOX.alice");
    defer allocator.free(scoped);
    try std.testing.expect(std.mem.startsWith(u8, scoped, "_INBOX.alice."));
    // One nuid past the prefix and its dot, nothing else.
    try std.testing.expectEqual("_INBOX.alice.".len + nuid.NUID_TOTAL_LEN, scoped.len);
}

test "a prefix has to be usable as a subject" {
    try validatePrefix("_INBOX");
    try validatePrefix("_INBOX.alice");
    try validatePrefix("replies.eu-west");
    try validatePrefix(&@as([max_prefix_len]u8, @splat('x')));

    try std.testing.expectError(error.InvalidInboxPrefix, validatePrefix(""));
    try std.testing.expectError(error.InvalidInboxPrefix, validatePrefix("_INBOX."));
    try std.testing.expectError(error.InvalidInboxPrefix, validatePrefix("_INBOX.>"));
    try std.testing.expectError(error.InvalidInboxPrefix, validatePrefix("_INBOX.*"));
    try std.testing.expectError(error.InvalidInboxPrefix, validatePrefix("_INBOX alice"));
    try std.testing.expectError(error.InvalidInboxPrefix, validatePrefix(&@as([max_prefix_len + 1]u8, @splat('x'))));
}

test "inbox generation" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const inbox1 = try newInbox(allocator);
    defer allocator.free(inbox1);

    const inbox2 = try newInbox(allocator);
    defer allocator.free(inbox2);

    // Should start with _INBOX.
    try testing.expect(std.mem.startsWith(u8, inbox1, INBOX_PREFIX));
    try testing.expect(std.mem.startsWith(u8, inbox2, INBOX_PREFIX));

    // Should be 29 characters total (7 + 22)
    try testing.expectEqual(@as(usize, 29), inbox1.len);
    try testing.expectEqual(@as(usize, 29), inbox2.len);

    // Should be unique
    try testing.expect(!std.mem.eql(u8, inbox1, inbox2));
}
