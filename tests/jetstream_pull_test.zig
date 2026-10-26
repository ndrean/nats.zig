const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");
const Connection = nats.Connection;
const JetStream = nats.JetStream;
const StreamConfig = nats.StreamConfig;
const ConsumerConfig = nats.ConsumerConfig;
const FetchRequest = nats.FetchRequest;
const MessageBatch = nats.MessageBatch;
const PullSubscription = nats.PullSubscription;

test "JetStream pull consumer basic fetch" {
    const io = std.testing.io;

    // Use existing utility functions
    const nc = try utils.createDefaultConnection(io);
    defer utils.closeConnection(nc);

    var js = nc.jetstream(.{});

    const stream_name = "TEST_PULL_STREAM";
    const consumer_name = "test_pull_consumer";

    // Create a test stream
    const stream_config = StreamConfig{
        .name = stream_name,
        .subjects = &[_][]const u8{"test.pull.*"},
        .storage = .memory,
        .max_msgs = 10,
    };

    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a pull consumer (ack_policy defaults to .explicit)
    var subscription = try js.pullSubscribe("test.pull.*", consumer_name, .{
        .stream = stream_name,
    });
    defer subscription.deinit();

    // Publish some test messages
    try nc.publish("test.pull.msg1", "Hello Pull 1");
    try nc.publish("test.pull.msg2", "Hello Pull 2");
    try nc.publish("test.pull.msg3", "Hello Pull 3");
    try nc.flush();

    // Fetch the messages
    var batch = try subscription.fetch(2, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
    defer batch.deinit();

    // Verify we got the expected messages
    try testing.expectEqual(null, batch.err); // always null now — see MessageBatch.err
    try testing.expectEqual(2, batch.messages.len);

    // Check message contents
    try testing.expectEqualStrings("Hello Pull 1", batch.messages[0].msg.data);
    try testing.expectEqualStrings("Hello Pull 2", batch.messages[1].msg.data);

    // Acknowledge messages
    try batch.messages[0].ack();
    try batch.messages[1].ack();

    // Fetch the messages
    var batch2 = try subscription.fetch(2, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
    defer batch2.deinit();

    // Verify we got the expected messages
    try testing.expectEqual(null, batch2.err);
    std.log.debug("Received messages: {}", .{batch2.messages.len});
    for (batch2.messages) |msg| {
        std.log.debug("Message: {s}", .{msg.msg.data});
    }
    try testing.expectEqual(1, batch2.messages.len);

    // Check message contents
    try testing.expectEqualStrings("Hello Pull 3", batch2.messages[0].msg.data);

    // Acknowledge messages
    try batch2.messages[0].ack();
}

test "JetStream pull subscribe resolves the stream from the subject" {
    const io = std.testing.io;

    const nc = try utils.createDefaultConnection(io);
    defer utils.closeConnection(nc);

    var js = nc.jetstream(.{});

    var stream_info = try js.addStream(.{
        .name = "TEST_PULL_LOOKUP_STREAM",
        .subjects = &.{"test.pull.lookup.*"},
        .storage = .memory,
    });
    defer stream_info.deinit();

    // No .stream: the name is looked up by subject, and must outlive the call.
    var subscription = try js.pullSubscribe("test.pull.lookup.*", "pull_lookup_consumer", .{});
    defer subscription.deinit();

    try nc.publish("test.pull.lookup.msg", "looked up");

    var batch = try subscription.fetch(1, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
    defer batch.deinit();

    try testing.expectEqual(null, batch.err);
    try testing.expectEqual(1, batch.messages.len);
    try testing.expectEqualStrings("looked up", batch.messages[0].msg.data);
}

test "JetStream pull fetch ignores a status left over from an earlier request" {
    const io = std.testing.io;

    const nc = try utils.createDefaultConnection(io);
    defer utils.closeConnection(nc);

    var js = nc.jetstream(.{});

    var stream_info = try js.addStream(.{
        .name = "TEST_PULL_STALE_STREAM",
        .subjects = &.{"test.pull.stale.*"},
        .storage = .memory,
    });
    defer stream_info.deinit();

    var subscription = try js.pullSubscribe("test.pull.stale.*", "pull_stale_consumer", .{
        .stream = "TEST_PULL_STALE_STREAM",
    });
    defer subscription.deinit();

    // Nothing to fetch: this gives up locally, and the server's 408 for the
    // expired request arrives afterwards.
    // (ZeBridge series, patch 05: an empty fetch is error.Timeout, not an empty batch.)
    try testing.expectError(error.Timeout, subscription.fetch(1, .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }));
    try io.sleep(.fromMilliseconds(200), .awake);

    try nc.publish("test.pull.stale.msg", "fresh");

    // The leftover 408 belongs to the first request and must not end this one.
    var batch = try subscription.fetch(1, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
    defer batch.deinit();

    try testing.expectEqual(null, batch.err);
    try testing.expectEqual(1, batch.messages.len);
    try testing.expectEqualStrings("fresh", batch.messages[0].msg.data);
}

// ── fetch contract (2026-08-29 patch, NOTES.md "fetch: return on the first message") ──

fn elapsedMs(io: std.Io, start: std.Io.Timestamp) i64 {
    const now = std.Io.Timestamp.now(io, .awake);
    return @intCast(@divTrunc(now.nanoseconds - start.nanoseconds, std.time.ns_per_ms));
}

test "JetStream pull fetch with nothing queued returns Timeout as an error" {
    const io = std.testing.io;
    const nc = try utils.createDefaultConnection(io);
    defer utils.closeConnection(nc);
    var js = nc.jetstream(.{});

    const stream_name = "TEST_PULL_EMPTY";
    var stream_info = try js.addStream(.{
        .name = stream_name,
        .subjects = &[_][]const u8{"test.empty.*"},
        .storage = .memory,
        .max_msgs = 10,
    });
    defer stream_info.deinit();
    defer js.deleteStream(stream_name) catch {};

    var sub = try js.pullSubscribe("test.empty.*", "empty_consumer", .{ .stream = stream_name });
    defer sub.deinit();

    // Nothing published: the expiry is an error the caller can `catch`, not a field
    // to remember to read.
    try testing.expectError(error.Timeout, sub.fetch(1, .{ .duration = .{ .raw = .fromMilliseconds(200), .clock = .awake } }));
}

test "JetStream pull fetch returns on the first message, not at expiry" {
    const io = std.testing.io;
    const nc = try utils.createDefaultConnection(io);
    defer utils.closeConnection(nc);
    var js = nc.jetstream(.{});

    const stream_name = "TEST_PULL_EARLY";
    var stream_info = try js.addStream(.{
        .name = stream_name,
        .subjects = &[_][]const u8{"test.early.*"},
        .storage = .memory,
        .max_msgs = 10,
    });
    defer stream_info.deinit();
    defer js.deleteStream(stream_name) catch {};

    var sub = try js.pullSubscribe("test.early.*", "early_consumer", .{ .stream = stream_name });
    defer sub.deinit();

    // ONE message queued, a fetch for a HUNDRED with a 2 s expiry.
    try nc.publish("test.early.one", "only one");
    try nc.flush();

    const start = std.Io.Timestamp.now(io, .awake);
    var batch = try sub.fetch(100, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    defer batch.deinit();
    const ms = elapsedMs(io, start);

    try testing.expectEqual(1, batch.messages.len);
    try testing.expectEqualStrings("only one", batch.messages[0].msg.data);
    // Unpatched: parks until the 2 s expiry (~2000 ms). Patched: the idle window after
    // the first message (10 ms) plus the round trip.
    std.debug.print("fetch(100) with one queued message returned after {d} ms\n", .{ms});
    try testing.expect(ms < 500);
}

test "JetStream pull fetch on a deleted consumer answers NoResponders, not Timeout" {
    const io = std.testing.io;
    const nc = try utils.createDefaultConnection(io);
    defer utils.closeConnection(nc);
    var js = nc.jetstream(.{});

    const stream_name = "TEST_PULL_GONE";
    var stream_info = try js.addStream(.{
        .name = stream_name,
        .subjects = &[_][]const u8{"test.gone.*"},
        .storage = .memory,
        .max_msgs = 10,
    });
    defer stream_info.deinit();
    defer js.deleteStream(stream_name) catch {};

    var sub = try js.pullSubscribe("test.gone.*", "gone_consumer", .{ .stream = stream_name });
    defer sub.deinit();

    // The consumer disappears underneath the subscription — what a server-side
    // inactive_threshold does to an idle client.
    try js.deleteConsumer(stream_name, "gone_consumer");

    const start = std.Io.Timestamp.now(io, .awake);
    const result = sub.fetch(1, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    const ms = elapsedMs(io, start);
    std.debug.print("fetch on a deleted consumer answered after {d} ms\n", .{ms});

    // An empty batch with a terminal status is a returned error (nats.go's legacy
    // `Fetch`). Unpatched: the 503 is discarded, the call waits out the 2 s and the
    // expiry comes back as an empty batch with `err = Timeout`.
    try testing.expectError(error.NoResponders, result);
    try testing.expect(ms < 500);
}

// ── shared inbox (2026-08-29 patch, NOTES.md "PullInbox") ──────────────────────

test "JetStream shared pull inbox: one wait over two streams, partial failure per consumer" {
    const io = std.testing.io;
    const nc = try utils.createDefaultConnection(io);
    defer utils.closeConnection(nc);
    var js = nc.jetstream(.{});

    var info_a = try js.addStream(.{ .name = "TEST_INBOX_A", .subjects = &[_][]const u8{"test.inbox.a.*"}, .storage = .memory, .max_msgs = 10 });
    defer info_a.deinit();
    defer js.deleteStream("TEST_INBOX_A") catch {};
    var info_b = try js.addStream(.{ .name = "TEST_INBOX_B", .subjects = &[_][]const u8{"test.inbox.b.*"}, .storage = .memory, .max_msgs = 10 });
    defer info_b.deinit();
    defer js.deleteStream("TEST_INBOX_B") catch {};

    const shared = try js.pullInbox();
    defer shared.deinit();
    var sub_a = try js.pullSubscribe("test.inbox.a.*", "inbox_a", .{ .stream = "TEST_INBOX_A", .inbox = shared });
    defer sub_a.deinit();
    var sub_b = try js.pullSubscribe("test.inbox.b.*", "inbox_b", .{ .stream = "TEST_INBOX_B", .inbox = shared });
    defer sub_b.deinit();

    // A private fetch on a shared-inbox subscription is refused.
    try testing.expectError(error.SharedInbox, sub_a.fetch(1, .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }));

    // Stream A is idle; ONE message on stream B. A single 2 s wait must end at once.
    try nc.publish("test.inbox.b.one", "from b");
    try nc.flush();
    const subs = [_]*PullSubscription{ sub_a, sub_b };

    var start = std.Io.Timestamp.now(io, .awake);
    var mb = try shared.fetch(&subs, 100, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    defer mb.deinit();
    var ms = elapsedMs(io, start);
    std.debug.print("shared inbox: one message on the second of two streams, returned after {d} ms\n", .{ms});
    try testing.expectEqual(1, mb.messages.len);
    try testing.expectEqualStrings("from b", mb.messages[0].msg.data);
    try testing.expectEqualStrings("TEST_INBOX_B", mb.messages[0].metadata.stream);
    try testing.expectEqualStrings("inbox_b", mb.messages[0].metadata.consumer);
    try testing.expect(!mb.gone[0] and !mb.gone[1]);
    try testing.expect(ms < 500);
    try mb.messages[0].ack();

    // Consumer A disappears (what an inactive_threshold does). The next wait reports
    // it per consumer, and still waits for B — an expiry, not an error.
    try js.deleteConsumer("TEST_INBOX_A", "inbox_a");
    start = std.Io.Timestamp.now(io, .awake);
    var mb2 = try shared.fetch(&subs, 100, .{ .duration = .{ .raw = .fromMilliseconds(300), .clock = .awake } });
    defer mb2.deinit();
    ms = elapsedMs(io, start);
    std.debug.print("shared inbox: consumer A deleted, B idle, returned after {d} ms with gone=[{}, {}]\n", .{ ms, mb2.gone[0], mb2.gone[1] });
    try testing.expectEqual(0, mb2.messages.len);
    try testing.expect(mb2.gone[0]);
    try testing.expect(!mb2.gone[1]);

    // Both gone: the whole fetch is an error, as with a single consumer.
    try js.deleteConsumer("TEST_INBOX_B", "inbox_b");
    try testing.expectError(error.NoResponders, shared.fetch(&subs, 1, .{ .duration = .{ .raw = .fromMilliseconds(300), .clock = .awake } }));
}
