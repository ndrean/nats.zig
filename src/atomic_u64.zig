//! A 64-bit counter for every target. `std.atomic.Value(u64)` does not compile on 32-bit
//! ARM in Zig 0.16 ("expected 32-bit integer type or smaller"), and Android Go phones run
//! a 32-bit userspace (a moto e20: armeabi-v7a only, ZeBridge §10jc). On such targets the
//! value sits behind a spin lock with the same four operations the connection and the
//! subscriptions use; they are touched once per message and never contended enough for a
//! lock to matter. On 64-bit targets it IS `std.atomic.Value(u64)`.
const std = @import("std");

pub const AtomicU64 = if (@bitSizeOf(usize) >= 64) std.atomic.Value(u64) else Locked;

const Locked = struct {
    raw: u64,
    lock: std.atomic.Value(u32) = .init(0),

    pub fn init(v: u64) Locked {
        return .{ .raw = v };
    }

    fn acquire(self: *Locked) void {
        while (self.lock.cmpxchgWeak(0, 1, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
    }

    fn release(self: *Locked) void {
        self.lock.store(0, .release);
    }

    pub fn load(self: *const Locked, comptime _: std.builtin.AtomicOrder) u64 {
        const m: *Locked = @constCast(self);
        m.acquire();
        defer m.release();
        return m.raw;
    }

    pub fn store(self: *Locked, v: u64, comptime _: std.builtin.AtomicOrder) void {
        self.acquire();
        defer self.release();
        self.raw = v;
    }

    pub fn fetchAdd(self: *Locked, v: u64, comptime _: std.builtin.AtomicOrder) u64 {
        self.acquire();
        defer self.release();
        const old = self.raw;
        self.raw +%= v;
        return old;
    }

    pub fn fetchSub(self: *Locked, v: u64, comptime _: std.builtin.AtomicOrder) u64 {
        self.acquire();
        defer self.release();
        const old = self.raw;
        self.raw -%= v;
        return old;
    }
};

test "AtomicU64: the locked form behaves like the atomic one" {
    var a = Locked.init(5);
    try std.testing.expectEqual(@as(u64, 5), a.fetchAdd(3, .acq_rel));
    try std.testing.expectEqual(@as(u64, 8), a.fetchSub(2, .acq_rel));
    a.store(1 << 40, .release);
    try std.testing.expectEqual(@as(u64, 1 << 40), a.load(.acquire));
}
