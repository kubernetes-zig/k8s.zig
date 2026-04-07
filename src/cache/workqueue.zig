const std = @import("std");
const mem = std.mem;
const Io = std.Io;
const Allocator = mem.Allocator;
const testing = std.testing;

const k8s = @import("k8s_zig");
const Unstructured = k8s.Unstructured;

/// A rate-limited, deduplicating work queue.
/// Matches Go's workqueue.TypedRateLimitingInterface.
///
/// - Items are deduped: adding an item already in the queue is a no-op.
/// - Items being processed are tracked: re-adding a processing item queues it for later.
/// - Failed items can be requeued with exponential backoff.
/// - Shutdown drains the queue and unblocks all waiters.
pub fn WorkQueue(comptime T: type) type {
    return struct {
        const Self = @This();
        const MapContext = HashContext(T);
        const VoidMap = std.HashMapUnmanaged(T, void, MapContext, std.hash_map.default_max_load_percentage);
        const U32Map = std.HashMapUnmanaged(T, u32, MapContext, std.hash_map.default_max_load_percentage);
        const UsizeMap = std.HashMapUnmanaged(T, usize, MapContext, std.hash_map.default_max_load_percentage);
        const DelayedEntry = struct {
            item: T,
            ready_at: Io.Clock.Timestamp,
        };

        allocator: Allocator,
        io: Io,

        /// Items waiting to be processed.
        queue: std.ArrayList(T),
        queue_head: usize,
        /// Set of items in the queue (for dedup).
        dirty: VoidMap,
        /// Items currently being processed.
        processing: VoidMap,
        /// Per-item failure count for exponential backoff.
        failures: U32Map,
        /// Delayed re-add heap and index for rate-limited items.
        waiting: std.ArrayList(DelayedEntry),
        waiting_index: UsizeMap,
        delay_loop_group: Io.Group,
        delay_loop_started: bool,
        delay_wakeup: Io.Event,

        /// Synchronization.
        mutex: Io.Mutex,
        cond: Io.Condition,
        shutting_down: bool,

        /// Backoff config.
        base_delay_ms: u64,
        max_delay_ms: u64,

        pub const Options = struct {
            base_delay_ms: u64 = 5,
            max_delay_ms: u64 = 1_000_000, // 1000 seconds
        };

        pub fn init(allocator: Allocator, io: Io, opts: Options) Self {
            return .{
                .allocator = allocator,
                .io = io,
                .queue = .empty,
                .queue_head = 0,
                .dirty = .empty,
                .processing = .empty,
                .failures = .empty,
                .waiting = .empty,
                .waiting_index = .empty,
                .delay_loop_group = .init,
                .delay_loop_started = false,
                .delay_wakeup = .unset,
                .mutex = .init,
                .cond = .init,
                .shutting_down = false,
                .base_delay_ms = opts.base_delay_ms,
                .max_delay_ms = opts.max_delay_ms,
            };
        }

        pub fn deinit(self: *Self) void {
            self.shutdown();
            if (self.delay_loop_started) {
                self.delay_loop_group.await(self.io) catch {};
            }
            self.queue.deinit(self.allocator);
            self.dirty.deinit(self.allocator);
            self.processing.deinit(self.allocator);
            self.failures.deinit(self.allocator);
            self.waiting.deinit(self.allocator);
            self.waiting_index.deinit(self.allocator);
        }

        /// Add an item to the queue. If already queued or processing, deduped.
        pub fn add(self: *Self, item: T) !void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            if (self.shutting_down) return;

            // Already queued
            if (self.dirty.get(item) != null) return;

            try self.dirty.put(self.allocator, item, {});

            // If being processed, will be re-queued when Done is called
            if (self.processing.get(item) != null) return;

            try self.queue.append(self.allocator, item);
            self.cond.signal(self.io);
        }

        /// Get the next item to process. Blocks until an item is available or shutdown.
        /// Returns null on shutdown with empty queue.
        pub fn get(self: *Self) Allocator.Error!?T {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            while (self.queueLenLocked() == 0 and !self.shutting_down) {
                self.cond.wait(self.io, &self.mutex) catch return null;
            }

            if (self.queueLenLocked() == 0) return null; // shutdown

            // Pre-ensure capacity so we can track the item before dequeuing.
            try self.processing.ensureUnusedCapacity(self.allocator, 1);

            const item = self.queue.items[self.queue_head];
            self.queue_head += 1;
            self.compactQueueLocked();
            self.processing.putAssumeCapacity(item, {});
            _ = self.dirty.fetchRemove(item);
            return item;
        }

        /// Mark an item as done processing. If it was re-added while processing,
        /// it will be put back in the queue.
        pub fn done(self: *Self, item: T) !void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            _ = self.processing.fetchRemove(item);

            // Re-queued while processing
            if (self.dirty.get(item) != null) {
                try self.queue.append(self.allocator, item);
                self.cond.signal(self.io);
            } else if (self.processing.count() == 0) {
                self.cond.signal(self.io);
            }
        }

        /// Requeue an item with exponential backoff. Increments failure count.
        pub fn requeue(self: *Self, item: T) !void {
            self.mutex.lockUncancelable(self.io);

            const count = if (self.failures.get(item)) |c| c + 1 else @as(u32, 1);
            try self.failures.put(self.allocator, item, count);

            self.mutex.unlock(self.io);

            // Exponential backoff: base * 2^(failures-1), capped at max
            const delay = @min(
                self.base_delay_ms * (@as(u64, 1) << @intCast(@min(count - 1, 30))),
                self.max_delay_ms,
            );

            try self.addAfter(item, delay);
        }

        /// Reset failure count for an item (call after successful processing).
        pub fn forget(self: *Self, item: T) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            _ = self.failures.fetchRemove(item);
        }

        /// Number of items in the queue.
        pub fn len(self: *Self) usize {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            return self.queueLenLocked();
        }

        /// Signal shutdown. Unblocks all Get() calls.
        pub fn shutdown(self: *Self) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            self.shutting_down = true;
            self.cond.broadcast(self.io);
            self.delay_wakeup.set(self.io);
        }

        pub fn isShuttingDown(self: *Self) bool {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            return self.shutting_down;
        }

        /// Number of times an item has failed.
        pub fn numRequeues(self: *Self, item: T) u32 {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            return self.failures.get(item) orelse 0;
        }

        pub fn addAfter(self: *Self, item: T, delay_ms: u64) Allocator.Error!void {
            if (delay_ms == 0) {
                return self.add(item);
            }

            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            if (self.shutting_down) return;

            if (!self.delay_loop_started) {
                self.delay_loop_group.@"async"(self.io, delayLoopTask, .{self});
                self.delay_loop_started = true;
            }

            const delay: Io.Clock.Duration = .{
                .raw = Io.Duration.fromMilliseconds(@intCast(delay_ms)),
                .clock = .awake,
            };
            const ready_at = Io.Clock.Timestamp.now(self.io, .awake).addDuration(delay);
            const previous_head = self.waiting.items.len > 0;
            const previous_head_time = if (previous_head) self.waiting.items[0].ready_at else null;

            if (self.waiting_index.get(item)) |idx| {
                if (self.waiting.items[idx].ready_at.raw.nanoseconds <= ready_at.raw.nanoseconds) {
                    return;
                }
                self.waiting.items[idx].ready_at = ready_at;
                self.siftUpWaitingLocked(idx);
            } else {
                try self.waiting.ensureUnusedCapacity(self.allocator, 1);
                try self.waiting_index.ensureUnusedCapacity(self.allocator, 1);

                const idx = self.waiting.items.len;
                self.waiting.appendAssumeCapacity(.{
                    .item = item,
                    .ready_at = ready_at,
                });
                self.waiting_index.putAssumeCapacity(item, idx);
                self.siftUpWaitingLocked(idx);
            }

            const new_head = self.waiting.items[0].ready_at;
            if (!previous_head or new_head.raw.nanoseconds < previous_head_time.?.raw.nanoseconds) {
                self.delay_wakeup.set(self.io);
            }
        }

        fn queueLenLocked(self: *const Self) usize {
            return self.queue.items.len - self.queue_head;
        }

        fn compactQueueLocked(self: *Self) void {
            if (self.queue_head == 0) return;
            if (self.queue_head < 1024 and self.queue_head * 2 < self.queue.items.len) return;

            const remaining = self.queueLenLocked();
            std.mem.copyForwards(T, self.queue.items[0..remaining], self.queue.items[self.queue_head..]);
            self.queue.items.len = remaining;
            self.queue_head = 0;
        }

        fn delayLoopTask(self: *Self) void {
            while (true) {
                const action: union(enum) {
                    stop,
                    add: T,
                    wait,
                    wait_until: Io.Clock.Timestamp,
                } = blk: {
                    self.mutex.lockUncancelable(self.io);
                    defer self.mutex.unlock(self.io);

                    if (self.shutting_down) break :blk .stop;

                    if (self.waiting.items.len == 0) {
                        self.delay_wakeup.reset();
                        break :blk .wait;
                    }

                    const head = self.waiting.items[0];
                    const now = Io.Clock.Timestamp.now(self.io, .awake);
                    if (head.ready_at.raw.nanoseconds <= now.raw.nanoseconds) {
                        const ready = self.popWaitingLocked().?;
                        break :blk .{ .add = ready.item };
                    }

                    self.delay_wakeup.reset();
                    break :blk .{ .wait_until = head.ready_at };
                };

                switch (action) {
                    .stop => return,
                    .add => |item| self.add(item) catch {},
                    .wait => self.delay_wakeup.wait(self.io) catch return,
                    .wait_until => |deadline| {
                        self.delay_wakeup.waitTimeout(self.io, .{ .deadline = deadline }) catch |err| switch (err) {
                            error.Timeout => {},
                            error.Canceled => return,
                        };
                    },
                }
            }
        }

        fn popWaitingLocked(self: *Self) ?DelayedEntry {
            if (self.waiting.items.len == 0) return null;

            const item = self.waiting.items[0];
            _ = self.waiting_index.fetchRemove(item.item);

            const last_index = self.waiting.items.len - 1;
            if (last_index == 0) {
                self.waiting.items.len = 0;
                return item;
            }

            self.waiting.items[0] = self.waiting.items[last_index];
            self.waiting.items.len = last_index;
            self.waiting_index.getPtr(self.waiting.items[0].item).?.* = 0;
            self.siftDownWaitingLocked(0);
            return item;
        }

        fn siftUpWaitingLocked(self: *Self, start_index: usize) void {
            const child = self.waiting.items[start_index];
            var child_index = start_index;
            while (child_index > 0) {
                const parent_index = (child_index - 1) >> 1;
                const parent = self.waiting.items[parent_index];
                if (child.ready_at.raw.nanoseconds >= parent.ready_at.raw.nanoseconds) break;
                self.waiting.items[child_index] = parent;
                self.waiting_index.getPtr(parent.item).?.* = child_index;
                child_index = parent_index;
            }
            self.waiting.items[child_index] = child;
            self.waiting_index.getPtr(child.item).?.* = child_index;
        }

        fn siftDownWaitingLocked(self: *Self, start_index: usize) void {
            const target = self.waiting.items[start_index];
            var index = start_index;
            while (true) {
                var lesser_child = (std.math.mul(usize, index, 2) catch break) | 1;
                if (lesser_child >= self.waiting.items.len) break;

                const right_child = lesser_child + 1;
                if (right_child < self.waiting.items.len and
                    self.waiting.items[right_child].ready_at.raw.nanoseconds < self.waiting.items[lesser_child].ready_at.raw.nanoseconds)
                {
                    lesser_child = right_child;
                }

                if (target.ready_at.raw.nanoseconds <= self.waiting.items[lesser_child].ready_at.raw.nanoseconds) break;

                self.waiting.items[index] = self.waiting.items[lesser_child];
                self.waiting_index.getPtr(self.waiting.items[index].item).?.* = index;
                index = lesser_child;
            }

            self.waiting.items[index] = target;
            self.waiting_index.getPtr(target.item).?.* = index;
        }
    };
}

/// Fixed-size object key suitable for use in hash maps and work queues.
/// Stored as either `namespace/name` or `name` for cluster-scoped objects.
/// K8s max: namespace=63 + '/' + name=253 = 317. Rounded to 320.
pub const ObjectKey = struct {
    pub const max_len = 320;

    len: u16 = 0,
    namespace_len: u16 = 0,
    buf: [max_len]u8 = [_]u8{0} ** max_len,

    pub fn fromParts(namespace_value: ?[]const u8, object_name: []const u8) !ObjectKey {
        const ns = namespace_value orelse "";
        const total_len: usize = if (ns.len > 0) ns.len + 1 + object_name.len else object_name.len;
        if (total_len > max_len) return error.KeyTooLong;

        var key = ObjectKey{};
        key.len = @intCast(total_len);
        key.namespace_len = if (ns.len > 0) @intCast(ns.len) else 0;

        if (ns.len > 0) {
            @memcpy(key.buf[0..ns.len], ns);
            key.buf[ns.len] = '/';
            @memcpy(key.buf[ns.len + 1 .. total_len], object_name);
        } else {
            @memcpy(key.buf[0..object_name.len], object_name);
        }

        return key;
    }

    pub fn slice(self: *const ObjectKey) []const u8 {
        return self.buf[0..self.len];
    }

    pub fn fromObject(obj: *const Unstructured) !ObjectKey {
        const object_name = obj.getName() orelse return error.MissingName;
        return fromParts(obj.getNamespace(), object_name);
    }

    pub fn namespace(self: *const ObjectKey) ?[]const u8 {
        if (self.namespace_len == 0) return null;
        return self.buf[0..self.namespace_len];
    }

    pub fn name(self: *const ObjectKey) []const u8 {
        if (self.namespace_len == 0) return self.slice();
        return self.buf[self.namespace_len + 1 .. self.len];
    }

    pub fn eql(a: ObjectKey, b: ObjectKey) bool {
        return mem.eql(u8, a.slice(), b.slice());
    }

    pub fn format(self: ObjectKey, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        try writer.writeAll(self.slice());
    }
};

const ObjectKeyContext = struct {
    pub fn hash(_: @This(), key: ObjectKey) u64 {
        return std.hash_map.hashString(key.slice());
    }

    pub fn eql(_: @This(), a: ObjectKey, b: ObjectKey) bool {
        return ObjectKey.eql(a, b);
    }
};

fn HashContext(comptime T: type) type {
    return if (T == ObjectKey) ObjectKeyContext else std.hash_map.AutoContext(T);
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "workqueue: add and get" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    try q.add(1);
    try q.add(2);
    try q.add(3);

    try testing.expectEqual(@as(usize, 3), q.len());

    try testing.expectEqual(@as(u32, 1), (try q.get()).?);
    try testing.expectEqual(@as(u32, 2), (try q.get()).?);
    try testing.expectEqual(@as(u32, 3), (try q.get()).?);
}

test "workqueue: dedup" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    try q.add(1);
    try q.add(1); // deduped
    try q.add(1); // deduped

    try testing.expectEqual(@as(usize, 1), q.len());
    try testing.expectEqual(@as(u32, 1), (try q.get()).?);
}

test "workqueue: done allows re-add" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    try q.add(1);
    const item = (try q.get()).?;
    try testing.expectEqual(@as(u32, 1), item);

    // While processing, add same item — gets queued for after
    try q.add(1);
    try testing.expectEqual(@as(usize, 0), q.len()); // not in queue yet

    try q.done(item); // marks done, re-adds since dirty
    try testing.expectEqual(@as(usize, 1), q.len()); // now in queue
    try testing.expectEqual(@as(u32, 1), (try q.get()).?);
}

test "workqueue: shutdown returns null" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    q.shutdown();
    try testing.expect((try q.get()) == null);
    try testing.expect(q.isShuttingDown());
}

test "workqueue: add after shutdown is noop" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    q.shutdown();
    try q.add(1);
    try testing.expectEqual(@as(usize, 0), q.len());
}

test "workqueue: forget resets failure count" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    try q.add(1);
    _ = try q.get();

    // Simulate failures
    q.mutex.lockUncancelable(q.io);
    q.failures.put(q.allocator, 1, 5) catch {};
    q.mutex.unlock(q.io);

    try testing.expectEqual(@as(u32, 5), q.numRequeues(1));
    q.forget(1);
    try testing.expectEqual(@as(u32, 0), q.numRequeues(1));
}

test "workqueue: multiple items ordering" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    const items = [_]u32{ 5, 3, 7, 1, 9 };
    for (items) |item| try q.add(item);

    // FIFO order preserved
    for (items) |expected| {
        try testing.expectEqual(expected, (try q.get()).?);
    }
}

test "workqueue: interleaved add and get" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    try q.add(1);
    try testing.expectEqual(@as(u32, 1), (try q.get()).?);
    try q.done(1);

    try q.add(2);
    try q.add(3);
    try testing.expectEqual(@as(u32, 2), (try q.get()).?);

    try q.add(4);
    try testing.expectEqual(@as(u32, 3), (try q.get()).?);
    try testing.expectEqual(@as(u32, 4), (try q.get()).?);
}

test "workqueue: object key roundtrip" {
    const namespaced = try ObjectKey.fromParts("default", "demo");
    try testing.expectEqualStrings("default/demo", namespaced.slice());
    try testing.expectEqualStrings("default", namespaced.namespace().?);
    try testing.expectEqualStrings("demo", namespaced.name());

    const cluster = try ObjectKey.fromParts(null, "node-a");
    try testing.expectEqualStrings("node-a", cluster.slice());
    try testing.expect(cluster.namespace() == null);
    try testing.expectEqualStrings("node-a", cluster.name());
}

test "workqueue: object key validation errors" {
    var unnamed = try Unstructured.init(testing.allocator);
    defer unnamed.deinit();
    try testing.expectError(error.MissingName, ObjectKey.fromObject(&unnamed));

    var long_name_buf: [ObjectKey.max_len + 1]u8 = undefined;
    @memset(&long_name_buf, 'a');
    try testing.expectError(error.KeyTooLong, ObjectKey.fromParts(null, long_name_buf[0..]));
}

test "workqueue: requeue increments failure count and re-adds item" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    try q.add(7);
    const item = (try q.get()).?;
    try testing.expectEqual(@as(u32, 7), item);

    try q.done(item);
    try q.requeue(item);

    try testing.expectEqual(@as(u32, 1), q.numRequeues(item));
    var remaining_ms: u64 = 100;
    while (q.len() == 0 and remaining_ms > 0) {
        const step = @min(remaining_ms, 5);
        remaining_ms -= step;
        try Io.sleep(testing.io, Io.Duration.fromMilliseconds(@intCast(step)), .awake);
    }
    try testing.expectEqual(@as(usize, 1), q.len());
    try testing.expectEqual(@as(u32, 7), (try q.get()).?);
}

test "workqueue: requeue honors backoff delay without blocking caller" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
        .base_delay_ms = 25,
        .max_delay_ms = 25,
    });
    defer q.deinit();

    try q.add(9);
    const item = (try q.get()).?;
    try q.done(item);

    const start = Io.Clock.Timestamp.now(testing.io, .awake);
    try q.requeue(item);
    try testing.expectEqual(@as(usize, 0), q.len());

    try Io.sleep(testing.io, Io.Duration.fromMilliseconds(5), .awake);
    try testing.expectEqual(@as(usize, 0), q.len());

    var remaining_ms: u64 = 200;
    while (q.len() == 0 and remaining_ms > 0) {
        const step = @min(remaining_ms, 5);
        remaining_ms -= step;
        try Io.sleep(testing.io, Io.Duration.fromMilliseconds(@intCast(step)), .awake);
    }

    const elapsed_ms = @divTrunc(
        start.durationTo(Io.Clock.Timestamp.now(testing.io, .awake)).raw.nanoseconds,
        std.time.ns_per_ms,
    );

    try testing.expectEqual(@as(usize, 1), q.len());
    try testing.expect(elapsed_ms >= 20);
}

test "workqueue: addAfter coalesces duplicates to earliest deadline" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    try q.addAfter(1, 40);
    try q.addAfter(1, 5);

    var remaining_ms: u64 = 100;
    while (q.len() == 0 and remaining_ms > 0) {
        const step = @min(remaining_ms, 5);
        remaining_ms -= step;
        try Io.sleep(testing.io, Io.Duration.fromMilliseconds(@intCast(step)), .awake);
    }

    try testing.expectEqual(@as(usize, 1), q.len());
    try testing.expectEqual(@as(u32, 1), (try q.get()).?);
}

test "workqueue: addAfter does not delay an existing earlier deadline" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    const start = Io.Clock.Timestamp.now(testing.io, .awake);
    try q.addAfter(1, 5);
    try q.addAfter(1, 40);

    var remaining_ms: u64 = 100;
    while (q.len() == 0 and remaining_ms > 0) {
        const step = @min(remaining_ms, 5);
        remaining_ms -= step;
        try Io.sleep(testing.io, Io.Duration.fromMilliseconds(@intCast(step)), .awake);
    }

    const elapsed_ms = start.durationTo(Io.Clock.Timestamp.now(testing.io, .awake)).raw.toMilliseconds();
    try testing.expectEqual(@as(usize, 1), q.len());
    try testing.expect(elapsed_ms < 30);
}

test "workqueue: shutdown drops delayed items" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    try q.addAfter(1, 50);
    q.shutdown();
    try Io.sleep(testing.io, Io.Duration.fromMilliseconds(60), .awake);

    try testing.expectEqual(@as(usize, 0), q.len());
    try testing.expect((try q.get()) == null);
}

test "workqueue: randomized dedup round" {
    var prng = std.Random.DefaultPrng.init(0x5eed1234);
    const random = prng.random();

    var q = WorkQueue(u8).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    var expected_present = [_]bool{false} ** 16;
    for (0..256) |_| {
        const value: u8 = @intCast(random.uintLessThan(u8, expected_present.len));
        try q.add(value);
        expected_present[value] = true;
    }

    var drained_present = [_]bool{false} ** 16;
    var expected_count: usize = 0;
    for (expected_present) |present| {
        if (present) expected_count += 1;
    }

    for (0..expected_count) |_| {
        const item = (try q.get()).?;
        try testing.expect(!drained_present[item]);
        drained_present[item] = true;
        try q.done(item);
    }

    for (expected_present, drained_present) |expected, drained| {
        try testing.expectEqual(expected, drained);
    }
}

const QueueModel = struct {
    allocator: Allocator,
    queue: std.ArrayList(u8),
    dirty: [4]bool,
    processing: [4]bool,
    shutting_down: bool,

    fn init(allocator: Allocator) QueueModel {
        return .{
            .allocator = allocator,
            .queue = .empty,
            .dirty = [_]bool{false} ** 4,
            .processing = [_]bool{false} ** 4,
            .shutting_down = false,
        };
    }

    fn deinit(self: *QueueModel) void {
        self.queue.deinit(self.allocator);
    }

    fn add(self: *QueueModel, item: u8) !void {
        if (self.shutting_down) return;
        if (self.dirty[item]) return;
        self.dirty[item] = true;
        if (self.processing[item]) return;
        try self.queue.append(self.allocator, item);
    }

    fn get(self: *QueueModel) ?u8 {
        if (self.queue.items.len == 0) return null;
        const item = self.queue.orderedRemove(0);
        self.processing[item] = true;
        self.dirty[item] = false;
        return item;
    }

    fn done(self: *QueueModel, item: u8) !void {
        self.processing[item] = false;
        if (self.dirty[item]) {
            try self.queue.append(self.allocator, item);
        }
    }

    fn shutdown(self: *QueueModel) void {
        self.shutting_down = true;
    }
};

test "workqueue: randomized state machine matches model" {
    var prng = std.Random.DefaultPrng.init(0xdecafbad);
    const random = prng.random();

    var q = WorkQueue(u8).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    var model = QueueModel.init(testing.allocator);
    defer model.deinit();

    var in_flight: std.ArrayList(u8) = .empty;
    defer in_flight.deinit(testing.allocator);

    for (0..500) |_| {
        const can_get = model.queue.items.len > 0 or model.shutting_down;
        const can_done = in_flight.items.len > 0;
        const op: u8 = random.uintLessThan(u8, if (can_done) 4 else if (can_get) 3 else 2);

        switch (op) {
            0 => {
                const item: u8 = @intCast(random.uintLessThan(u8, 4));
                try q.add(item);
                try model.add(item);
            },
            1 => if (can_get) {
                const actual = try q.get();
                const expected = model.get();
                try testing.expectEqual(expected, actual);
                if (actual) |item| {
                    try in_flight.append(testing.allocator, item);
                }
            } else {
                model.shutdown();
                q.shutdown();
            },
            2 => if (can_done) {
                const idx = random.uintLessThan(usize, in_flight.items.len);
                const item = in_flight.orderedRemove(idx);
                try q.done(item);
                try model.done(item);
            } else {
                model.shutdown();
                q.shutdown();
            },
            3 => {
                model.shutdown();
                q.shutdown();
            },
            else => unreachable,
        }

        try testing.expectEqual(model.queue.items.len, q.len());
        try testing.expectEqual(model.shutting_down, q.isShuttingDown());
    }
}

test "workqueue: fuzz against queue model" {
    try std.testing.fuzz({}, fuzzQueueAgainstModel, .{});
}

fn fuzzQueueAgainstModel(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();

    var q = WorkQueue(u8).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    var model = QueueModel.init(testing.allocator);
    defer model.deinit();

    var in_flight: std.ArrayList(u8) = .empty;
    defer in_flight.deinit(testing.allocator);

    const steps = smith.valueRangeAtMost(u16, 1, 256);
    for (0..steps) |_| {
        const can_get = model.queue.items.len > 0 or model.shutting_down;
        const can_done = in_flight.items.len > 0;
        const op = smith.valueRangeAtMost(u8, 0, if (can_done) 3 else if (can_get) 2 else 1);

        switch (op) {
            0 => {
                const item = smith.valueRangeAtMost(u8, 0, 3);
                try q.add(item);
                try model.add(item);
            },
            1 => if (can_get) {
                const actual = try q.get();
                const expected = model.get();
                try testing.expectEqual(expected, actual);
                if (actual) |item| try in_flight.append(testing.allocator, item);
            } else {
                model.shutdown();
                q.shutdown();
            },
            2 => if (can_done) {
                const idx: usize = @intCast(smith.valueRangeLessThan(u8, 0, @intCast(in_flight.items.len)));
                const item = in_flight.orderedRemove(idx);
                try q.done(item);
                try model.done(item);
            } else {
                model.shutdown();
                q.shutdown();
            },
            3 => {
                model.shutdown();
                q.shutdown();
            },
            else => unreachable,
        }

        try testing.expectEqual(model.queue.items.len, q.len());
        try testing.expectEqual(model.shutting_down, q.isShuttingDown());
    }
}

test "workqueue: get tracks item in processing set" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    try q.add(42);
    const item = (try q.get()).?;
    try testing.expectEqual(@as(u32, 42), item);

    // Item is processing — re-adding marks dirty but doesn't queue
    try q.add(42);
    try testing.expectEqual(@as(usize, 0), q.len());

    // done() re-enqueues because dirty
    try q.done(item);
    try testing.expectEqual(@as(usize, 1), q.len());
}

test "workqueue: requeue tracks failure count across retries" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    try q.add(1);
    const item = (try q.get()).?;
    try q.done(item);

    const expected_counts = [_]u32{ 1, 2, 3 };
    for (expected_counts) |expected| {
        try q.requeue(item);
        try testing.expectEqual(expected, q.numRequeues(item));
        var remaining: u64 = 50;
        while (q.len() == 0 and remaining > 0) {
            const step = @min(remaining, 2);
            remaining -= step;
            try Io.sleep(testing.io, Io.Duration.fromMilliseconds(@intCast(step)), .awake);
        }
        const requeued = (try q.get()).?;
        try q.done(requeued);
    }

    q.forget(item);
    try testing.expectEqual(@as(u32, 0), q.numRequeues(item));
}

test "workqueue: addAfter with zero delay adds immediately" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    try q.addAfter(7, 0);
    try testing.expectEqual(@as(usize, 1), q.len());
    try testing.expectEqual(@as(u32, 7), (try q.get()).?);
}

// ── Additional table-driven tests ────────────────────────────────────────

test "workqueue: addAfter zero delay is immediate add" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    // Multiple zero-delay adds should behave like regular add (dedup)
    try q.addAfter(10, 0);
    try q.addAfter(20, 0);
    try q.addAfter(10, 0); // deduped

    try testing.expectEqual(@as(usize, 2), q.len());
    try testing.expectEqual(@as(u32, 10), (try q.get()).?);
    try testing.expectEqual(@as(u32, 20), (try q.get()).?);
}

test "workqueue: ObjectKey hash and equality" {
    const a = try ObjectKey.fromParts("default", "nginx");
    const b = try ObjectKey.fromParts("default", "nginx");

    // Same parts produce equal keys
    try testing.expect(ObjectKey.eql(a, b));
    try testing.expectEqualStrings(a.slice(), b.slice());

    // HashContext produces same hash for equal keys
    const ctx = ObjectKeyContext{};
    try testing.expectEqual(ctx.hash(a), ctx.hash(b));
    try testing.expect(ctx.eql(a, b));

    // Different keys are not equal
    const c = try ObjectKey.fromParts("other", "nginx");
    try testing.expect(!ObjectKey.eql(a, c));
    try testing.expect(!ctx.eql(a, c));

    // Cluster-scoped vs namespaced are not equal
    const d = try ObjectKey.fromParts(null, "nginx");
    try testing.expect(!ObjectKey.eql(a, d));
}

test "workqueue: ObjectKey boundary lengths" {
    const Case = struct {
        ns: ?[]const u8,
        name: []const u8,
        expect_error: bool,
    };

    // Build a name that exactly fills max_len
    var max_name_buf: [ObjectKey.max_len]u8 = undefined;
    @memset(&max_name_buf, 'x');

    // Build a name that exceeds max_len by 1
    var over_name_buf: [ObjectKey.max_len + 1]u8 = undefined;
    @memset(&over_name_buf, 'y');

    const cases = [_]Case{
        // Typical key — ok
        .{ .ns = "default", .name = "nginx", .expect_error = false },
        // max_len name, no namespace — ok
        .{ .ns = null, .name = max_name_buf[0..], .expect_error = false },
        // max_len+1 name, no namespace — error
        .{ .ns = null, .name = over_name_buf[0..], .expect_error = true },
        // namespace + name that together exceed max_len — error
        .{ .ns = "ns", .name = max_name_buf[0..], .expect_error = true },
    };
    for (cases) |c| {
        if (c.expect_error) {
            try testing.expectError(error.KeyTooLong, ObjectKey.fromParts(c.ns, c.name));
        } else {
            const key = try ObjectKey.fromParts(c.ns, c.name);
            if (c.ns) |ns| {
                if (ns.len > 0) {
                    try testing.expectEqualStrings(ns, key.namespace().?);
                }
            }
        }
    }
}

test "workqueue: done then re-add processes item again" {
    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{});
    defer q.deinit();

    // Add, get, done — full first pass
    try q.add(42);
    const first = (try q.get()).?;
    try testing.expectEqual(@as(u32, 42), first);
    try q.done(first);
    try testing.expectEqual(@as(usize, 0), q.len());

    // Re-add after done — should be queued again
    try q.add(42);
    try testing.expectEqual(@as(usize, 1), q.len());

    // Get again — same item
    const second = (try q.get()).?;
    try testing.expectEqual(@as(u32, 42), second);
    try q.done(second);
    try testing.expectEqual(@as(usize, 0), q.len());
}

