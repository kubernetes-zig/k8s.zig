const std = @import("std");
const mem = std.mem;
const Io = std.Io;
const Allocator = mem.Allocator;
const testing = std.testing;

const WorkQueue = @import("workqueue.zig").WorkQueue;

/// A concurrent reconciler loop that pulls items from a WorkQueue and
/// dispatches them to a reconcile function via N parallel workers.
///
/// Mirrors the core loop from controller-runtime's internal controller:
///   - N workers dequeue items in a tight loop
///   - Each worker calls the reconcile function and handles the result
///   - ok → done + forget (reset backoff)
///   - requeue → done + requeue (with rate-limited backoff)
///   - requeue_after → done + forget + sleep + re-add
///   - err → done + requeue (with rate-limited backoff), up to max_retries
///   - Graceful shutdown: queue.shutdown() → workers drain and exit
pub fn Reconciler(comptime KeyType: type) type {
    return struct {
        const Self = @This();

        queue: *WorkQueue(KeyType),
        reconcile_ctx: *anyopaque,
        reconcile_fn: *const fn (ctx: *anyopaque, key: KeyType) ReconcileResult,
        num_workers: usize,
        max_retries: u32,
        io: Io,
        allocator: Allocator,

        /// Tracks how many workers are currently running, used for
        /// coordinated shutdown.
        active_workers: std.atomic.Value(u32),

        /// Set to true once start() has been called.
        started: bool,

        pub const ReconcileResult = union(enum) {
            /// Reconciliation succeeded. Item is forgotten (backoff reset)
            /// and marked done.
            ok,
            /// Reconciliation needs a retry. Item is requeued with
            /// rate-limited backoff.
            requeue,
            /// Reconciliation needs a retry after a specific delay (ms).
            /// Backoff counter is reset, then item is re-added after the delay.
            requeue_after: u64,
            /// Reconciliation failed. Item is requeued with rate-limited
            /// backoff unless max_retries is exceeded.
            err: anyerror,
        };

        pub const Options = struct {
            num_workers: usize = 1,
            max_retries: u32 = 10,
        };

        pub fn init(
            allocator: Allocator,
            io: Io,
            queue: *WorkQueue(KeyType),
            reconcile_ctx: *anyopaque,
            reconcile_fn: *const fn (ctx: *anyopaque, key: KeyType) ReconcileResult,
            opts: Options,
        ) Self {
            return .{
                .queue = queue,
                .reconcile_ctx = reconcile_ctx,
                .reconcile_fn = reconcile_fn,
                .num_workers = opts.num_workers,
                .max_retries = opts.max_retries,
                .io = io,
                .allocator = allocator,
                .active_workers = std.atomic.Value(u32).init(0),
                .started = false,
            };
        }

        /// Start the reconciler by spawning num_workers concurrent worker
        /// loops. Each worker pulls items from the queue until shutdown.
        /// This function blocks until all workers have finished (i.e. the
        /// queue has been shut down and drained).
        pub fn start(self: *Self) !void {
            if (self.started) return error.AlreadyStarted;
            self.started = true;

            var group: Io.Group = .init;
            for (0..self.num_workers) |_| {
                group.@"async"(self.io, Self.workerLoop, .{self});
            }
            group.await(self.io) catch {};
        }

        /// Signal the queue to shut down, which will cause all workers to
        /// drain remaining items and exit.
        pub fn stop(self: *Self) void {
            self.queue.shutdown();
        }

        /// The number of workers currently processing items.
        pub fn activeWorkers(self: *Self) u32 {
            return self.active_workers.load(.acquire);
        }

        /// A single worker loop: dequeue → reconcile → handle result → repeat.
        fn workerLoop(self: *Self) void {
            while (self.processNextWorkItem()) {}
        }

        /// Dequeue a single item, invoke the reconcile function, and
        /// handle the result. Returns false when the queue is shut down
        /// and empty (worker should exit).
        fn processNextWorkItem(self: *Self) bool {
            const item = (self.queue.get() catch return false) orelse return false;

            _ = self.active_workers.fetchAdd(1, .acq_rel);
            defer _ = self.active_workers.fetchSub(1, .acq_rel);

            const result = (self.reconcile_fn)(self.reconcile_ctx, item);
            self.handleResult(item, result);
            return true;
        }

        /// Apply the reconcile result to the queue, mirroring the Go
        /// reconcileHandler switch.
        fn handleResult(self: *Self, item: KeyType, result: ReconcileResult) void {
            switch (result) {
                .ok => {
                    self.queue.forget(item);
                    self.queue.done(item) catch |e| self.logQueueError("done", e);
                },
                .requeue => {
                    self.queue.done(item) catch |e| self.logQueueError("done", e);
                    self.queue.requeue(item) catch |e| self.logQueueError("requeue", e);
                },
                .requeue_after => |delay_ms| {
                    self.queue.forget(item);
                    self.queue.done(item) catch |e| self.logQueueError("done", e);
                    self.queue.addAfter(item, delay_ms) catch |e| self.logQueueError("addAfter", e);
                },
                .err => {
                    const retries = self.queue.numRequeues(item);
                    if (retries >= self.max_retries) {
                        self.queue.forget(item);
                        self.queue.done(item) catch |e| self.logQueueError("done", e);
                    } else {
                        self.queue.done(item) catch |e| self.logQueueError("done", e);
                        self.queue.requeue(item) catch |e| self.logQueueError("requeue", e);
                    }
                },
            }
        }

        fn logQueueError(self: *const Self, op: []const u8, err: anyerror) void {
            _ = self;
            std.log.err("reconciler: queue {s} failed: {}", .{ op, err });
        }

        pub const StartError = error{AlreadyStarted};
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

/// Shared mutable test state, keyed by item, tracking reconcile calls and
/// controlling what result to return. Uses atomics for cross-worker safety.
const TestTracker = struct {
    call_count: std.atomic.Value(u32),
    result_to_return: Reconciler(u32).ReconcileResult,

    fn init(result: Reconciler(u32).ReconcileResult) TestTracker {
        return .{
            .call_count = std.atomic.Value(u32).init(0),
            .result_to_return = result,
        };
    }
};

// We use thread-local-ish patterns here: a global tracker that tests configure
// before each run. This works because zig test is single-threaded at the test
// level (each test block runs sequentially).
var global_tracker: TestTracker = TestTracker.init(.ok);

fn testReconcile(ctx: *anyopaque, key: u32) Reconciler(u32).ReconcileResult {
    _ = ctx;
    _ = key;
    _ = global_tracker.call_count.fetchAdd(1, .acq_rel);
    return global_tracker.result_to_return;
}

// A reconcile fn that returns different results based on the key.
var per_key_results: [16]Reconciler(u32).ReconcileResult = .{.ok} ** 16;
var per_key_counts: [16]std.atomic.Value(u32) = .{std.atomic.Value(u32).init(0)} ** 16;

fn testReconcilePerKey(ctx: *anyopaque, key: u32) Reconciler(u32).ReconcileResult {
    _ = ctx;
    const idx: usize = @intCast(key);
    if (idx < per_key_results.len) {
        _ = per_key_counts[idx].fetchAdd(1, .acq_rel);
        return per_key_results[idx];
    }
    return .ok;
}

test "reconciler: single worker processes items" {
    global_tracker = TestTracker.init(.ok);

    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    try q.add(1);
    try q.add(2);
    try q.add(3);

    // Schedule shutdown after items are processed
    var r = Reconciler(u32).init(
        testing.allocator,
        testing.io,
        &q,
        @ptrFromInt(1),
        &testReconcile,
        .{ .num_workers = 1 },
    );

    // We need to shut down the queue from another "thread" so start()
    // can return. Use a group to run the shutdown concurrently.
    var group: Io.Group = .init;
    group.@"async"(testing.io, struct {
        fn run(queue: *WorkQueue(u32), tracker: *const TestTracker) void {
            // Spin-wait until all 3 items have been reconciled.
            while (tracker.call_count.load(.acquire) < 3) {
                Io.sleep(testing.io, Io.Duration.fromMilliseconds(1), .awake) catch {};
            }
            queue.shutdown();
        }
    }.run, .{ &q, &global_tracker });

    try r.start();
    group.await(testing.io) catch {};

    try testing.expectEqual(@as(u32, 3), global_tracker.call_count.load(.acquire));
}

test "reconciler: multiple workers" {
    global_tracker = TestTracker.init(.ok);

    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    const num_items: u32 = 20;
    for (0..num_items) |i| {
        try q.add(@intCast(i));
    }

    var r = Reconciler(u32).init(
        testing.allocator,
        testing.io,
        &q,
        @ptrFromInt(1),
        &testReconcile,
        .{ .num_workers = 4 },
    );

    var group: Io.Group = .init;
    group.@"async"(testing.io, struct {
        fn run(queue: *WorkQueue(u32), tracker: *const TestTracker) void {
            while (tracker.call_count.load(.acquire) < 20) {
                Io.sleep(testing.io, Io.Duration.fromMilliseconds(1), .awake) catch {};
            }
            queue.shutdown();
        }
    }.run, .{ &q, &global_tracker });

    try r.start();
    group.await(testing.io) catch {};

    try testing.expectEqual(@as(u32, num_items), global_tracker.call_count.load(.acquire));
}

test "reconciler: ok result forgets and completes item" {
    // Reset per-key state
    for (0..per_key_counts.len) |i| {
        per_key_counts[i] = std.atomic.Value(u32).init(0);
        per_key_results[i] = .ok;
    }

    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    try q.add(0);

    var r = Reconciler(u32).init(
        testing.allocator,
        testing.io,
        &q,
        @ptrFromInt(1),
        &testReconcilePerKey,
        .{ .num_workers = 1 },
    );

    var group: Io.Group = .init;
    group.@"async"(testing.io, struct {
        fn run(queue: *WorkQueue(u32)) void {
            while (per_key_counts[0].load(.acquire) < 1) {
                Io.sleep(testing.io, Io.Duration.fromMilliseconds(1), .awake) catch {};
            }
            // Give the worker a moment to call forget/done
            Io.sleep(testing.io, Io.Duration.fromMilliseconds(5), .awake) catch {};
            queue.shutdown();
        }
    }.run, .{&q});

    try r.start();
    group.await(testing.io) catch {};

    // Item should have been reconciled exactly once (ok = no requeue).
    try testing.expectEqual(@as(u32, 1), per_key_counts[0].load(.acquire));
    // Backoff counter should have been reset.
    try testing.expectEqual(@as(u32, 0), q.numRequeues(0));
}

test "reconciler: requeue result requeues item" {
    for (0..per_key_counts.len) |i| {
        per_key_counts[i] = std.atomic.Value(u32).init(0);
        per_key_results[i] = .ok;
    }
    per_key_results[0] = .requeue;

    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    try q.add(0);

    var r = Reconciler(u32).init(
        testing.allocator,
        testing.io,
        &q,
        @ptrFromInt(1),
        &testReconcilePerKey,
        .{ .num_workers = 1 },
    );

    var group: Io.Group = .init;
    group.@"async"(testing.io, struct {
        fn run(queue: *WorkQueue(u32)) void {
            // Requeue means the item keeps coming back; wait for several
            // reconcile calls to prove requeuing is working.
            while (per_key_counts[0].load(.acquire) < 3) {
                Io.sleep(testing.io, Io.Duration.fromMilliseconds(1), .awake) catch {};
            }
            queue.shutdown();
        }
    }.run, .{&q});

    try r.start();
    group.await(testing.io) catch {};

    // Should have been reconciled at least 3 times.
    try testing.expect(per_key_counts[0].load(.acquire) >= 3);
}

test "reconciler: error result requeues up to max retries then drops" {
    for (0..per_key_counts.len) |i| {
        per_key_counts[i] = std.atomic.Value(u32).init(0);
        per_key_results[i] = .ok;
    }
    per_key_results[0] = .{ .err = error.TestReconcileError };

    const max_retries: u32 = 3;

    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    try q.add(0);

    var r = Reconciler(u32).init(
        testing.allocator,
        testing.io,
        &q,
        @ptrFromInt(1),
        &testReconcilePerKey,
        .{ .num_workers = 1, .max_retries = max_retries },
    );

    var group: Io.Group = .init;
    group.@"async"(testing.io, struct {
        fn run(queue: *WorkQueue(u32)) void {
            // Wait enough time for the item to be retried and eventually dropped.
            // The item should be processed max_retries + 1 times total:
            // initial attempt + max_retries requeues, then the next attempt
            // sees retries >= max and drops it.
            Io.sleep(testing.io, Io.Duration.fromMilliseconds(500), .awake) catch {};
            queue.shutdown();
        }
    }.run, .{&q});

    try r.start();
    group.await(testing.io) catch {};

    // The item should have been processed. The exact count depends on
    // timing with the workqueue's requeue backoff, but it should be
    // more than 1 (was retried) and the requeue count should be 0
    // (was forgotten after exceeding max_retries).
    try testing.expect(per_key_counts[0].load(.acquire) >= 2);
    try testing.expectEqual(@as(u32, 0), q.numRequeues(0));
}

test "reconciler: shutdown stops workers" {
    global_tracker = TestTracker.init(.ok);

    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    var r = Reconciler(u32).init(
        testing.allocator,
        testing.io,
        &q,
        @ptrFromInt(1),
        &testReconcile,
        .{ .num_workers = 2 },
    );

    // Start reconciler, immediately shut down the queue.
    var group: Io.Group = .init;
    group.@"async"(testing.io, struct {
        fn run(queue: *WorkQueue(u32)) void {
            // Tiny delay to let workers start blocking on get().
            Io.sleep(testing.io, Io.Duration.fromMilliseconds(5), .awake) catch {};
            queue.shutdown();
        }
    }.run, .{&q});

    try r.start();
    group.await(testing.io) catch {};

    // No items were added, so no reconcile calls should have happened.
    try testing.expectEqual(@as(u32, 0), global_tracker.call_count.load(.acquire));
    try testing.expect(q.isShuttingDown());
}

test "reconciler: cannot start twice" {
    global_tracker = TestTracker.init(.ok);

    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    q.shutdown(); // pre-shut down so start returns immediately

    var r = Reconciler(u32).init(
        testing.allocator,
        testing.io,
        &q,
        @ptrFromInt(1),
        &testReconcile,
        .{ .num_workers = 1 },
    );

    try r.start();
    try testing.expectError(error.AlreadyStarted, r.start());
}

test "reconciler: table-driven result handling" {
    const Case = struct {
        name: []const u8,
        result: Reconciler(u32).ReconcileResult,
        /// Minimum expected reconcile count after the test.
        min_calls: u32,
        /// Whether the item's backoff should be zero after the test.
        expect_forgotten: bool,
    };

    const cases = [_]Case{
        .{
            .name = "ok forgets and completes",
            .result = .ok,
            .min_calls = 1,
            .expect_forgotten = true,
        },
        .{
            .name = "requeue requeues with backoff",
            .result = .requeue,
            .min_calls = 2,
            .expect_forgotten = false,
        },
        .{
            .name = "error requeues with backoff",
            .result = .{ .err = error.SomeError },
            .min_calls = 2,
            .expect_forgotten = false,
        },
    };

    for (cases) |tc| {
        // Reset
        for (0..per_key_counts.len) |i| {
            per_key_counts[i] = std.atomic.Value(u32).init(0);
            per_key_results[i] = .ok;
        }
        per_key_results[0] = tc.result;

        var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
            .base_delay_ms = 1,
            .max_delay_ms = 1,
        });
        defer q.deinit();

        try q.add(0);

        var r = Reconciler(u32).init(
            testing.allocator,
            testing.io,
            &q,
            @ptrFromInt(1),
            &testReconcilePerKey,
            .{ .num_workers = 1, .max_retries = 10 },
        );

        var group: Io.Group = .init;
        group.@"async"(testing.io, struct {
            fn run(queue: *WorkQueue(u32), min: u32) void {
                // Wait until the minimum number of reconcile calls.
                while (per_key_counts[0].load(.acquire) < min) {
                    Io.sleep(testing.io, Io.Duration.fromMilliseconds(1), .awake) catch {};
                }
                Io.sleep(testing.io, Io.Duration.fromMilliseconds(5), .awake) catch {};
                queue.shutdown();
            }
        }.run, .{ &q, tc.min_calls });

        try r.start();
        group.await(testing.io) catch {};

        try testing.expect(per_key_counts[0].load(.acquire) >= tc.min_calls);

        if (tc.expect_forgotten) {
            try testing.expectEqual(@as(u32, 0), q.numRequeues(0));
        }
    }
}

const RequeueAfterContext = struct {
    call_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    first_call_ns: std.atomic.Value(i64) = std.atomic.Value(i64).init(0),
    second_call_ns: std.atomic.Value(i64) = std.atomic.Value(i64).init(0),
};

fn reconcileWithRequeueAfter(ctx: *anyopaque, key: u32) Reconciler(u32).ReconcileResult {
    _ = key;
    const state: *RequeueAfterContext = @ptrCast(@alignCast(ctx));
    const call_number = state.call_count.fetchAdd(1, .acq_rel) + 1;
    const now = Io.Clock.Timestamp.now(testing.io, .awake).raw.toNanoseconds();
    if (call_number == 1) {
        state.first_call_ns.store(@intCast(now), .release);
        return .{ .requeue_after = 10 };
    }

    state.second_call_ns.store(@intCast(now), .release);
    return .ok;
}

test "reconciler: requeue_after delays and re-adds item" {
    var ctx = RequeueAfterContext{};

    var q = WorkQueue(u32).init(testing.allocator, testing.io, .{
        .base_delay_ms = 1,
        .max_delay_ms = 1,
    });
    defer q.deinit();

    try q.add(0);

    var r = Reconciler(u32).init(
        testing.allocator,
        testing.io,
        &q,
        @ptrCast(&ctx),
        &reconcileWithRequeueAfter,
        .{ .num_workers = 1 },
    );

    var group: Io.Group = .init;
    group.@"async"(testing.io, struct {
        fn run(queue: *WorkQueue(u32), state: *RequeueAfterContext) void {
            while (state.call_count.load(.acquire) < 2) {
                Io.sleep(testing.io, Io.Duration.fromMilliseconds(1), .awake) catch {};
            }
            queue.shutdown();
        }
    }.run, .{ &q, &ctx });

    try r.start();
    group.await(testing.io) catch {};

    try testing.expectEqual(@as(u32, 2), ctx.call_count.load(.acquire));
    try testing.expectEqual(@as(u32, 0), q.numRequeues(0));

    const first_call_ns = ctx.first_call_ns.load(.acquire);
    const second_call_ns = ctx.second_call_ns.load(.acquire);
    try testing.expect(second_call_ns > first_call_ns);

    const delta_ms = @divTrunc(second_call_ns - first_call_ns, std.time.ns_per_ms);
    try testing.expect(delta_ms >= 8);
}
