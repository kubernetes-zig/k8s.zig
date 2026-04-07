const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const k8s = @import("k8s_zig");
const cache = @import("k8s_cache");

const Unstructured = k8s.Unstructured;

const ReconcileBenchContext = struct {
    call_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var io_instance: Io.Threaded = .init(allocator, .{});
    defer io_instance.deinit();
    const io = io_instance.io();

    const queue_items = envInt("K8S_ZIG_BENCH_QUEUE_ITEMS", 1_000_000);
    const store_objects = envInt("K8S_ZIG_BENCH_STORE_OBJECTS", 200_000);
    const store_rounds = envInt("K8S_ZIG_BENCH_STORE_ROUNDS", 5);
    const reconcile_items = envInt("K8S_ZIG_BENCH_RECONCILE_ITEMS", 500_000);

    std.debug.print("k8s-zig cache benchmark\n", .{});
    std.debug.print("queue_items={d} store_objects={d} store_rounds={d} reconcile_items={d}\n", .{
        queue_items,
        store_objects,
        store_rounds,
        reconcile_items,
    });

    try benchQueueBurst(allocator, io, queue_items);
    try benchStoreLookups(allocator, io, store_objects, store_rounds);

    for (&[_]usize{ 1, 2, 4, 8, 16 }) |workers| {
        try benchReconcilerThroughput(allocator, io, reconcile_items, workers);
    }
}

fn benchQueueBurst(allocator: Allocator, io: Io, item_count: usize) !void {
    var q = cache.WorkQueue(u32).init(allocator, io, .{});
    defer q.deinit();

    const start = nowNs(io);
    for (0..item_count) |i| {
        try q.add(@intCast(i));
    }
    for (0..item_count) |i| {
        const item = (try q.get()) orelse return error.UnexpectedNullItem;
        if (item != @as(u32, @intCast(i))) return error.UnexpectedQueueOrder;
        try q.done(item);
    }
    const elapsed_ns = nowNs(io) - start;

    printResult("queue-burst", item_count * 2, elapsed_ns, "items");
}

fn benchStoreLookups(allocator: Allocator, io: Io, object_count: usize, rounds: usize) !void {
    var store = cache.Store.init(allocator, io);
    defer store.deinit();

    const objects = try allocator.alloc(*Unstructured, object_count);
    defer {
        for (objects) |obj| {
            obj.deinit();
            allocator.destroy(obj);
        }
        allocator.free(objects);
    }

    for (objects, 0..) |*slot, i| {
        const obj = try allocator.create(Unstructured);
        obj.* = try Unstructured.init(allocator);
        try obj.setNamespace("default");

        var name_buf: [32]u8 = undefined;
        const name = try std.fmt.bufPrint(&name_buf, "cm-{d}", .{i});
        try obj.setName(name);

        var rv_buf: [32]u8 = undefined;
        const rv = try std.fmt.bufPrint(&rv_buf, "{d}", .{i + 1});
        try obj.setResourceVersion(rv);

        slot.* = obj;
        try store.add(obj);
    }

    const start = nowNs(io);
    var checksum: u64 = 0;
    for (0..rounds) |_| {
        for (0..object_count) |i| {
            var name_buf: [32]u8 = undefined;
            const name = try std.fmt.bufPrint(&name_buf, "cm-{d}", .{i});
            const obj = store.getByName("default", name) orelse return error.MissingStoreObject;
            checksum +%= @intCast(obj.getResourceVersion().?.len);
        }
    }
    const elapsed_ns = nowNs(io) - start;

    std.debug.print("store-lookups checksum={d}\n", .{checksum});
    printResult("store-lookups", object_count * rounds, elapsed_ns, "lookups");
}

fn benchReconcilerThroughput(allocator: Allocator, io: Io, item_count: usize, worker_count: usize) !void {
    var q = cache.WorkQueue(u32).init(allocator, io, .{});
    defer q.deinit();

    for (0..item_count) |i| {
        try q.add(@intCast(i));
    }

    var ctx = ReconcileBenchContext{};
    var reconciler = cache.Reconciler(u32).init(
        allocator,
        io,
        &q,
        @ptrCast(&ctx),
        reconcileNoop,
        .{
            .num_workers = worker_count,
            .max_retries = 0,
        },
    );

    const start = nowNs(io);
    var future = try io.concurrent(runReconciler, .{&reconciler});

    while (ctx.call_count.load(.acquire) < item_count) {
        try Io.sleep(io, Io.Duration.fromMilliseconds(1), .awake);
    }

    q.shutdown();
    try future.await(io);
    const elapsed_ns = nowNs(io) - start;

    var label_buf: [64]u8 = undefined;
    const label = try std.fmt.bufPrint(&label_buf, "reconciler-workers={d}", .{worker_count});
    printResult(label, item_count, elapsed_ns, "reconciles");
}

fn reconcileNoop(ptr: *anyopaque, _: u32) cache.Reconciler(u32).ReconcileResult {
    const ctx: *ReconcileBenchContext = @ptrCast(@alignCast(ptr));
    _ = ctx.call_count.fetchAdd(1, .acq_rel);
    return .ok;
}

fn runReconciler(reconciler: *cache.Reconciler(u32)) !void {
    try reconciler.start();
}

fn printResult(name: []const u8, operations: usize, elapsed_ns: i64, unit: []const u8) void {
    const elapsed_ms: f64 = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, std.time.ns_per_ms);
    const ops_per_sec: f64 = if (elapsed_ns > 0)
        @as(f64, @floatFromInt(operations)) * @as(f64, std.time.ns_per_s) / @as(f64, @floatFromInt(elapsed_ns))
    else
        0;
    std.debug.print("{s}: {d} {s} in {d:.2} ms ({d:.2} ops/s)\n", .{
        name,
        operations,
        unit,
        elapsed_ms,
        ops_per_sec,
    });
}

fn nowNs(io: Io) i64 {
    return @intCast(Io.Clock.Timestamp.now(io, .awake).raw.nanoseconds);
}

fn envInt(name: []const u8, default_value: usize) usize {
    var name_buf: [128:0]u8 = undefined;
    if (name.len >= name_buf.len) return default_value;
    @memcpy(name_buf[0..name.len], name);
    name_buf[name.len] = 0;

    const raw = std.c.getenv(&name_buf) orelse return default_value;
    const value = std.mem.sliceTo(raw, 0);
    return std.fmt.parseInt(usize, value, 10) catch default_value;
}
