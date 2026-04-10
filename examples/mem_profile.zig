const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const k8s = @import("k8s_zig");
const client = @import("k8s_client");
const cache = @import("k8s_cache");

const Config = client.Config;
const DynamicClient = client.DynamicClient;
const GroupVersionResource = k8s.GroupVersionResource;
const Unstructured = k8s.Unstructured;
const ObjectKey = cache.workqueue.ObjectKey;
const Store = cache.Store;

const pods_gvr = GroupVersionResource{ .group = "", .version = "v1", .resource = "pods" };
const configmaps_gvr = GroupVersionResource{ .group = "", .version = "v1", .resource = "configmaps" };

// ─────────────────────────────────────────────────────────────────────────────
// Counting allocator — wraps any allocator and tracks stats
// ─────────────────────────────────────────────────────────────────────────────

const CountingAllocator = struct {
    backing: Allocator,
    total_allocated: usize = 0,
    total_freed: usize = 0,
    current_live: usize = 0,
    peak_live: usize = 0,
    alloc_count: usize = 0,
    free_count: usize = 0,

    fn allocator(self: *CountingAllocator) Allocator {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    const vtable: Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const result = self.backing.vtable.alloc(self.backing.ptr, len, alignment, ret_addr);
        if (result != null) {
            self.total_allocated += len;
            self.current_live += len;
            self.alloc_count += 1;
            if (self.current_live > self.peak_live) {
                self.peak_live = self.current_live;
            }
        }
        return result;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const old_len = memory.len;
        const result = self.backing.vtable.resize(self.backing.ptr, memory, alignment, new_len, ret_addr);
        if (result) {
            if (new_len > old_len) {
                const delta = new_len - old_len;
                self.total_allocated += delta;
                self.current_live += delta;
            } else {
                const delta = old_len - new_len;
                self.total_freed += delta;
                self.current_live -= delta;
            }
            if (self.current_live > self.peak_live) {
                self.peak_live = self.current_live;
            }
        }
        return result;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const old_len = memory.len;
        const result = self.backing.vtable.remap(self.backing.ptr, memory, alignment, new_len, ret_addr);
        if (result != null) {
            if (new_len > old_len) {
                self.total_allocated += new_len - old_len;
                self.current_live += new_len - old_len;
            } else {
                self.total_freed += old_len - new_len;
                self.current_live -= old_len - new_len;
            }
            if (self.current_live > self.peak_live) {
                self.peak_live = self.current_live;
            }
        }
        return result;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.total_freed += memory.len;
        self.current_live -= memory.len;
        self.free_count += 1;
        self.backing.vtable.free(self.backing.ptr, memory, alignment, ret_addr);
    }

    fn report(self: *const CountingAllocator, label: []const u8) void {
        std.debug.print("\n=== Memory Profile: {s} ===\n", .{label});
        std.debug.print("  total allocated:  {d: >12} bytes ({d:.1} MB)\n", .{ self.total_allocated, @as(f64, @floatFromInt(self.total_allocated)) / 1048576.0 });
        std.debug.print("  total freed:      {d: >12} bytes ({d:.1} MB)\n", .{ self.total_freed, @as(f64, @floatFromInt(self.total_freed)) / 1048576.0 });
        std.debug.print("  peak live:        {d: >12} bytes ({d:.1} MB)\n", .{ self.peak_live, @as(f64, @floatFromInt(self.peak_live)) / 1048576.0 });
        std.debug.print("  current live:     {d: >12} bytes ({d:.1} MB)\n", .{ self.current_live, @as(f64, @floatFromInt(self.current_live)) / 1048576.0 });
        std.debug.print("  alloc count:      {d: >12}\n", .{self.alloc_count});
        std.debug.print("  free count:       {d: >12}\n", .{self.free_count});
        std.debug.print("  leaked allocs:    {d: >12}\n", .{self.alloc_count - self.free_count});
        std.debug.print("  bytes/alloc avg:  {d: >12}\n", .{if (self.alloc_count > 0) self.total_allocated / self.alloc_count else 0});
        std.debug.print("===\n\n", .{});
    }

    fn snapshot(self: *const CountingAllocator) Snapshot {
        return .{
            .total_allocated = self.total_allocated,
            .current_live = self.current_live,
            .alloc_count = self.alloc_count,
        };
    }

    const Snapshot = struct {
        total_allocated: usize,
        current_live: usize,
        alloc_count: usize,
    };

    fn reportDelta(self: *const CountingAllocator, before: Snapshot, label: []const u8) void {
        const alloc_delta = self.total_allocated - before.total_allocated;
        const live_delta = if (self.current_live >= before.current_live)
            self.current_live - before.current_live
        else
            0;
        const count_delta = self.alloc_count - before.alloc_count;
        std.debug.print("  {s: <30} alloc={d: >10} bytes  live_delta={d: >10} bytes  ops={d}\n", .{
            label, alloc_delta, live_delta, count_delta,
        });
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Timing helper
// ─────────────────────────────────────────────────────────────────────────────

fn nowMs(io: Io) i64 {
    return Io.Clock.Timestamp.now(io, .awake).raw.toMilliseconds();
}

// ─────────────────────────────────────────────────────────────────────────────
// Profiled operations
// ─────────────────────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa_instance: std.heap.DebugAllocator(.{}) = .{};
    defer _ = gpa_instance.deinit();

    var counter = CountingAllocator{ .backing = gpa_instance.allocator() };
    const allocator = counter.allocator();

    var io_instance: std.Io.Threaded = .init(gpa_instance.allocator(), .{});
    defer io_instance.deinit();
    const io = io_instance.io();

    var cfg = try loadConfig(io, allocator);
    defer cfg.deinit();

    const namespace = if (envOwned(allocator, "K8S_ZIG_NAMESPACE")) |ns| ns else try allocator.dupe(u8, cfg.namespace);
    defer allocator.free(namespace);

    std.debug.print("k8s-zig memory profiler\n", .{});
    std.debug.print("server: {s}\n", .{cfg.server});
    std.debug.print("namespace: {s}\n\n", .{namespace});

    // ── Profile 1: DynamicClient init ────────────────────────────────────
    var snap = counter.snapshot();
    var dc = try DynamicClient.init(allocator, io, &cfg);
    counter.reportDelta(snap, "DynamicClient.init");

    // ── Profile 2: List pods ─────────────────────────────────────────────
    const pods = dc.resource(pods_gvr, .{ .namespace = namespace });
    snap = counter.snapshot();
    var t0 = nowMs(io);
    var list_result = try pods.list(.{});
    var t1 = nowMs(io);
    counter.reportDelta(snap, "pods.list()");
    std.debug.print("  {s: <30} {d}ms\n", .{ "  list latency:", t1 - t0 });

    // Count pods
    const pod_count = list_result.field("items").len() orelse 0;
    std.debug.print("  {s: <30} {d}\n", .{ "  pod count:", pod_count });

    list_result.deinit();
    counter.reportDelta(snap, "after list_result.deinit()");

    // ── Profile 3: Store operations ──────────────────────────────────────
    std.debug.print("\n--- Store profiling ---\n", .{});
    var store = Store.init(allocator, io);
    defer store.deinit();

    try store.addIndexer("namespace", &cache.indexByNamespace);

    // List again and populate store
    snap = counter.snapshot();
    var list2 = try pods.list(.{});
    counter.reportDelta(snap, "pods.list() (second)");

    // Parse and add items to store
    snap = counter.snapshot();
    t0 = nowMs(io);
    var items_added: usize = 0;
    var items_it = list2.field("items").iter();
    while (items_it.next()) |item_nav| {
        const raw_value = item_nav.raw() orelse continue;
        const obj = try allocator.create(Unstructured);
        obj.* = try Unstructured.fromJsonValue(allocator, raw_value);
        store.add(obj) catch {
            obj.deinit();
            allocator.destroy(obj);
            continue;
        };
        items_added += 1;
    }
    t1 = nowMs(io);
    counter.reportDelta(snap, "parse + store.add (all)");
    std.debug.print("  {s: <30} {d}\n", .{ "  items added:", items_added });
    std.debug.print("  {s: <30} {d}ms\n", .{ "  parse+add latency:", t1 - t0 });
    if (items_added > 0) {
        const per_obj = (counter.total_allocated - snap.total_allocated) / items_added;
        std.debug.print("  {s: <30} {d} bytes\n", .{ "  bytes/object:", per_obj });
    }

    // ── Profile 4: Index queries ─────────────────────────────────────────
    std.debug.print("\n--- Index profiling ---\n", .{});
    snap = counter.snapshot();
    t0 = nowMs(io);
    const by_ns = try store.byIndex(allocator, "namespace", namespace);
    t1 = nowMs(io);
    allocator.free(by_ns);
    counter.reportDelta(snap, "byIndex(namespace)");
    std.debug.print("  {s: <30} {d} objects, {d}ms\n", .{ "  index result:", by_ns.len, t1 - t0 });

    snap = counter.snapshot();
    const idx_keys = try store.indexKeys(allocator, "namespace", namespace);
    allocator.free(idx_keys);
    counter.reportDelta(snap, "indexKeys(namespace)");

    snap = counter.snapshot();
    const idx_values = try store.listIndexFuncValues(allocator, "namespace");
    allocator.free(idx_values);
    counter.reportDelta(snap, "listIndexFuncValues");
    std.debug.print("  {s: <30} {d}\n", .{ "  distinct namespaces:", idx_values.len });

    // ── Profile 5: Store.replace() ───────────────────────────────────────
    std.debug.print("\n--- Replace profiling ---\n", .{});
    var list3 = try pods.list(.{});
    var replace_items: std.ArrayList(*Unstructured) = .empty;
    defer replace_items.deinit(allocator);

    var replace_it = list3.field("items").iter();
    while (replace_it.next()) |item_nav| {
        const raw_value = item_nav.raw() orelse continue;
        const obj = try allocator.create(Unstructured);
        obj.* = try Unstructured.fromJsonValue(allocator, raw_value);
        try replace_items.append(allocator, obj);
    }

    snap = counter.snapshot();
    t0 = nowMs(io);
    try store.replace(replace_items.items, "999");
    t1 = nowMs(io);
    counter.reportDelta(snap, "store.replace()");
    std.debug.print("  {s: <30} {d}ms for {d} objects\n", .{ "  replace latency:", t1 - t0, replace_items.items.len });

    // ── Profile 6: WorkQueue operations ──────────────────────────────────
    std.debug.print("\n--- WorkQueue profiling ---\n", .{});
    var queue = cache.WorkQueue(ObjectKey).init(allocator, io, .{});
    defer queue.deinit();

    snap = counter.snapshot();
    t0 = nowMs(io);
    const store_items = try store.list(allocator);
    defer allocator.free(store_items);
    for (store_items) |obj| {
        const key = ObjectKey.fromObject(obj) catch continue;
        queue.add(key) catch continue;
    }
    t1 = nowMs(io);
    counter.reportDelta(snap, "queue.add (all items)");
    std.debug.print("  {s: <30} {d}ms for {d} items\n", .{ "  enqueue latency:", t1 - t0, store_items.len });

    // Drain queue (shutdown first so get() doesn't block)
    queue.shutdown();
    snap = counter.snapshot();
    t0 = nowMs(io);
    var drained: usize = 0;
    while (true) {
        const item = (queue.get() catch break) orelse break;
        queue.done(item) catch {};
        drained += 1;
    }
    t1 = nowMs(io);
    counter.reportDelta(snap, "queue.get+done (all)");
    std.debug.print("  {s: <30} {d}ms for {d} items\n", .{ "  drain latency:", t1 - t0, drained });

    // ── Cleanup ──────────────────────────────────────────────────────────
    list2.deinit();
    list3.deinit();
    dc.deinit();

    // ── Final report ─────────────────────────────────────────────────────
    counter.report("TOTAL SESSION");
}

fn loadConfig(io: Io, allocator: Allocator) !Config {
    if (envOwned(allocator, "K8S_ZIG_SERVER")) |server| {
        errdefer allocator.free(server);
        const ns = if (envOwned(allocator, "K8S_ZIG_NAMESPACE")) |n| n else try allocator.dupe(u8, "default");
        var cfg = Config{
            .allocator = allocator,
            .server = server,
            .namespace = ns,
        };
        if (envOwned(allocator, "K8S_ZIG_TOKEN")) |tok| cfg.token = tok;
        const insecure_val = envOwned(allocator, "K8S_ZIG_INSECURE");
        cfg.insecure = insecure_val != null;
        if (insecure_val) |v| allocator.free(v);
        return cfg;
    }

    const kubeconfig_path = if (envOwned(allocator, "KUBECONFIG")) |path|
        path
    else
        try defaultKubeconfigPath(allocator);
    defer allocator.free(kubeconfig_path);

    const bytes = try readFileAlloc(io, allocator, kubeconfig_path);
    defer allocator.free(bytes);

    var kc = client.kubeconfig.fromYaml(allocator, bytes) catch try client.kubeconfig.fromJson(allocator, bytes);
    defer kc.deinit();

    return try Config.fromKubeconfig(allocator, &kc);
}

fn defaultKubeconfigPath(allocator: Allocator) ![]const u8 {
    const home = envOwned(allocator, "HOME") orelse return error.MissingHome;
    defer allocator.free(home);
    return try std.fs.path.join(allocator, &.{ home, ".kube", "config" });
}

fn readFileAlloc(io: Io, allocator: Allocator, path: []const u8) ![]u8 {
    return try std.Io.Dir.readFileAlloc(.cwd(), io, path, allocator, .limited(4 * 1024 * 1024));
}

fn envOwned(allocator: Allocator, name: []const u8) ?[]const u8 {
    const name_z = allocator.dupeZ(u8, name) catch return null;
    defer allocator.free(name_z);
    const value_z = std.c.getenv(name_z.ptr) orelse return null;
    return allocator.dupe(u8, std.mem.span(value_z)) catch null;
}
