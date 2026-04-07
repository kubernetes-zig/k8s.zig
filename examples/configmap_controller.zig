const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const k8s = @import("k8s_zig");
const client = @import("k8s_client");
const cache = @import("k8s_cache");

const Config = client.Config;
const DynamicClient = client.DynamicClient;
const GroupVersionResource = k8s.GroupVersionResource;
const ObjectKey = cache.workqueue.ObjectKey;

const configmaps_gvr = GroupVersionResource{
    .group = "",
    .version = "v1",
    .resource = "configmaps",
};

const ControllerState = struct {
    allocator: Allocator,
    client: *DynamicClient,
    queue: *cache.WorkQueue(ObjectKey),
    default_namespace: []const u8,
};

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var io_instance: std.Io.Threaded = .init(allocator, .{});
    defer io_instance.deinit();
    const io = io_instance.io();

    var cfg = try loadConfig(io, allocator);
    defer cfg.deinit();

    var dynamic_client = try DynamicClient.init(allocator, io, &cfg);
    defer dynamic_client.deinit();

    var queue = cache.WorkQueue(ObjectKey).init(allocator, io, .{});
    defer queue.deinit();

    const ErrorPrinter = struct {
        fn handler(_: *anyopaque, err: anyerror, ctx: cache.ErrorContext, consecutive: u32) void {
            std.debug.print("reflector error: {s} ctx={s} consecutive={d}\n", .{ @errorName(err), @tagName(ctx), consecutive });
        }
    };
    const informer = try cache.Informer.createDynamic(allocator, io, &dynamic_client, configmaps_gvr, .{
        .namespace = cfg.namespace,
        .error_handler = .{ .ptr = @ptrFromInt(1), .on_error_fn = ErrorPrinter.handler },
    });
    defer informer.deinit();

    var state = ControllerState{
        .allocator = allocator,
        .client = &dynamic_client,
        .queue = &queue,
        .default_namespace = cfg.namespace,
    };

    try informer.addEventHandler(.{
        .ptr = @ptrCast(&state),
        .on_add_fn = enqueueOnAdd,
        .on_update_fn = enqueueOnUpdate,
        .on_delete_fn = enqueueOnDelete,
    });

    try informer.start();
    if (!informer.waitForSync(10_000)) return error.CacheSyncTimeout;

    var reconciler = cache.Reconciler(ObjectKey).init(
        allocator,
        io,
        &queue,
        @ptrCast(&state),
        reconcile,
        .{
            .num_workers = envInt("K8S_ZIG_WORKERS", 2),
            .max_retries = 10,
        },
    );

    var reconciler_future = try io.concurrent(runReconciler, .{&reconciler});
    defer {
        queue.shutdown();
        _ = reconciler_future.await(io) catch {};
    }

    const run_seconds = envInt("K8S_ZIG_RUN_SECONDS", 30);
    std.debug.print(
        "configmap controller running for {d}s against {s} namespace {s}\n",
        .{ run_seconds, cfg.server, cfg.namespace },
    );

    Io.sleep(io, Io.Duration.fromSeconds(@intCast(run_seconds)), .awake) catch {};

    queue.shutdown();
    informer.stop();
    std.process.exit(0);
}

fn enqueueOnAdd(ptr: *anyopaque, obj: *const k8s.Unstructured, _: bool) anyerror!void {
    const state: *ControllerState = @ptrCast(@alignCast(ptr));
    try state.queue.add(try ObjectKey.fromObject(obj));
}

fn enqueueOnUpdate(ptr: *anyopaque, _: *const k8s.Unstructured, new_obj: *const k8s.Unstructured) anyerror!void {
    const state: *ControllerState = @ptrCast(@alignCast(ptr));
    try state.queue.add(try ObjectKey.fromObject(new_obj));
}

fn enqueueOnDelete(ptr: *anyopaque, obj: *const k8s.Unstructured) anyerror!void {
    const state: *ControllerState = @ptrCast(@alignCast(ptr));
    try state.queue.add(try ObjectKey.fromObject(obj));
}

fn reconcile(ptr: *anyopaque, key: ObjectKey) cache.Reconciler(ObjectKey).ReconcileResult {
    const state: *ControllerState = @ptrCast(@alignCast(ptr));
    return reconcileConfigMap(state, key);
}

fn reconcileConfigMap(state: *ControllerState, key: ObjectKey) cache.Reconciler(ObjectKey).ReconcileResult {
    const namespace = key.namespace() orelse state.default_namespace;
    const name = key.name();
    const resource = state.client.resource(configmaps_gvr, .{ .namespace = namespace });

    var obj = resource.getOrNull(name) catch |err| return .{ .err = err };
    if (obj == null) {
        std.debug.print("configmap {s}/{s} no longer exists\n", .{ namespace, name });
        return .ok;
    }
    defer obj.?.deinit();

    const rv = obj.?.getResourceVersion() orelse return .ok;
    const current = obj.?.field("metadata").field("annotations").field("k8s-zig.dev/observed-resource-version").str();
    if (current) |value| {
        if (std.mem.eql(u8, value, rv)) {
            return .ok;
        }
    }

    const patch = std.mem.concat(
        state.allocator,
        u8,
        &.{
            "{\"metadata\":{\"annotations\":{\"k8s-zig.dev/observed-resource-version\":\"",
            rv,
            "\"}}}",
        },
    ) catch |err| return .{ .err = err };
    defer state.allocator.free(patch);

    _ = resource.patch(name, patch, .{}) catch |err| return .{ .err = err };
    std.debug.print("annotated configmap {s}/{s} with rv {s}\n", .{ namespace, name, rv });
    return .ok;
}

fn runReconciler(reconciler: *cache.Reconciler(ObjectKey)) !void {
    try reconciler.start();
}

fn loadConfig(io: Io, allocator: Allocator) !Config {
    if (envOwned(allocator, "K8S_ZIG_SERVER")) |server| {
        errdefer allocator.free(server);
        const namespace = if (envOwned(allocator, "K8S_ZIG_NAMESPACE")) |ns| ns else try allocator.dupe(u8, "default");
        var cfg = Config{
            .allocator = allocator,
            .server = server,
            .namespace = namespace,
        };
        if (envOwned(allocator, "K8S_ZIG_TOKEN")) |tok| {
            cfg.token = tok;
        }
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

    var cfg = try Config.fromKubeconfig(allocator, &kc);
    if (envOwned(allocator, "K8S_ZIG_NAMESPACE")) |ns| {
        allocator.free(cfg.namespace);
        cfg.namespace = ns;
    }
    return cfg;
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

fn envInt(name: []const u8, default_value: u64) u64 {
    const value = envOwned(std.heap.page_allocator, name) orelse return default_value;
    defer std.heap.page_allocator.free(value);
    return std.fmt.parseInt(u64, value, 10) catch default_value;
}
