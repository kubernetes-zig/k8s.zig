const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const k8s = @import("k8s_zig");
const client = @import("k8s_client");

const Config = client.Config;
const DynamicClient = client.DynamicClient;
const GroupVersionResource = k8s.GroupVersionResource;
const Unstructured = k8s.Unstructured;

const pods_gvr = GroupVersionResource{
    .group = "",
    .version = "v1",
    .resource = "pods",
};

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var io_instance: std.Io.Threaded = .init(allocator, .{});
    defer io_instance.deinit();
    const io = io_instance.io();

    // Load kubeconfig
    var cfg = try loadConfig(io, allocator);
    defer cfg.deinit();

    const namespace = if (envOwned(allocator, "K8S_ZIG_NAMESPACE")) |ns| ns else try allocator.dupe(u8, cfg.namespace);
    defer allocator.free(namespace);

    std.debug.print("connecting to {s}\n", .{cfg.server});
    std.debug.print("listing pods in namespace: {s}\n\n", .{namespace});

    // Create client
    var dc = try DynamicClient.init(allocator, io, &cfg);
    defer dc.deinit();

    // List pods
    const pods = dc.resource(pods_gvr, .{ .namespace = namespace });
    var list_result = try pods.list(.{});
    defer list_result.deinit();

    // Extract items
    const items = list_result.field("items");
    const count = items.len() orelse 0;
    std.debug.print("found {d} pod(s)\n\n", .{count});

    var it = items.iter();
    var idx: usize = 0;
    while (it.next()) |pod| {
        const name = pod.field("metadata").field("name").str() orelse "<unknown>";
        const ns = pod.field("metadata").field("namespace").str() orelse "<unknown>";
        const phase = pod.field("status").field("phase").str() orelse "<unknown>";
        const node = pod.field("spec").field("nodeName").str() orelse "<unscheduled>";

        // Container count
        const containers = pod.field("spec").field("containers");
        const container_count = containers.len() orelse 0;

        // Ready container count
        const statuses = pod.field("status").field("containerStatuses");
        var ready_count: usize = 0;
        if (statuses.len()) |status_count| {
            var status_it = statuses.iter();
            while (status_it.next()) |status| {
                if (status.field("ready").boolean() orelse false) {
                    ready_count += 1;
                }
            }
            _ = status_count;
        }

        std.debug.print("{d: >3}. {s}/{s}  phase={s}  ready={d}/{d}  node={s}\n", .{
            idx + 1,
            ns,
            name,
            phase,
            ready_count,
            container_count,
            node,
        });
        idx += 1;
    }

    if (count == 0) {
        std.debug.print("(no pods found)\n", .{});
    }
    std.debug.print("\ndone.\n", .{});
}

fn loadConfig(io: Io, allocator: Allocator) !Config {
    // Try explicit server URL first
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

    // Fall back to kubeconfig
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
