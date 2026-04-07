const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const Allocator = mem.Allocator;
const scheme = @import("k8s_zig").scheme;

/// Build K8s API URLs from GVR, namespace, and name.
pub const UrlBuilder = struct {
    base: []const u8,

    /// Create a URL builder from a server base URL.
    /// The base should include scheme and host, e.g., "https://kubernetes.default.svc:6443".
    pub fn init(base: []const u8) UrlBuilder {
        return .{ .base = base };
    }

    /// Build the URL for a resource.
    /// core group (v1): /api/v1/[namespaces/{ns}/]{resource}[/{name}]
    /// named group:     /apis/{group}/{version}/[namespaces/{ns}/]{resource}[/{name}]
    pub fn resource(
        self: UrlBuilder,
        allocator: Allocator,
        gvr: scheme.GroupVersionResource,
        namespace: ?[]const u8,
        name: ?[]const u8,
    ) ![]const u8 {
        const api_prefix = if (gvr.group.len == 0) "api" else "apis";

        if (namespace) |ns| {
            if (name) |n| {
                return if (gvr.group.len == 0)
                    try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/namespaces/{s}/{s}/{s}", .{ self.base, api_prefix, gvr.version, ns, gvr.resource, n })
                else
                    try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/{s}/namespaces/{s}/{s}/{s}", .{ self.base, api_prefix, gvr.group, gvr.version, ns, gvr.resource, n });
            } else {
                return if (gvr.group.len == 0)
                    try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/namespaces/{s}/{s}", .{ self.base, api_prefix, gvr.version, ns, gvr.resource })
                else
                    try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/{s}/namespaces/{s}/{s}", .{ self.base, api_prefix, gvr.group, gvr.version, ns, gvr.resource });
            }
        } else {
            if (name) |n| {
                return if (gvr.group.len == 0)
                    try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/{s}/{s}", .{ self.base, api_prefix, gvr.version, gvr.resource, n })
                else
                    try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/{s}/{s}/{s}", .{ self.base, api_prefix, gvr.group, gvr.version, gvr.resource, n });
            } else {
                return if (gvr.group.len == 0)
                    try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/{s}", .{ self.base, api_prefix, gvr.version, gvr.resource })
                else
                    try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/{s}/{s}", .{ self.base, api_prefix, gvr.group, gvr.version, gvr.resource });
            }
        }
    }

    /// Build the URL for a resource's status subresource.
    pub fn status(
        self: UrlBuilder,
        allocator: Allocator,
        gvr: scheme.GroupVersionResource,
        namespace: ?[]const u8,
        name: []const u8,
    ) ![]const u8 {
        const base_url = try self.resource(allocator, gvr, namespace, name);
        defer allocator.free(base_url);
        return try std.fmt.allocPrint(allocator, "{s}/status", .{base_url});
    }

    /// Build the discovery URL for an API group version.
    /// /apis/{group}/{version} or /api/v1
    pub fn apiGroupVersion(
        self: UrlBuilder,
        allocator: Allocator,
        gv: scheme.GroupVersion,
    ) ![]const u8 {
        if (gv.group.len == 0) {
            return try std.fmt.allocPrint(allocator, "{s}/api/{s}", .{ self.base, gv.version });
        }
        return try std.fmt.allocPrint(allocator, "{s}/apis/{s}/{s}", .{ self.base, gv.group, gv.version });
    }

    /// Build the discovery URL for listing all API groups.
    pub fn apiGroups(self: UrlBuilder, allocator: Allocator) ![]const u8 {
        return try std.fmt.allocPrint(allocator, "{s}/apis", .{self.base});
    }

    /// Build the URL for watching a resource (adds ?watch=true).
    pub fn watch(
        self: UrlBuilder,
        allocator: Allocator,
        gvr: scheme.GroupVersionResource,
        namespace: ?[]const u8,
        rv: ?[]const u8,
    ) ![]const u8 {
        const base_url = try self.resource(allocator, gvr, namespace, null);
        defer allocator.free(base_url);
        if (rv) |r| {
            return try std.fmt.allocPrint(allocator, "{s}?watch=true&resourceVersion={s}", .{ base_url, r });
        }
        return try std.fmt.allocPrint(allocator, "{s}?watch=true", .{base_url});
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const test_base = "https://kubernetes.default.svc";
const ub = UrlBuilder.init(test_base);

test "resource: core group, namespaced, with name" {
    const cases = .{
        .{ scheme.GroupVersionResource{ .group = "", .version = "v1", .resource = "pods" }, "default", "nginx", test_base ++ "/api/v1/namespaces/default/pods/nginx" },
        .{ scheme.GroupVersionResource{ .group = "", .version = "v1", .resource = "configmaps" }, "kube-system", "coredns", test_base ++ "/api/v1/namespaces/kube-system/configmaps/coredns" },
    };
    inline for (cases) |c| {
        const url = try ub.resource(testing.allocator, c[0], c[1], c[2]);
        defer testing.allocator.free(url);
        try testing.expectEqualStrings(c[3], url);
    }
}

test "resource: named group, namespaced" {
    const cases = .{
        .{ scheme.GroupVersionResource{ .group = "apps", .version = "v1", .resource = "deployments" }, "default", "nginx", test_base ++ "/apis/apps/v1/namespaces/default/deployments/nginx" },
        .{ scheme.GroupVersionResource{ .group = "batch", .version = "v1", .resource = "jobs" }, "ns", null, test_base ++ "/apis/batch/v1/namespaces/ns/jobs" },
    };
    inline for (cases) |c| {
        const url = try ub.resource(testing.allocator, c[0], c[1], c[2]);
        defer testing.allocator.free(url);
        try testing.expectEqualStrings(c[3], url);
    }
}

test "resource: cluster-scoped" {
    // Namespaces resource: GET /api/v1/namespaces/kube-system (name, no ns)
    const ns_url = try ub.resource(testing.allocator, .{ .group = "", .version = "v1", .resource = "namespaces" }, null, "kube-system");
    defer testing.allocator.free(ns_url);
    try testing.expectEqualStrings(test_base ++ "/api/v1/namespaces/kube-system", ns_url);

    // Nodes: GET /api/v1/nodes (list, cluster-scoped)
    const nodes_url = try ub.resource(testing.allocator, .{ .group = "", .version = "v1", .resource = "nodes" }, null, null);
    defer testing.allocator.free(nodes_url);
    try testing.expectEqualStrings(test_base ++ "/api/v1/nodes", nodes_url);

    // ClusterRoles: GET /apis/rbac.authorization.k8s.io/v1/clusterroles
    const cr_url = try ub.resource(testing.allocator, .{ .group = "rbac.authorization.k8s.io", .version = "v1", .resource = "clusterroles" }, null, null);
    defer testing.allocator.free(cr_url);
    try testing.expectEqualStrings(test_base ++ "/apis/rbac.authorization.k8s.io/v1/clusterroles", cr_url);
}

test "status subresource" {
    const gvr = scheme.GroupVersionResource{ .group = "apps", .version = "v1", .resource = "deployments" };
    const url = try ub.status(testing.allocator, gvr, "default", "nginx");
    defer testing.allocator.free(url);
    try testing.expectEqualStrings(test_base ++ "/apis/apps/v1/namespaces/default/deployments/nginx/status", url);
}

test "discovery URLs" {
    const gv_url = try ub.apiGroupVersion(testing.allocator, .{ .group = "apps", .version = "v1" });
    defer testing.allocator.free(gv_url);
    try testing.expectEqualStrings(test_base ++ "/apis/apps/v1", gv_url);

    const core_url = try ub.apiGroupVersion(testing.allocator, .{ .group = "", .version = "v1" });
    defer testing.allocator.free(core_url);
    try testing.expectEqualStrings(test_base ++ "/api/v1", core_url);

    const groups_url = try ub.apiGroups(testing.allocator);
    defer testing.allocator.free(groups_url);
    try testing.expectEqualStrings(test_base ++ "/apis", groups_url);
}

test "watch URL" {
    const gvr = scheme.GroupVersionResource{ .group = "", .version = "v1", .resource = "pods" };

    const url1 = try ub.watch(testing.allocator, gvr, "default", null);
    defer testing.allocator.free(url1);
    try testing.expectEqualStrings(test_base ++ "/api/v1/namespaces/default/pods?watch=true", url1);

    const url2 = try ub.watch(testing.allocator, gvr, "default", "12345");
    defer testing.allocator.free(url2);
    try testing.expectEqualStrings(test_base ++ "/api/v1/namespaces/default/pods?watch=true&resourceVersion=12345", url2);
}
