const std = @import("std");
const mem = std.mem;
const json = std.json;
const Allocator = mem.Allocator;
const testing = std.testing;

const k8s = @import("k8s_zig");
const scheme = k8s.scheme;

const k8s_api = @import("k8s_api");
const apidiscovery = k8s_api.apidiscovery_v2;

const transport_mod = @import("transport.zig");
const Transport = transport_mod.Transport;
const url_mod = @import("url.zig");
const UrlBuilder = url_mod.UrlBuilder;

/// Accept header for aggregated discovery v2.
const accept_v2 = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList";

/// DiscoveryClient provides access to the Kubernetes API server's discovery endpoints.
/// Fetches API groups, resources, and server version using aggregated discovery (v2).
pub const DiscoveryClient = struct {
    allocator: Allocator,
    transport: *Transport,
    url_builder: UrlBuilder,

    pub fn init(allocator: Allocator, transport: *Transport, url_builder: UrlBuilder) DiscoveryClient {
        return .{
            .allocator = allocator,
            .transport = transport,
            .url_builder = url_builder,
        };
    }

    /// Fetch all API groups and their resources via aggregated discovery.
    /// Two requests: GET /api and GET /apis with v2 Accept header.
    /// Caller owns the returned ServerGroupsAndResources.
    pub fn serverGroupsAndResources(self: *DiscoveryClient) !ServerGroupsAndResources {
        var result = ServerGroupsAndResources{
            .arena = std.heap.ArenaAllocator.init(self.allocator),
        };
        errdefer result.deinit();

        const arena = result.arena.allocator();

        // Fetch core group (/api) and named groups (/apis) in sequence.
        const core = try self.fetchDiscovery(arena, "/api");
        const apis = try self.fetchDiscovery(arena, "/apis");

        // Merge: core groups first, then named groups.
        const core_len = core.items.items.len;
        const apis_len = apis.items.items.len;
        const total = core_len + apis_len;

        const groups = try arena.alloc(apidiscovery.APIGroupDiscovery, total);
        @memcpy(groups[0..core_len], core.items.items);
        @memcpy(groups[core_len..total], apis.items.items);
        result.groups = groups;

        return result;
    }

    /// Fetch resources for a single group version from the aggregated data.
    /// Filters the full discovery response for the matching GV.
    pub fn serverResourcesForGroupVersion(
        self: *DiscoveryClient,
        gv: scheme.GroupVersion,
    ) !ServerResourcesForGroupVersion {
        var all = try self.serverGroupsAndResources();
        defer all.deinit();

        for (all.groups) |group| {
            const group_name = groupNameFromDiscovery(&group);
            if (!mem.eql(u8, group_name, gv.group)) continue;

            for (group.versions.items) |ver| {
                const ver_name = ver.version orelse continue;
                if (!mem.eql(u8, ver_name, gv.version)) continue;

                // Found matching GV — copy resources into caller-owned memory.
                const resources = try self.allocator.alloc(
                    apidiscovery.APIResourceDiscovery,
                    ver.resources.items.len,
                );
                @memcpy(resources, ver.resources.items);
                return .{
                    .group = gv.group,
                    .version = gv.version,
                    .resources = resources,
                    .allocator = self.allocator,
                };
            }
        }
        return error.GroupVersionNotFound;
    }

    /// Fetch only preferred-version resources across all groups.
    /// The first version in each group's versions list is the preferred version.
    pub fn serverPreferredResources(self: *DiscoveryClient) !ServerGroupsAndResources {
        var all = try self.serverGroupsAndResources();
        errdefer all.deinit();

        const arena = all.arena.allocator();
        var preferred: std.ArrayListUnmanaged(apidiscovery.APIGroupDiscovery) = .empty;

        for (all.groups) |group| {
            if (group.versions.items.len == 0) continue;
            // First version is preferred (sorted descending by preference upstream).
            var trimmed = group;
            const first_ver = group.versions.items[0];
            trimmed.versions = .empty;
            try trimmed.versions.append(arena, first_ver);
            try preferred.append(arena, trimmed);
        }

        all.groups = try preferred.toOwnedSlice(arena);
        return all;
    }

    /// Like serverPreferredResources but only namespaced resources.
    pub fn serverPreferredNamespacedResources(self: *DiscoveryClient) !ServerGroupsAndResources {
        var all = try self.serverPreferredResources();
        errdefer all.deinit();

        const arena = all.arena.allocator();
        var filtered: std.ArrayListUnmanaged(apidiscovery.APIGroupDiscovery) = .empty;

        for (all.groups) |group| {
            var filtered_group = group;
            for (group.versions.items) |ver| {
                var filtered_ver = ver;
                var ns_resources: std.ArrayListUnmanaged(apidiscovery.APIResourceDiscovery) = .empty;
                for (ver.resources.items) |res| {
                    const scope = res.scope orelse continue;
                    if (mem.eql(u8, scope, "Namespaced")) {
                        try ns_resources.append(arena, res);
                    }
                }
                filtered_ver.resources = .empty;
                for (ns_resources.items) |r| {
                    try filtered_ver.resources.append(arena, r);
                }
                filtered_group.versions = .empty;
                try filtered_group.versions.append(arena, filtered_ver);
            }
            if (filtered_group.versions.items.len > 0 and
                filtered_group.versions.items[0].resources.items.len > 0)
            {
                try filtered.append(arena, filtered_group);
            }
        }

        all.groups = try filtered.toOwnedSlice(arena);
        return all;
    }

    /// GET /version — fetch server version info.
    pub fn serverVersion(self: *DiscoveryClient) !ServerVersion {
        const url = try std.fmt.allocPrint(self.allocator, "{s}/version", .{self.url_builder.base});
        defer self.allocator.free(url);

        var resp = try self.transport.request(.GET, url, null, null);
        defer resp.deinit();

        if (resp.status >= 400) {
            return error.ServerVersionFailed;
        }

        const parsed = try json.parseFromSlice(ServerVersionJson, self.allocator, resp.body, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();
        const v = parsed.value;

        return .{
            .major = try self.allocator.dupe(u8, v.major orelse ""),
            .minor = try self.allocator.dupe(u8, v.minor orelse ""),
            .git_version = try self.allocator.dupe(u8, v.gitVersion orelse ""),
            .git_commit = try self.allocator.dupe(u8, v.gitCommit orelse ""),
            .platform = try self.allocator.dupe(u8, v.platform orelse ""),
            .allocator = self.allocator,
        };
    }

    /// Check if a specific GVR is served by the cluster.
    pub fn isResourceEnabled(self: *DiscoveryClient, gvr: scheme.GroupVersionResource) !bool {
        var all = try self.serverGroupsAndResources();
        defer all.deinit();
        return findResource(all.groups, gvr);
    }

    /// Check if the server supports a specific GroupVersion.
    pub fn serverSupportsVersion(self: *DiscoveryClient, gv: scheme.GroupVersion) !bool {
        var all = try self.serverGroupsAndResources();
        defer all.deinit();
        return findGroupVersion(all.groups, gv);
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    fn fetchDiscovery(
        self: *DiscoveryClient,
        arena: Allocator,
        path: []const u8,
    ) !apidiscovery.APIGroupDiscoveryList {
        const url = try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.url_builder.base, path });
        defer self.allocator.free(url);

        var resp = try self.transport.request(.GET, url, null, accept_v2);
        defer resp.deinit();

        if (resp.status >= 400) {
            return error.DiscoveryFailed;
        }

        const parsed = try json.parseFromSlice(
            apidiscovery.APIGroupDiscoveryList,
            arena,
            resp.body,
            .{ .ignore_unknown_fields = true },
        );
        return parsed.value;
    }
};

/// Extracted group name from discovery metadata.
/// Core group has empty name or null metadata.
pub fn groupNameFromDiscovery(group: *const apidiscovery.APIGroupDiscovery) []const u8 {
    if (group.metadata) |meta| {
        if (meta.name) |name| return name;
    }
    return "";
}

/// Check if a GVR exists in a set of discovered groups.
pub fn findResource(groups: []const apidiscovery.APIGroupDiscovery, gvr: scheme.GroupVersionResource) bool {
    for (groups) |group| {
        const group_name = groupNameFromDiscovery(&group);
        if (!mem.eql(u8, group_name, gvr.group)) continue;
        for (group.versions.items) |ver| {
            const ver_name = ver.version orelse continue;
            if (!mem.eql(u8, ver_name, gvr.version)) continue;
            for (ver.resources.items) |res| {
                const res_name = res.resource orelse continue;
                if (mem.eql(u8, res_name, gvr.resource)) return true;
            }
        }
    }
    return false;
}

/// Check if a GroupVersion exists in a set of discovered groups.
pub fn findGroupVersion(groups: []const apidiscovery.APIGroupDiscovery, gv: scheme.GroupVersion) bool {
    for (groups) |group| {
        const group_name = groupNameFromDiscovery(&group);
        if (!mem.eql(u8, group_name, gv.group)) continue;
        for (group.versions.items) |ver| {
            const ver_name = ver.version orelse continue;
            if (mem.eql(u8, ver_name, gv.version)) return true;
        }
    }
    return false;
}

/// Filter resources that support all specified verbs.
pub fn filterByVerbs(
    allocator: Allocator,
    resources: []const apidiscovery.APIResourceDiscovery,
    verbs: []const []const u8,
) ![]const apidiscovery.APIResourceDiscovery {
    var result: std.ArrayListUnmanaged(apidiscovery.APIResourceDiscovery) = .empty;
    for (resources) |res| {
        if (supportsAllVerbs(&res, verbs)) {
            try result.append(allocator, res);
        }
    }
    return try result.toOwnedSlice(allocator);
}

fn supportsAllVerbs(res: *const apidiscovery.APIResourceDiscovery, required: []const []const u8) bool {
    for (required) |verb| {
        var found = false;
        for (res.verbs.items) |v| {
            if (mem.eql(u8, v, verb)) {
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}

// ── Result types ─────────────────────────────────────────────────────────────

pub const ServerGroupsAndResources = struct {
    groups: []const apidiscovery.APIGroupDiscovery = &.{},
    arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *ServerGroupsAndResources) void {
        self.arena.deinit();
    }
};

pub const ServerResourcesForGroupVersion = struct {
    group: []const u8,
    version: []const u8,
    resources: []const apidiscovery.APIResourceDiscovery,
    allocator: Allocator,

    pub fn deinit(self: *ServerResourcesForGroupVersion) void {
        self.allocator.free(self.resources);
    }
};

/// JSON shape of /version response.
const ServerVersionJson = struct {
    major: ?[]const u8 = null,
    minor: ?[]const u8 = null,
    gitVersion: ?[]const u8 = null,
    gitCommit: ?[]const u8 = null,
    platform: ?[]const u8 = null,
};

pub const ServerVersion = struct {
    major: []const u8,
    minor: []const u8,
    git_version: []const u8,
    git_commit: []const u8,
    platform: []const u8,
    allocator: Allocator,

    pub fn deinit(self: *ServerVersion) void {
        self.allocator.free(self.major);
        self.allocator.free(self.minor);
        self.allocator.free(self.git_version);
        self.allocator.free(self.git_commit);
        self.allocator.free(self.platform);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

// Test helpers: build discovery data without an API server.

const TestGroups = struct {
    groups: []apidiscovery.APIGroupDiscovery,
    arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *TestGroups) void {
        self.arena.deinit();
    }
};

fn makeTestGroups(backing_allocator: Allocator) !TestGroups {
    var arena = std.heap.ArenaAllocator.init(backing_allocator);
    errdefer arena.deinit();
    const allocator = arena.allocator();
    var groups: std.ArrayListUnmanaged(apidiscovery.APIGroupDiscovery) = .empty;

    // Core group (v1)
    {
        var core_meta = @import("k8s_api").ObjectMeta{};
        core_meta.name = "";

        var pods_res = apidiscovery.APIResourceDiscovery{};
        pods_res.resource = "pods";
        pods_res.singularResource = "pod";
        pods_res.scope = "Namespaced";
        pods_res.verbs = .empty;
        for ([_][]const u8{ "get", "list", "watch", "create", "update", "patch", "delete" }) |v| {
            try pods_res.verbs.append(allocator, v);
        }

        var nodes_res = apidiscovery.APIResourceDiscovery{};
        nodes_res.resource = "nodes";
        nodes_res.singularResource = "node";
        nodes_res.scope = "Cluster";
        nodes_res.verbs = .empty;
        for ([_][]const u8{ "get", "list", "watch", "create", "update", "patch", "delete" }) |v| {
            try nodes_res.verbs.append(allocator, v);
        }

        var namespaces_res = apidiscovery.APIResourceDiscovery{};
        namespaces_res.resource = "namespaces";
        namespaces_res.singularResource = "namespace";
        namespaces_res.scope = "Cluster";
        namespaces_res.verbs = .empty;
        for ([_][]const u8{ "get", "list", "watch", "create", "update", "patch", "delete" }) |v| {
            try namespaces_res.verbs.append(allocator, v);
        }

        var configmaps_res = apidiscovery.APIResourceDiscovery{};
        configmaps_res.resource = "configmaps";
        configmaps_res.singularResource = "configmap";
        configmaps_res.scope = "Namespaced";
        configmaps_res.verbs = .empty;
        for ([_][]const u8{ "get", "list", "watch", "create", "update", "patch", "delete" }) |v| {
            try configmaps_res.verbs.append(allocator, v);
        }

        var v1_ver = apidiscovery.APIVersionDiscovery{};
        v1_ver.version = "v1";
        v1_ver.freshness = "Current";
        v1_ver.resources = .empty;
        try v1_ver.resources.append(allocator, pods_res);
        try v1_ver.resources.append(allocator, nodes_res);
        try v1_ver.resources.append(allocator, namespaces_res);
        try v1_ver.resources.append(allocator, configmaps_res);

        var core_group = apidiscovery.APIGroupDiscovery{};
        core_group.metadata = core_meta;
        try core_group.versions.append(allocator, v1_ver);
        try groups.append(allocator, core_group);
    }

    // apps group
    {
        var apps_meta = @import("k8s_api").ObjectMeta{};
        apps_meta.name = "apps";

        var deploy_res = apidiscovery.APIResourceDiscovery{};
        deploy_res.resource = "deployments";
        deploy_res.singularResource = "deployment";
        deploy_res.scope = "Namespaced";
        deploy_res.verbs = .empty;
        for ([_][]const u8{ "get", "list", "watch", "create", "update", "patch", "delete" }) |v| {
            try deploy_res.verbs.append(allocator, v);
        }

        var statefulsets_res = apidiscovery.APIResourceDiscovery{};
        statefulsets_res.resource = "statefulsets";
        statefulsets_res.singularResource = "statefulset";
        statefulsets_res.scope = "Namespaced";
        statefulsets_res.verbs = .empty;
        for ([_][]const u8{ "get", "list", "watch", "create", "update", "patch", "delete" }) |v| {
            try statefulsets_res.verbs.append(allocator, v);
        }

        var v1_ver = apidiscovery.APIVersionDiscovery{};
        v1_ver.version = "v1";
        v1_ver.freshness = "Current";
        v1_ver.resources = .empty;
        try v1_ver.resources.append(allocator, deploy_res);
        try v1_ver.resources.append(allocator, statefulsets_res);

        var apps_group = apidiscovery.APIGroupDiscovery{};
        apps_group.metadata = apps_meta;
        try apps_group.versions.append(allocator, v1_ver);
        try groups.append(allocator, apps_group);
    }

    // rbac.authorization.k8s.io group
    {
        var rbac_meta = @import("k8s_api").ObjectMeta{};
        rbac_meta.name = "rbac.authorization.k8s.io";

        var clusterroles_res = apidiscovery.APIResourceDiscovery{};
        clusterroles_res.resource = "clusterroles";
        clusterroles_res.singularResource = "clusterrole";
        clusterroles_res.scope = "Cluster";
        clusterroles_res.verbs = .empty;
        for ([_][]const u8{ "get", "list", "watch", "create", "update", "patch", "delete" }) |v| {
            try clusterroles_res.verbs.append(allocator, v);
        }

        var roles_res = apidiscovery.APIResourceDiscovery{};
        roles_res.resource = "roles";
        roles_res.singularResource = "role";
        roles_res.scope = "Namespaced";
        roles_res.verbs = .empty;
        for ([_][]const u8{ "get", "list", "watch", "create", "update", "patch", "delete" }) |v| {
            try roles_res.verbs.append(allocator, v);
        }

        var v1_ver = apidiscovery.APIVersionDiscovery{};
        v1_ver.version = "v1";
        v1_ver.freshness = "Current";
        v1_ver.resources = .empty;
        try v1_ver.resources.append(allocator, clusterroles_res);
        try v1_ver.resources.append(allocator, roles_res);

        var rbac_group = apidiscovery.APIGroupDiscovery{};
        rbac_group.metadata = rbac_meta;
        try rbac_group.versions.append(allocator, v1_ver);
        try groups.append(allocator, rbac_group);
    }

    return .{
        .groups = try groups.toOwnedSlice(allocator),
        .arena = arena,
    };
}

test "groupNameFromDiscovery: core group" {
    var group = apidiscovery.APIGroupDiscovery{};
    try testing.expectEqualStrings("", groupNameFromDiscovery(&group));

    var meta = @import("k8s_api").ObjectMeta{};
    meta.name = "";
    group.metadata = meta;
    try testing.expectEqualStrings("", groupNameFromDiscovery(&group));
}

test "groupNameFromDiscovery: named group" {
    var meta = @import("k8s_api").ObjectMeta{};
    meta.name = "apps";
    var group = apidiscovery.APIGroupDiscovery{};
    group.metadata = meta;
    try testing.expectEqualStrings("apps", groupNameFromDiscovery(&group));
}

test "findResource: existing resources" {
    var tg = try makeTestGroups(testing.allocator);
    defer tg.deinit();
    const groups = tg.groups;

    const Case = struct { gvr: scheme.GroupVersionResource, expected: bool };
    const cases = [_]Case{
        .{ .gvr = .{ .group = "", .version = "v1", .resource = "pods" }, .expected = true },
        .{ .gvr = .{ .group = "", .version = "v1", .resource = "nodes" }, .expected = true },
        .{ .gvr = .{ .group = "apps", .version = "v1", .resource = "deployments" }, .expected = true },
        .{ .gvr = .{ .group = "apps", .version = "v1", .resource = "statefulsets" }, .expected = true },
        .{ .gvr = .{ .group = "rbac.authorization.k8s.io", .version = "v1", .resource = "clusterroles" }, .expected = true },
        .{ .gvr = .{ .group = "", .version = "v1", .resource = "nonexistent" }, .expected = false },
        .{ .gvr = .{ .group = "fake", .version = "v1", .resource = "pods" }, .expected = false },
        .{ .gvr = .{ .group = "", .version = "v2", .resource = "pods" }, .expected = false },
        .{ .gvr = .{ .group = "apps", .version = "v1beta1", .resource = "deployments" }, .expected = false },
    };
    for (cases) |c| {
        try testing.expectEqual(c.expected, findResource(groups, c.gvr));
    }
}

test "findGroupVersion: existing group versions" {
    var tg = try makeTestGroups(testing.allocator);
    defer tg.deinit();
    const groups = tg.groups;

    const Case = struct { gv: scheme.GroupVersion, expected: bool };
    const cases = [_]Case{
        .{ .gv = .{ .group = "", .version = "v1" }, .expected = true },
        .{ .gv = .{ .group = "apps", .version = "v1" }, .expected = true },
        .{ .gv = .{ .group = "rbac.authorization.k8s.io", .version = "v1" }, .expected = true },
        .{ .gv = .{ .group = "", .version = "v2" }, .expected = false },
        .{ .gv = .{ .group = "nonexistent", .version = "v1" }, .expected = false },
    };
    for (cases) |c| {
        try testing.expectEqual(c.expected, findGroupVersion(groups, c.gv));
    }
}

test "filterByVerbs: match and filter" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var resources_list: [3]apidiscovery.APIResourceDiscovery = undefined;

    // Resource with full CRUD
    resources_list[0] = .{};
    resources_list[0].resource = "pods";
    resources_list[0].verbs = .empty;
    for ([_][]const u8{ "get", "list", "watch", "create", "delete" }) |v| {
        try resources_list[0].verbs.append(alloc, v);
    }

    // Resource with read-only
    resources_list[1] = .{};
    resources_list[1].resource = "events";
    resources_list[1].verbs = .empty;
    for ([_][]const u8{ "get", "list", "watch" }) |v| {
        try resources_list[1].verbs.append(alloc, v);
    }

    // Resource with get only
    resources_list[2] = .{};
    resources_list[2].resource = "bindings";
    resources_list[2].verbs = .empty;
    try resources_list[2].verbs.append(alloc, "create");

    const all_verbs = &[_][]const u8{ "get", "list", "watch" };
    const filtered = try filterByVerbs(alloc, &resources_list, all_verbs);

    try testing.expectEqual(@as(usize, 2), filtered.len);
    try testing.expectEqualStrings("pods", filtered[0].resource.?);
    try testing.expectEqualStrings("events", filtered[1].resource.?);

    const create_verbs = &[_][]const u8{"create"};
    const create_only = try filterByVerbs(alloc, &resources_list, create_verbs);
    try testing.expectEqual(@as(usize, 2), create_only.len);
}

test "ServerVersion JSON parsing" {
    const version_json =
        \\{"major":"1","minor":"30","gitVersion":"v1.30.0","gitCommit":"abc123","gitTreeState":"clean","buildDate":"2024-01-01T00:00:00Z","goVersion":"go1.22.2","compiler":"gc","platform":"linux/amd64"}
    ;

    const parsed = try json.parseFromSlice(ServerVersionJson, testing.allocator, version_json, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    try testing.expectEqualStrings("1", parsed.value.major.?);
    try testing.expectEqualStrings("30", parsed.value.minor.?);
    try testing.expectEqualStrings("v1.30.0", parsed.value.gitVersion.?);
    try testing.expectEqualStrings("abc123", parsed.value.gitCommit.?);
    try testing.expectEqualStrings("linux/amd64", parsed.value.platform.?);
}

test "APIGroupDiscoveryList JSON parsing" {
    const discovery_json =
        \\{
        \\  "kind": "APIGroupDiscoveryList",
        \\  "apiVersion": "apidiscovery.k8s.io/v2",
        \\  "metadata": {},
        \\  "items": [
        \\    {
        \\      "metadata": {"name": "apps"},
        \\      "versions": [
        \\        {
        \\          "version": "v1",
        \\          "resources": [
        \\            {
        \\              "resource": "deployments",
        \\              "singularResource": "deployment",
        \\              "scope": "Namespaced",
        \\              "verbs": ["get", "list", "create"]
        \\            }
        \\          ],
        \\          "freshness": "Current"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    const parsed = try json.parseFromSlice(
        apidiscovery.APIGroupDiscoveryList,
        testing.allocator,
        discovery_json,
        .{ .ignore_unknown_fields = true },
    );
    defer parsed.deinit();

    try testing.expectEqual(@as(usize, 1), parsed.value.items.items.len);

    const group = parsed.value.items.items[0];
    try testing.expectEqualStrings("apps", group.metadata.?.name.?);
    try testing.expectEqual(@as(usize, 1), group.versions.items.len);

    const ver = group.versions.items[0];
    try testing.expectEqualStrings("v1", ver.version.?);
    try testing.expectEqualStrings("Current", ver.freshness.?);
    try testing.expectEqual(@as(usize, 1), ver.resources.items.len);

    const res = ver.resources.items[0];
    try testing.expectEqualStrings("deployments", res.resource.?);
    try testing.expectEqualStrings("deployment", res.singularResource.?);
    try testing.expectEqualStrings("Namespaced", res.scope.?);
    try testing.expectEqual(@as(usize, 3), res.verbs.items.len);
}

test "APIGroupDiscoveryList JSON parsing: core group" {
    const core_json =
        \\{
        \\  "kind": "APIGroupDiscoveryList",
        \\  "apiVersion": "apidiscovery.k8s.io/v2",
        \\  "metadata": {},
        \\  "items": [
        \\    {
        \\      "metadata": {"name": ""},
        \\      "versions": [
        \\        {
        \\          "version": "v1",
        \\          "resources": [
        \\            {
        \\              "resource": "pods",
        \\              "singularResource": "pod",
        \\              "scope": "Namespaced",
        \\              "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]
        \\            },
        \\            {
        \\              "resource": "nodes",
        \\              "singularResource": "node",
        \\              "scope": "Cluster",
        \\              "verbs": ["get", "list", "watch"]
        \\            }
        \\          ],
        \\          "freshness": "Current"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    const parsed = try json.parseFromSlice(
        apidiscovery.APIGroupDiscoveryList,
        testing.allocator,
        core_json,
        .{ .ignore_unknown_fields = true },
    );
    defer parsed.deinit();

    const group = parsed.value.items.items[0];
    try testing.expectEqualStrings("", group.metadata.?.name.?);

    const ver = group.versions.items[0];
    try testing.expectEqual(@as(usize, 2), ver.resources.items.len);

    const pods = ver.resources.items[0];
    try testing.expectEqualStrings("pods", pods.resource.?);
    try testing.expectEqualStrings("Namespaced", pods.scope.?);

    const nodes = ver.resources.items[1];
    try testing.expectEqualStrings("nodes", nodes.resource.?);
    try testing.expectEqualStrings("Cluster", nodes.scope.?);
}

test "APIGroupDiscoveryList JSON parsing: with subresources" {
    const json_with_subresources =
        \\{
        \\  "kind": "APIGroupDiscoveryList",
        \\  "apiVersion": "apidiscovery.k8s.io/v2",
        \\  "metadata": {},
        \\  "items": [
        \\    {
        \\      "metadata": {"name": "apps"},
        \\      "versions": [
        \\        {
        \\          "version": "v1",
        \\          "resources": [
        \\            {
        \\              "resource": "deployments",
        \\              "singularResource": "deployment",
        \\              "scope": "Namespaced",
        \\              "verbs": ["get", "list"],
        \\              "subresources": [
        \\                {
        \\                  "subresource": "status",
        \\                  "verbs": ["get", "patch", "update"]
        \\                },
        \\                {
        \\                  "subresource": "scale",
        \\                  "verbs": ["get", "patch", "update"]
        \\                }
        \\              ]
        \\            }
        \\          ],
        \\          "freshness": "Current"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    const parsed = try json.parseFromSlice(
        apidiscovery.APIGroupDiscoveryList,
        testing.allocator,
        json_with_subresources,
        .{ .ignore_unknown_fields = true },
    );
    defer parsed.deinit();

    const res = parsed.value.items.items[0].versions.items[0].resources.items[0];
    try testing.expectEqual(@as(usize, 2), res.subresources.items.len);
    try testing.expectEqualStrings("status", res.subresources.items[0].subresource.?);
    try testing.expectEqualStrings("scale", res.subresources.items[1].subresource.?);
    try testing.expectEqual(@as(usize, 3), res.subresources.items[0].verbs.items.len);
}

test "APIGroupDiscoveryList JSON parsing: multiple groups and versions" {
    const multi_json =
        \\{
        \\  "kind": "APIGroupDiscoveryList",
        \\  "apiVersion": "apidiscovery.k8s.io/v2",
        \\  "metadata": {},
        \\  "items": [
        \\    {
        \\      "metadata": {"name": "apps"},
        \\      "versions": [
        \\        {
        \\          "version": "v1",
        \\          "resources": [
        \\            {"resource": "deployments", "singularResource": "deployment", "scope": "Namespaced", "verbs": ["get"]},
        \\            {"resource": "statefulsets", "singularResource": "statefulset", "scope": "Namespaced", "verbs": ["get"]}
        \\          ],
        \\          "freshness": "Current"
        \\        }
        \\      ]
        \\    },
        \\    {
        \\      "metadata": {"name": "batch"},
        \\      "versions": [
        \\        {
        \\          "version": "v1",
        \\          "resources": [
        \\            {"resource": "jobs", "singularResource": "job", "scope": "Namespaced", "verbs": ["get", "create"]},
        \\            {"resource": "cronjobs", "singularResource": "cronjob", "scope": "Namespaced", "verbs": ["get", "create"]}
        \\          ],
        \\          "freshness": "Current"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    const parsed = try json.parseFromSlice(
        apidiscovery.APIGroupDiscoveryList,
        testing.allocator,
        multi_json,
        .{ .ignore_unknown_fields = true },
    );
    defer parsed.deinit();

    try testing.expectEqual(@as(usize, 2), parsed.value.items.items.len);
    try testing.expectEqualStrings("apps", parsed.value.items.items[0].metadata.?.name.?);
    try testing.expectEqualStrings("batch", parsed.value.items.items[1].metadata.?.name.?);

    const apps_resources = parsed.value.items.items[0].versions.items[0].resources.items;
    try testing.expectEqual(@as(usize, 2), apps_resources.len);
    try testing.expectEqualStrings("deployments", apps_resources[0].resource.?);
    try testing.expectEqualStrings("statefulsets", apps_resources[1].resource.?);

    const batch_resources = parsed.value.items.items[1].versions.items[0].resources.items;
    try testing.expectEqual(@as(usize, 2), batch_resources.len);
    try testing.expectEqualStrings("jobs", batch_resources[0].resource.?);
    try testing.expectEqualStrings("cronjobs", batch_resources[1].resource.?);
}

test "APIGroupDiscoveryList JSON parsing: multi-version group" {
    const multi_ver_json =
        \\{
        \\  "kind": "APIGroupDiscoveryList",
        \\  "apiVersion": "apidiscovery.k8s.io/v2",
        \\  "metadata": {},
        \\  "items": [
        \\    {
        \\      "metadata": {"name": "autoscaling"},
        \\      "versions": [
        \\        {
        \\          "version": "v2",
        \\          "resources": [{"resource": "horizontalpodautoscalers", "scope": "Namespaced", "verbs": ["get"]}],
        \\          "freshness": "Current"
        \\        },
        \\        {
        \\          "version": "v1",
        \\          "resources": [{"resource": "horizontalpodautoscalers", "scope": "Namespaced", "verbs": ["get"]}],
        \\          "freshness": "Current"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    const parsed = try json.parseFromSlice(
        apidiscovery.APIGroupDiscoveryList,
        testing.allocator,
        multi_ver_json,
        .{ .ignore_unknown_fields = true },
    );
    defer parsed.deinit();

    const group = parsed.value.items.items[0];
    try testing.expectEqual(@as(usize, 2), group.versions.items.len);
    try testing.expectEqualStrings("v2", group.versions.items[0].version.?);
    try testing.expectEqualStrings("v1", group.versions.items[1].version.?);
}

test "APIGroupDiscoveryList JSON parsing: stale freshness" {
    const stale_json =
        \\{
        \\  "kind": "APIGroupDiscoveryList",
        \\  "apiVersion": "apidiscovery.k8s.io/v2",
        \\  "metadata": {},
        \\  "items": [
        \\    {
        \\      "metadata": {"name": "example.io"},
        \\      "versions": [
        \\        {
        \\          "version": "v1alpha1",
        \\          "resources": [{"resource": "widgets", "scope": "Namespaced", "verbs": ["get"]}],
        \\          "freshness": "Stale"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    const parsed = try json.parseFromSlice(
        apidiscovery.APIGroupDiscoveryList,
        testing.allocator,
        stale_json,
        .{ .ignore_unknown_fields = true },
    );
    defer parsed.deinit();

    try testing.expectEqualStrings("Stale", parsed.value.items.items[0].versions.items[0].freshness.?);
}

test "APIGroupDiscoveryList JSON parsing: empty items" {
    const empty_json =
        \\{
        \\  "kind": "APIGroupDiscoveryList",
        \\  "apiVersion": "apidiscovery.k8s.io/v2",
        \\  "metadata": {},
        \\  "items": []
        \\}
    ;

    const parsed = try json.parseFromSlice(
        apidiscovery.APIGroupDiscoveryList,
        testing.allocator,
        empty_json,
        .{ .ignore_unknown_fields = true },
    );
    defer parsed.deinit();

    try testing.expectEqual(@as(usize, 0), parsed.value.items.items.len);
}

test "APIGroupDiscoveryList JSON parsing: shortNames and categories" {
    const rich_json =
        \\{
        \\  "kind": "APIGroupDiscoveryList",
        \\  "apiVersion": "apidiscovery.k8s.io/v2",
        \\  "metadata": {},
        \\  "items": [
        \\    {
        \\      "metadata": {"name": "apps"},
        \\      "versions": [
        \\        {
        \\          "version": "v1",
        \\          "resources": [
        \\            {
        \\              "resource": "deployments",
        \\              "singularResource": "deployment",
        \\              "scope": "Namespaced",
        \\              "verbs": ["get", "list"],
        \\              "shortNames": ["deploy"],
        \\              "categories": ["all"]
        \\            }
        \\          ],
        \\          "freshness": "Current"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    const parsed = try json.parseFromSlice(
        apidiscovery.APIGroupDiscoveryList,
        testing.allocator,
        rich_json,
        .{ .ignore_unknown_fields = true },
    );
    defer parsed.deinit();

    const res = parsed.value.items.items[0].versions.items[0].resources.items[0];
    try testing.expectEqual(@as(usize, 1), res.shortNames.items.len);
    try testing.expectEqualStrings("deploy", res.shortNames.items[0]);
    try testing.expectEqual(@as(usize, 1), res.categories.items.len);
    try testing.expectEqualStrings("all", res.categories.items[0]);
}

test "APIGroupDiscoveryList JSON parsing: CRD resource" {
    const crd_json =
        \\{
        \\  "kind": "APIGroupDiscoveryList",
        \\  "apiVersion": "apidiscovery.k8s.io/v2",
        \\  "metadata": {},
        \\  "items": [
        \\    {
        \\      "metadata": {"name": "kro.run"},
        \\      "versions": [
        \\        {
        \\          "version": "v1alpha1",
        \\          "resources": [
        \\            {
        \\              "resource": "resourcegraphdefinitions",
        \\              "singularResource": "resourcegraphdefinition",
        \\              "scope": "Cluster",
        \\              "verbs": ["delete", "deletecollection", "get", "list", "patch", "create", "update", "watch"],
        \\              "shortNames": ["rgd"],
        \\              "categories": [],
        \\              "subresources": [
        \\                {"subresource": "status", "verbs": ["get", "patch", "update"]}
        \\              ]
        \\            }
        \\          ],
        \\          "freshness": "Current"
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    const parsed = try json.parseFromSlice(
        apidiscovery.APIGroupDiscoveryList,
        testing.allocator,
        crd_json,
        .{ .ignore_unknown_fields = true },
    );
    defer parsed.deinit();

    const group = parsed.value.items.items[0];
    try testing.expectEqualStrings("kro.run", group.metadata.?.name.?);

    const res = group.versions.items[0].resources.items[0];
    try testing.expectEqualStrings("resourcegraphdefinitions", res.resource.?);
    try testing.expectEqualStrings("resourcegraphdefinition", res.singularResource.?);
    try testing.expectEqualStrings("Cluster", res.scope.?);
    try testing.expectEqual(@as(usize, 8), res.verbs.items.len);
    try testing.expectEqual(@as(usize, 1), res.shortNames.items.len);
    try testing.expectEqualStrings("rgd", res.shortNames.items[0]);
    try testing.expectEqual(@as(usize, 1), res.subresources.items.len);
    try testing.expectEqualStrings("status", res.subresources.items[0].subresource.?);
}

test "ServerVersion JSON parsing: minimal fields" {
    const minimal_json =
        \\{"major":"1","minor":"28"}
    ;

    const parsed = try json.parseFromSlice(ServerVersionJson, testing.allocator, minimal_json, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    try testing.expectEqualStrings("1", parsed.value.major.?);
    try testing.expectEqualStrings("28", parsed.value.minor.?);
    try testing.expect(parsed.value.gitVersion == null);
    try testing.expect(parsed.value.platform == null);
}

test "findResource: core group edge cases" {
    var tg = try makeTestGroups(testing.allocator);
    defer tg.deinit();
    const groups = tg.groups;

    // configmaps is namespaced
    try testing.expect(findResource(groups, .{ .group = "", .version = "v1", .resource = "configmaps" }));

    // Case sensitivity
    try testing.expect(!findResource(groups, .{ .group = "", .version = "v1", .resource = "Pods" }));
    try testing.expect(!findResource(groups, .{ .group = "", .version = "v1", .resource = "PODS" }));
}

test "findResource: empty inputs" {
    var tg = try makeTestGroups(testing.allocator);
    defer tg.deinit();
    const groups = tg.groups;

    try testing.expect(!findResource(groups, .{ .group = "", .version = "", .resource = "" }));
    try testing.expect(!findResource(groups, .{ .group = "", .version = "v1", .resource = "" }));
}

test "findResource: against empty groups" {
    const empty: []const apidiscovery.APIGroupDiscovery = &.{};
    try testing.expect(!findResource(empty, .{ .group = "", .version = "v1", .resource = "pods" }));
}

test "findGroupVersion: against empty groups" {
    const empty: []const apidiscovery.APIGroupDiscovery = &.{};
    try testing.expect(!findGroupVersion(empty, .{ .group = "", .version = "v1" }));
}

test "findGroupVersion: case sensitivity" {
    var tg = try makeTestGroups(testing.allocator);
    defer tg.deinit();
    const groups = tg.groups;

    try testing.expect(!findGroupVersion(groups, .{ .group = "Apps", .version = "v1" }));
    try testing.expect(!findGroupVersion(groups, .{ .group = "apps", .version = "V1" }));
}

test "filterByVerbs: empty verbs filter returns all" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var resources_list: [1]apidiscovery.APIResourceDiscovery = undefined;
    resources_list[0] = .{};
    resources_list[0].resource = "pods";
    resources_list[0].verbs = .empty;
    try resources_list[0].verbs.append(alloc, "get");

    const filtered = try filterByVerbs(alloc, &resources_list, &.{});
    try testing.expectEqual(@as(usize, 1), filtered.len);
}

test "filterByVerbs: empty resources returns empty" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const empty: []const apidiscovery.APIResourceDiscovery = &.{};
    const filtered = try filterByVerbs(alloc, empty, &[_][]const u8{"get"});
    try testing.expectEqual(@as(usize, 0), filtered.len);
}

test "filterByVerbs: resource with no verbs matches empty filter" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var resources_list: [1]apidiscovery.APIResourceDiscovery = undefined;
    resources_list[0] = .{};
    resources_list[0].resource = "bindings";

    // No verbs on resource, but also no required verbs — should match
    const filtered = try filterByVerbs(alloc, &resources_list, &.{});
    try testing.expectEqual(@as(usize, 1), filtered.len);

    // Require "get" — resource has no verbs — should not match
    const filtered2 = try filterByVerbs(alloc, &resources_list, &[_][]const u8{"get"});
    try testing.expectEqual(@as(usize, 0), filtered2.len);
}

test "groupNameFromDiscovery: null metadata" {
    const group = apidiscovery.APIGroupDiscovery{};
    try testing.expectEqualStrings("", groupNameFromDiscovery(&group));
}

test "groupNameFromDiscovery: metadata with null name" {
    const meta = @import("k8s_api").ObjectMeta{};
    // name is null by default in the generated type
    var group = apidiscovery.APIGroupDiscovery{};
    group.metadata = meta;
    try testing.expectEqualStrings("", groupNameFromDiscovery(&group));
}

test "groupNameFromDiscovery: long group name" {
    var meta = @import("k8s_api").ObjectMeta{};
    meta.name = "rbac.authorization.k8s.io";
    var group = apidiscovery.APIGroupDiscovery{};
    group.metadata = meta;
    try testing.expectEqualStrings("rbac.authorization.k8s.io", groupNameFromDiscovery(&group));
}
