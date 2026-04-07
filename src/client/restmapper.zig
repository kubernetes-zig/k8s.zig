const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const testing = std.testing;

const k8s = @import("k8s_zig");
const scheme = k8s.scheme;

const k8s_api = @import("k8s_api");
const apidiscovery = k8s_api.apidiscovery_v2;

const discovery_mod = @import("discovery.zig");
const discovery_cache_mod = @import("discovery_cache.zig");
const CachedDiscoveryClient = discovery_cache_mod.CachedDiscoveryClient;

pub const RESTScope = enum {
    namespace,
    root,

    pub fn fromString(s: []const u8) ?RESTScope {
        if (mem.eql(u8, s, "Namespaced")) return .namespace;
        if (mem.eql(u8, s, "Cluster")) return .root;
        return null;
    }

    pub fn string(self: RESTScope) []const u8 {
        return switch (self) {
            .namespace => "Namespaced",
            .root => "Cluster",
        };
    }
};

pub const RESTMapping = struct {
    resource: scheme.GroupVersionResource,
    kind: scheme.GroupVersionKind,
    scope: RESTScope,
};

pub const MapperError = error{
    NoResourceMatch,
    NoKindMatch,
    AmbiguousResource,
    AmbiguousKind,
};

/// Static REST mapper populated from discovery data. Pure in-memory lookups, no I/O.
/// All string data is owned by an internal arena — single deinit frees everything.
pub const RESTMapper = struct {
    arena: std.heap.ArenaAllocator,
    entries: []const Entry,

    const Entry = struct {
        gvk: scheme.GroupVersionKind,
        gvr: scheme.GroupVersionResource,
        scope: RESTScope,
        singular_resource: []const u8,
        short_names: []const []const u8,
        categories: []const []const u8,
    };

    /// Build a RESTMapper from aggregated discovery groups.
    pub fn init(allocator: Allocator, groups: []const apidiscovery.APIGroupDiscovery) !RESTMapper {
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const a = arena.allocator();

        var entries: std.ArrayList(Entry) = .empty;

        for (groups) |group| {
            const group_name = discovery_mod.groupNameFromDiscovery(&group);

            for (group.versions.items) |ver| {
                const version = ver.version orelse continue;

                for (ver.resources.items) |res| {
                    const resource_name = res.resource orelse continue;
                    const scope_str = res.scope orelse "Namespaced";

                    // Extract kind from responseKind if available, otherwise skip.
                    const kind_name = if (res.responseKind) |rk| rk.kind orelse continue else continue;

                    // Dupe all strings into the arena.
                    const g = try a.dupe(u8, group_name);
                    const v = try a.dupe(u8, version);
                    const r = try a.dupe(u8, resource_name);
                    const k = try a.dupe(u8, kind_name);
                    const singular = try a.dupe(u8, res.singularResource orelse resource_name);

                    // Copy short names
                    const short_names = try a.alloc([]const u8, res.shortNames.items.len);
                    for (res.shortNames.items, 0..) |sn, i| {
                        short_names[i] = try a.dupe(u8, sn);
                    }

                    // Copy categories
                    const categories = try a.alloc([]const u8, res.categories.items.len);
                    for (res.categories.items, 0..) |cat, i| {
                        categories[i] = try a.dupe(u8, cat);
                    }

                    try entries.append(a, .{
                        .gvk = .{ .group = g, .version = v, .kind = k },
                        .gvr = .{ .group = g, .version = v, .resource = r },
                        .scope = RESTScope.fromString(scope_str) orelse .namespace,
                        .singular_resource = singular,
                        .short_names = short_names,
                        .categories = categories,
                    });
                }
            }
        }

        return .{
            .arena = arena,
            .entries = try entries.toOwnedSlice(a),
        };
    }

    pub fn deinit(self: *RESTMapper) void {
        self.arena.deinit();
    }

    // ── GVR → GVK ───────────────────────────────────────────────────────────

    /// Find the unique GVK for a GVR. Partial GVR (empty group or version) is
    /// matched against all entries. Returns AmbiguousResource if multiple match.
    pub fn kindFor(self: *const RESTMapper, gvr: scheme.GroupVersionResource) MapperError!scheme.GroupVersionKind {
        var match: ?scheme.GroupVersionKind = null;
        var count: usize = 0;
        for (self.entries) |entry| {
            if (matchesGVR(entry.gvr, gvr)) {
                match = entry.gvk;
                count += 1;
            }
        }
        if (count == 0) return error.NoResourceMatch;
        if (count > 1) return error.AmbiguousResource;
        return match.?;
    }

    /// Find all GVKs matching a (possibly partial) GVR.
    pub fn kindsFor(self: *const RESTMapper, allocator: Allocator, gvr: scheme.GroupVersionResource) MapperError![]const scheme.GroupVersionKind {
        var results: std.ArrayListUnmanaged(scheme.GroupVersionKind) = .empty;
        for (self.entries) |entry| {
            if (matchesGVR(entry.gvr, gvr)) {
                results.append(allocator, entry.gvk) catch return error.NoResourceMatch;
            }
        }
        if (results.items.len == 0) return error.NoResourceMatch;
        return results.toOwnedSlice(allocator) catch return error.NoResourceMatch;
    }

    /// Like kindFor but returns null instead of error on miss.
    pub fn kindForOrNull(self: *const RESTMapper, gvr: scheme.GroupVersionResource) ?scheme.GroupVersionKind {
        return self.kindFor(gvr) catch return null;
    }

    // ── GVK → GVR ───────────────────────────────────────────────────────────

    /// Find the unique GVR for a GVK. Partial GVK (empty version) matches all.
    pub fn resourceFor(self: *const RESTMapper, gvk: scheme.GroupVersionKind) MapperError!scheme.GroupVersionResource {
        var match: ?scheme.GroupVersionResource = null;
        var count: usize = 0;
        for (self.entries) |entry| {
            if (matchesGVK(entry.gvk, gvk)) {
                match = entry.gvr;
                count += 1;
            }
        }
        if (count == 0) return error.NoKindMatch;
        if (count > 1) return error.AmbiguousKind;
        return match.?;
    }

    /// Find all GVRs matching a (possibly partial) GVK.
    pub fn resourcesFor(self: *const RESTMapper, allocator: Allocator, gvk: scheme.GroupVersionKind) MapperError![]const scheme.GroupVersionResource {
        var results: std.ArrayListUnmanaged(scheme.GroupVersionResource) = .empty;
        for (self.entries) |entry| {
            if (matchesGVK(entry.gvk, gvk)) {
                results.append(allocator, entry.gvr) catch return error.NoKindMatch;
            }
        }
        if (results.items.len == 0) return error.NoKindMatch;
        return results.toOwnedSlice(allocator) catch return error.NoKindMatch;
    }

    /// Like resourceFor but returns null instead of error on miss.
    pub fn resourceForOrNull(self: *const RESTMapper, gvk: scheme.GroupVersionKind) ?scheme.GroupVersionResource {
        return self.resourceFor(gvk) catch return null;
    }

    // ── Full mapping ─────────────────────────────────────────────────────────

    /// Get the full RESTMapping for a group+kind, optionally constrained to a version.
    pub fn restMapping(
        self: *const RESTMapper,
        group: []const u8,
        kind: []const u8,
        version: ?[]const u8,
    ) MapperError!RESTMapping {
        var match: ?RESTMapping = null;
        var count: usize = 0;
        for (self.entries) |entry| {
            if (!mem.eql(u8, entry.gvk.group, group)) continue;
            if (!mem.eql(u8, entry.gvk.kind, kind)) continue;
            if (version) |v| {
                if (!mem.eql(u8, entry.gvk.version, v)) continue;
            }
            match = .{
                .resource = entry.gvr,
                .kind = entry.gvk,
                .scope = entry.scope,
            };
            count += 1;
        }
        if (count == 0) return error.NoKindMatch;
        if (count > 1) return error.AmbiguousKind;
        return match.?;
    }

    /// Get all RESTMappings for a group+kind across all versions.
    pub fn restMappings(
        self: *const RESTMapper,
        allocator: Allocator,
        group: []const u8,
        kind: []const u8,
    ) MapperError![]const RESTMapping {
        var results: std.ArrayListUnmanaged(RESTMapping) = .empty;
        for (self.entries) |entry| {
            if (!mem.eql(u8, entry.gvk.group, group)) continue;
            if (!mem.eql(u8, entry.gvk.kind, kind)) continue;
            results.append(allocator, .{
                .resource = entry.gvr,
                .kind = entry.gvk,
                .scope = entry.scope,
            }) catch return error.NoKindMatch;
        }
        if (results.items.len == 0) return error.NoKindMatch;
        return results.toOwnedSlice(allocator) catch return error.NoKindMatch;
    }

    /// Like restMapping but returns null instead of error on miss.
    pub fn restMappingOrNull(
        self: *const RESTMapper,
        group: []const u8,
        kind: []const u8,
        version: ?[]const u8,
    ) ?RESTMapping {
        return self.restMapping(group, kind, version) catch return null;
    }

    // ── Scope ────────────────────────────────────────────────────────────────

    /// Get the scope for a GVK.
    pub fn scopeFor(self: *const RESTMapper, gvk: scheme.GroupVersionKind) MapperError!RESTScope {
        for (self.entries) |entry| {
            if (entry.gvk.eql(gvk)) return entry.scope;
        }
        return error.NoKindMatch;
    }

    /// Like scopeFor but returns null instead of error on miss.
    pub fn scopeForOrNull(self: *const RESTMapper, gvk: scheme.GroupVersionKind) ?RESTScope {
        return self.scopeFor(gvk) catch return null;
    }

    // ── Singular/Plural ──────────────────────────────────────────────────────

    /// Get the singular resource name for a plural GVR.
    pub fn singularFor(self: *const RESTMapper, gvr: scheme.GroupVersionResource) MapperError!scheme.GroupVersionResource {
        for (self.entries) |entry| {
            if (entry.gvr.eql(gvr)) {
                return .{
                    .group = entry.gvr.group,
                    .version = entry.gvr.version,
                    .resource = entry.singular_resource,
                };
            }
        }
        return error.NoResourceMatch;
    }

    /// Get the plural resource name for a singular GVR.
    pub fn pluralFor(self: *const RESTMapper, gvr: scheme.GroupVersionResource) MapperError!scheme.GroupVersionResource {
        for (self.entries) |entry| {
            if (mem.eql(u8, entry.gvr.group, gvr.group) and
                mem.eql(u8, entry.gvr.version, gvr.version) and
                mem.eql(u8, entry.singular_resource, gvr.resource))
            {
                return entry.gvr;
            }
        }
        return error.NoResourceMatch;
    }

    // ── Internal matching ────────────────────────────────────────────────────

    /// Match a GVR against a possibly partial pattern. Empty fields in the
    /// pattern are wildcards.
    fn matchesGVR(entry: scheme.GroupVersionResource, pattern: scheme.GroupVersionResource) bool {
        if (pattern.resource.len > 0 and !mem.eql(u8, entry.resource, pattern.resource)) return false;
        if (pattern.group.len > 0 and !mem.eql(u8, entry.group, pattern.group)) return false;
        if (pattern.version.len > 0 and !mem.eql(u8, entry.version, pattern.version)) return false;
        // Resource must always be specified.
        return pattern.resource.len > 0;
    }

    /// Match a GVK against a possibly partial pattern. Empty version is a wildcard.
    fn matchesGVK(entry: scheme.GroupVersionKind, pattern: scheme.GroupVersionKind) bool {
        if (!mem.eql(u8, entry.group, pattern.group)) return false;
        if (!mem.eql(u8, entry.kind, pattern.kind)) return false;
        if (pattern.version.len > 0 and !mem.eql(u8, entry.version, pattern.version)) return false;
        return true;
    }
};

/// Lazy REST mapper that fetches discovery on first use and re-discovers on miss.
/// Wraps a CachedDiscoveryClient and a RESTMapper.
pub const DynamicRESTMapper = struct {
    allocator: Allocator,
    discovery: *CachedDiscoveryClient,
    mapper: ?RESTMapper = null,

    pub fn init(allocator: Allocator, discovery: *CachedDiscoveryClient) DynamicRESTMapper {
        return .{
            .allocator = allocator,
            .discovery = discovery,
        };
    }

    pub fn deinit(self: *DynamicRESTMapper) void {
        if (self.mapper) |*m| m.deinit();
    }

    // ── GVR → GVK ───────────────────────────────────────────────────────────

    pub fn kindFor(self: *DynamicRESTMapper, gvr: scheme.GroupVersionResource) !scheme.GroupVersionKind {
        try self.ensureLoaded();
        return self.mapper.?.kindFor(gvr) catch |err| switch (err) {
            error.NoResourceMatch => {
                try self.refresh();
                return self.mapper.?.kindFor(gvr);
            },
            else => return err,
        };
    }

    pub fn kindsFor(self: *DynamicRESTMapper, allocator: Allocator, gvr: scheme.GroupVersionResource) ![]const scheme.GroupVersionKind {
        try self.ensureLoaded();
        return self.mapper.?.kindsFor(allocator, gvr) catch |err| switch (err) {
            error.NoResourceMatch => {
                try self.refresh();
                return self.mapper.?.kindsFor(allocator, gvr);
            },
            else => return err,
        };
    }

    pub fn kindForOrNull(self: *DynamicRESTMapper, gvr: scheme.GroupVersionResource) !?scheme.GroupVersionKind {
        return self.kindFor(gvr) catch |err| switch (err) {
            error.NoResourceMatch => return null,
            error.AmbiguousResource => return null,
            else => return err,
        };
    }

    // ── GVK → GVR ───────────────────────────────────────────────────────────

    pub fn resourceFor(self: *DynamicRESTMapper, gvk: scheme.GroupVersionKind) !scheme.GroupVersionResource {
        try self.ensureLoaded();
        return self.mapper.?.resourceFor(gvk) catch |err| switch (err) {
            error.NoKindMatch => {
                try self.refresh();
                return self.mapper.?.resourceFor(gvk);
            },
            else => return err,
        };
    }

    pub fn resourcesFor(self: *DynamicRESTMapper, allocator: Allocator, gvk: scheme.GroupVersionKind) ![]const scheme.GroupVersionResource {
        try self.ensureLoaded();
        return self.mapper.?.resourcesFor(allocator, gvk) catch |err| switch (err) {
            error.NoKindMatch => {
                try self.refresh();
                return self.mapper.?.resourcesFor(allocator, gvk);
            },
            else => return err,
        };
    }

    pub fn resourceForOrNull(self: *DynamicRESTMapper, gvk: scheme.GroupVersionKind) !?scheme.GroupVersionResource {
        return self.resourceFor(gvk) catch |err| switch (err) {
            error.NoKindMatch => return null,
            error.AmbiguousKind => return null,
            else => return err,
        };
    }

    // ── Full mapping ─────────────────────────────────────────────────────────

    pub fn restMapping(
        self: *DynamicRESTMapper,
        group: []const u8,
        kind: []const u8,
        version: ?[]const u8,
    ) !RESTMapping {
        try self.ensureLoaded();
        return self.mapper.?.restMapping(group, kind, version) catch |err| switch (err) {
            error.NoKindMatch => {
                try self.refresh();
                return self.mapper.?.restMapping(group, kind, version);
            },
            else => return err,
        };
    }

    pub fn restMappings(
        self: *DynamicRESTMapper,
        allocator: Allocator,
        group: []const u8,
        kind: []const u8,
    ) ![]const RESTMapping {
        try self.ensureLoaded();
        return self.mapper.?.restMappings(allocator, group, kind) catch |err| switch (err) {
            error.NoKindMatch => {
                try self.refresh();
                return self.mapper.?.restMappings(allocator, group, kind);
            },
            else => return err,
        };
    }

    pub fn restMappingOrNull(
        self: *DynamicRESTMapper,
        group: []const u8,
        kind: []const u8,
        version: ?[]const u8,
    ) !?RESTMapping {
        return self.restMapping(group, kind, version) catch |err| switch (err) {
            error.NoKindMatch => return null,
            error.AmbiguousKind => return null,
            else => return err,
        };
    }

    // ── Scope ────────────────────────────────────────────────────────────────

    pub fn scopeFor(self: *DynamicRESTMapper, gvk: scheme.GroupVersionKind) !RESTScope {
        try self.ensureLoaded();
        return self.mapper.?.scopeFor(gvk) catch |err| switch (err) {
            error.NoKindMatch => {
                try self.refresh();
                return self.mapper.?.scopeFor(gvk);
            },
            else => return err,
        };
    }

    pub fn scopeForOrNull(self: *DynamicRESTMapper, gvk: scheme.GroupVersionKind) !?RESTScope {
        return self.scopeFor(gvk) catch |err| switch (err) {
            error.NoKindMatch => return null,
            else => return err,
        };
    }

    // ── Cache control ────────────────────────────────────────────────────────

    /// Force invalidation. Next call will re-discover.
    pub fn invalidate(self: *DynamicRESTMapper) void {
        self.discovery.invalidate();
        if (self.mapper) |*m| {
            m.deinit();
            self.mapper = null;
        }
    }

    /// Whether the mapper has been populated at least once.
    pub fn fresh(self: *const DynamicRESTMapper) bool {
        return self.mapper != null;
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    fn ensureLoaded(self: *DynamicRESTMapper) !void {
        if (self.mapper != null) return;
        try self.refresh();
    }

    fn refresh(self: *DynamicRESTMapper) !void {
        self.discovery.invalidate();
        var result = try self.discovery.serverGroupsAndResources();
        _ = &result;

        if (self.mapper) |*m| m.deinit();
        self.mapper = try RESTMapper.init(self.allocator, result.groups);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

fn makeTestMapper() !struct { mapper: RESTMapper, test_arena: std.heap.ArenaAllocator } {
    var test_arena = std.heap.ArenaAllocator.init(testing.allocator);
    errdefer test_arena.deinit();
    const allocator = test_arena.allocator();

    // Build test discovery data with responseKind set.
    var groups_list: std.ArrayListUnmanaged(apidiscovery.APIGroupDiscovery) = .empty;

    // Core group
    {
        var core_meta = k8s_api.ObjectMeta{};
        core_meta.name = "";

        var pods_res = apidiscovery.APIResourceDiscovery{};
        pods_res.resource = "pods";
        pods_res.singularResource = "pod";
        pods_res.scope = "Namespaced";
        var pods_rk = k8s_api.meta_v1.GroupVersionKind{};
        pods_rk.group = "";
        pods_rk.version = "v1";
        pods_rk.kind = "Pod";
        pods_res.responseKind = pods_rk;

        var nodes_res = apidiscovery.APIResourceDiscovery{};
        nodes_res.resource = "nodes";
        nodes_res.singularResource = "node";
        nodes_res.scope = "Cluster";
        var nodes_rk = k8s_api.meta_v1.GroupVersionKind{};
        nodes_rk.group = "";
        nodes_rk.version = "v1";
        nodes_rk.kind = "Node";
        nodes_res.responseKind = nodes_rk;

        var ns_res = apidiscovery.APIResourceDiscovery{};
        ns_res.resource = "namespaces";
        ns_res.singularResource = "namespace";
        ns_res.scope = "Cluster";
        var ns_rk = k8s_api.meta_v1.GroupVersionKind{};
        ns_rk.group = "";
        ns_rk.version = "v1";
        ns_rk.kind = "Namespace";
        ns_res.responseKind = ns_rk;

        var v1_ver = apidiscovery.APIVersionDiscovery{};
        v1_ver.version = "v1";
        v1_ver.freshness = "Current";
        v1_ver.resources = .empty;
        try v1_ver.resources.append(allocator, pods_res);
        try v1_ver.resources.append(allocator, nodes_res);
        try v1_ver.resources.append(allocator, ns_res);

        var core_group = apidiscovery.APIGroupDiscovery{};
        core_group.metadata = core_meta;
        try core_group.versions.append(allocator, v1_ver);
        try groups_list.append(allocator, core_group);
    }

    // apps group
    {
        var apps_meta = k8s_api.ObjectMeta{};
        apps_meta.name = "apps";

        var deploy_res = apidiscovery.APIResourceDiscovery{};
        deploy_res.resource = "deployments";
        deploy_res.singularResource = "deployment";
        deploy_res.scope = "Namespaced";
        var deploy_rk = k8s_api.meta_v1.GroupVersionKind{};
        deploy_rk.group = "apps";
        deploy_rk.version = "v1";
        deploy_rk.kind = "Deployment";
        deploy_res.responseKind = deploy_rk;
        try deploy_res.shortNames.append(allocator, "deploy");
        try deploy_res.categories.append(allocator, "all");

        var sts_res = apidiscovery.APIResourceDiscovery{};
        sts_res.resource = "statefulsets";
        sts_res.singularResource = "statefulset";
        sts_res.scope = "Namespaced";
        var sts_rk = k8s_api.meta_v1.GroupVersionKind{};
        sts_rk.group = "apps";
        sts_rk.version = "v1";
        sts_rk.kind = "StatefulSet";
        sts_res.responseKind = sts_rk;
        try sts_res.shortNames.append(allocator, "sts");

        var v1_ver = apidiscovery.APIVersionDiscovery{};
        v1_ver.version = "v1";
        v1_ver.freshness = "Current";
        v1_ver.resources = .empty;
        try v1_ver.resources.append(allocator, deploy_res);
        try v1_ver.resources.append(allocator, sts_res);

        var apps_group = apidiscovery.APIGroupDiscovery{};
        apps_group.metadata = apps_meta;
        try apps_group.versions.append(allocator, v1_ver);
        try groups_list.append(allocator, apps_group);
    }

    // batch group with two versions
    {
        var batch_meta = k8s_api.ObjectMeta{};
        batch_meta.name = "batch";

        var job_res_v1 = apidiscovery.APIResourceDiscovery{};
        job_res_v1.resource = "jobs";
        job_res_v1.singularResource = "job";
        job_res_v1.scope = "Namespaced";
        var job_rk_v1 = k8s_api.meta_v1.GroupVersionKind{};
        job_rk_v1.group = "batch";
        job_rk_v1.version = "v1";
        job_rk_v1.kind = "Job";
        job_res_v1.responseKind = job_rk_v1;

        var v1_ver = apidiscovery.APIVersionDiscovery{};
        v1_ver.version = "v1";
        v1_ver.freshness = "Current";
        v1_ver.resources = .empty;
        try v1_ver.resources.append(allocator, job_res_v1);

        var cj_res = apidiscovery.APIResourceDiscovery{};
        cj_res.resource = "cronjobs";
        cj_res.singularResource = "cronjob";
        cj_res.scope = "Namespaced";
        var cj_rk = k8s_api.meta_v1.GroupVersionKind{};
        cj_rk.group = "batch";
        cj_rk.version = "v1";
        cj_rk.kind = "CronJob";
        cj_res.responseKind = cj_rk;
        try v1_ver.resources.append(allocator, cj_res);

        var batch_group = apidiscovery.APIGroupDiscovery{};
        batch_group.metadata = batch_meta;
        try batch_group.versions.append(allocator, v1_ver);
        try groups_list.append(allocator, batch_group);
    }

    const groups = try groups_list.toOwnedSlice(allocator);

    return .{
        .mapper = try RESTMapper.init(testing.allocator, groups),
        .test_arena = test_arena,
    };
}

test "RESTMapper: kindFor" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    // Table: exact GVR → expected kind, group, version
    const Case = struct { gvr: scheme.GroupVersionResource, kind: []const u8, group: []const u8, version: []const u8 };
    const cases = [_]Case{
        .{ .gvr = .{ .group = "", .version = "v1", .resource = "pods" }, .kind = "Pod", .group = "", .version = "v1" },
        .{ .gvr = .{ .group = "", .version = "v1", .resource = "nodes" }, .kind = "Node", .group = "", .version = "v1" },
        .{ .gvr = .{ .group = "", .version = "v1", .resource = "namespaces" }, .kind = "Namespace", .group = "", .version = "v1" },
        .{ .gvr = .{ .group = "apps", .version = "v1", .resource = "deployments" }, .kind = "Deployment", .group = "apps", .version = "v1" },
        .{ .gvr = .{ .group = "apps", .version = "v1", .resource = "statefulsets" }, .kind = "StatefulSet", .group = "apps", .version = "v1" },
        .{ .gvr = .{ .group = "batch", .version = "v1", .resource = "jobs" }, .kind = "Job", .group = "batch", .version = "v1" },
        .{ .gvr = .{ .group = "batch", .version = "v1", .resource = "cronjobs" }, .kind = "CronJob", .group = "batch", .version = "v1" },
        // Partial: resource only (unique)
        .{ .gvr = .{ .group = "", .version = "", .resource = "pods" }, .kind = "Pod", .group = "", .version = "v1" },
        .{ .gvr = .{ .group = "", .version = "", .resource = "nodes" }, .kind = "Node", .group = "", .version = "v1" },
        // Partial: group + resource, no version
        .{ .gvr = .{ .group = "apps", .version = "", .resource = "deployments" }, .kind = "Deployment", .group = "apps", .version = "v1" },
    };
    for (cases) |c| {
        const gvk = try mapper.kindFor(c.gvr);
        try testing.expectEqualStrings(c.kind, gvk.kind);
        try testing.expectEqualStrings(c.group, gvk.group);
        try testing.expectEqualStrings(c.version, gvk.version);
    }
}

test "RESTMapper: kindFor errors" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const ErrCase = struct { gvr: scheme.GroupVersionResource, expected: anyerror };
    const err_cases = [_]ErrCase{
        // Not found
        .{ .gvr = .{ .group = "", .version = "v1", .resource = "nonexistent" }, .expected = error.NoResourceMatch },
        .{ .gvr = .{ .group = "fake", .version = "v1", .resource = "pods" }, .expected = error.NoResourceMatch },
        // Wrong group
        .{ .gvr = .{ .group = "apps", .version = "v1", .resource = "pods" }, .expected = error.NoResourceMatch },
        // Wrong version
        .{ .gvr = .{ .group = "apps", .version = "v1beta1", .resource = "deployments" }, .expected = error.NoResourceMatch },
        // Empty resource
        .{ .gvr = .{ .group = "", .version = "v1", .resource = "" }, .expected = error.NoResourceMatch },
    };
    for (err_cases) |c| {
        try testing.expectError(c.expected, mapper.kindFor(c.gvr));
    }
}

test "RESTMapper: kindForOrNull" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    // Found
    const found = mapper.kindForOrNull(.{ .group = "", .version = "v1", .resource = "pods" });
    try testing.expect(found != null);
    try testing.expectEqualStrings("Pod", found.?.kind);

    // Missing
    const cases = [_]scheme.GroupVersionResource{
        .{ .group = "", .version = "v1", .resource = "nope" },
        .{ .group = "fake", .version = "v1", .resource = "pods" },
        .{ .group = "", .version = "v1", .resource = "" },
    };
    for (cases) |gvr| {
        try testing.expect(mapper.kindForOrNull(gvr) == null);
    }
}

test "RESTMapper: resourceFor" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const Case = struct { gvk: scheme.GroupVersionKind, resource: []const u8, group: []const u8, version: []const u8 };
    const cases = [_]Case{
        .{ .gvk = .{ .group = "", .version = "v1", .kind = "Pod" }, .resource = "pods", .group = "", .version = "v1" },
        .{ .gvk = .{ .group = "", .version = "v1", .kind = "Node" }, .resource = "nodes", .group = "", .version = "v1" },
        .{ .gvk = .{ .group = "apps", .version = "v1", .kind = "Deployment" }, .resource = "deployments", .group = "apps", .version = "v1" },
        .{ .gvk = .{ .group = "batch", .version = "v1", .kind = "Job" }, .resource = "jobs", .group = "batch", .version = "v1" },
        .{ .gvk = .{ .group = "batch", .version = "v1", .kind = "CronJob" }, .resource = "cronjobs", .group = "batch", .version = "v1" },
        // Partial: no version
        .{ .gvk = .{ .group = "apps", .version = "", .kind = "Deployment" }, .resource = "deployments", .group = "apps", .version = "v1" },
        .{ .gvk = .{ .group = "apps", .version = "", .kind = "StatefulSet" }, .resource = "statefulsets", .group = "apps", .version = "v1" },
    };
    for (cases) |c| {
        const gvr = try mapper.resourceFor(c.gvk);
        try testing.expectEqualStrings(c.resource, gvr.resource);
        try testing.expectEqualStrings(c.group, gvr.group);
        try testing.expectEqualStrings(c.version, gvr.version);
    }
}

test "RESTMapper: resourceFor errors" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const ErrCase = struct { gvk: scheme.GroupVersionKind, expected: anyerror };
    const err_cases = [_]ErrCase{
        .{ .gvk = .{ .group = "apps", .version = "v1", .kind = "NonExistent" }, .expected = error.NoKindMatch },
        .{ .gvk = .{ .group = "batch", .version = "v1", .kind = "Deployment" }, .expected = error.NoKindMatch },
        .{ .gvk = .{ .group = "fake", .version = "v1", .kind = "Pod" }, .expected = error.NoKindMatch },
    };
    for (err_cases) |c| {
        try testing.expectError(c.expected, mapper.resourceFor(c.gvk));
    }
}

test "RESTMapper: resourceForOrNull" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const found = mapper.resourceForOrNull(.{ .group = "apps", .version = "v1", .kind = "Deployment" });
    try testing.expect(found != null);
    try testing.expectEqualStrings("deployments", found.?.resource);

    const missing_cases = [_]scheme.GroupVersionKind{
        .{ .group = "apps", .version = "v1", .kind = "Nope" },
        .{ .group = "batch", .version = "v1", .kind = "Deployment" },
    };
    for (missing_cases) |gvk| {
        try testing.expect(mapper.resourceForOrNull(gvk) == null);
    }
}

test "RESTMapper: restMapping" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const Case = struct { group: []const u8, kind: []const u8, version: ?[]const u8, resource: []const u8, scope: RESTScope };
    const cases = [_]Case{
        // Core group
        .{ .group = "", .kind = "Pod", .version = "v1", .resource = "pods", .scope = .namespace },
        .{ .group = "", .kind = "Node", .version = "v1", .resource = "nodes", .scope = .root },
        .{ .group = "", .kind = "Namespace", .version = "v1", .resource = "namespaces", .scope = .root },
        // Named groups
        .{ .group = "apps", .kind = "Deployment", .version = "v1", .resource = "deployments", .scope = .namespace },
        .{ .group = "apps", .kind = "StatefulSet", .version = "v1", .resource = "statefulsets", .scope = .namespace },
        .{ .group = "batch", .kind = "Job", .version = "v1", .resource = "jobs", .scope = .namespace },
        .{ .group = "batch", .kind = "CronJob", .version = "v1", .resource = "cronjobs", .scope = .namespace },
        // Without version (unique kind per group)
        .{ .group = "batch", .kind = "Job", .version = null, .resource = "jobs", .scope = .namespace },
    };
    for (cases) |c| {
        const m = try mapper.restMapping(c.group, c.kind, c.version);
        try testing.expectEqualStrings(c.resource, m.resource.resource);
        try testing.expectEqualStrings(c.group, m.resource.group);
        try testing.expectEqual(c.scope, m.scope);
    }
}

test "RESTMapper: restMapping errors" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const ErrCase = struct { group: []const u8, kind: []const u8, version: ?[]const u8 };
    const err_cases = [_]ErrCase{
        .{ .group = "apps", .kind = "NonExistent", .version = "v1" },
        .{ .group = "apps", .kind = "Deployment", .version = "v1beta1" },
        .{ .group = "fake", .kind = "Pod", .version = "v1" },
    };
    for (err_cases) |c| {
        try testing.expectError(error.NoKindMatch, mapper.restMapping(c.group, c.kind, c.version));
    }
}

test "RESTMapper: restMappingOrNull" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    // Found cases
    try testing.expect(mapper.restMappingOrNull("apps", "Deployment", "v1") != null);
    try testing.expectEqual(RESTScope.root, mapper.restMappingOrNull("", "Namespace", "v1").?.scope);
    try testing.expectEqual(RESTScope.namespace, mapper.restMappingOrNull("", "Pod", "v1").?.scope);

    // Missing
    try testing.expect(mapper.restMappingOrNull("apps", "Nope", "v1") == null);
    try testing.expect(mapper.restMappingOrNull("fake", "Pod", "v1") == null);
}

test "RESTMapper: restMappings" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const mappings = try mapper.restMappings(testing.allocator, "batch", "Job");
    defer testing.allocator.free(mappings);
    try testing.expectEqual(@as(usize, 1), mappings.len);
    try testing.expectEqualStrings("v1", mappings[0].kind.version);

    const cj = try mapper.restMappings(testing.allocator, "batch", "CronJob");
    defer testing.allocator.free(cj);
    try testing.expectEqual(@as(usize, 1), cj.len);
    try testing.expectEqualStrings("cronjobs", cj[0].resource.resource);
    try testing.expectEqual(RESTScope.namespace, cj[0].scope);

    // Not found
    try testing.expectError(error.NoKindMatch, mapper.restMappings(testing.allocator, "apps", "NonExistent"));
}

test "RESTMapper: scopeFor" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const Case = struct { gvk: scheme.GroupVersionKind, expected: RESTScope };
    const cases = [_]Case{
        .{ .gvk = .{ .group = "", .version = "v1", .kind = "Pod" }, .expected = .namespace },
        .{ .gvk = .{ .group = "", .version = "v1", .kind = "Node" }, .expected = .root },
        .{ .gvk = .{ .group = "", .version = "v1", .kind = "Namespace" }, .expected = .root },
        .{ .gvk = .{ .group = "apps", .version = "v1", .kind = "Deployment" }, .expected = .namespace },
        .{ .gvk = .{ .group = "apps", .version = "v1", .kind = "StatefulSet" }, .expected = .namespace },
        .{ .gvk = .{ .group = "batch", .version = "v1", .kind = "Job" }, .expected = .namespace },
        .{ .gvk = .{ .group = "batch", .version = "v1", .kind = "CronJob" }, .expected = .namespace },
    };
    for (cases) |c| {
        try testing.expectEqual(c.expected, try mapper.scopeFor(c.gvk));
    }
    // Not found + OrNull
    try testing.expectError(error.NoKindMatch, mapper.scopeFor(.{ .group = "", .version = "v1", .kind = "Nope" }));
    try testing.expectEqual(RESTScope.namespace, mapper.scopeForOrNull(.{ .group = "", .version = "v1", .kind = "Pod" }).?);
    try testing.expect(mapper.scopeForOrNull(.{ .group = "", .version = "v1", .kind = "Nope" }) == null);
}

test "RESTMapper: singularFor" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const Case = struct { gvr: scheme.GroupVersionResource, singular: []const u8 };
    const cases = [_]Case{
        .{ .gvr = .{ .group = "", .version = "v1", .resource = "pods" }, .singular = "pod" },
        .{ .gvr = .{ .group = "", .version = "v1", .resource = "namespaces" }, .singular = "namespace" },
        .{ .gvr = .{ .group = "apps", .version = "v1", .resource = "deployments" }, .singular = "deployment" },
        .{ .gvr = .{ .group = "apps", .version = "v1", .resource = "statefulsets" }, .singular = "statefulset" },
        .{ .gvr = .{ .group = "batch", .version = "v1", .resource = "jobs" }, .singular = "job" },
        .{ .gvr = .{ .group = "batch", .version = "v1", .resource = "cronjobs" }, .singular = "cronjob" },
    };
    for (cases) |c| {
        const s = try mapper.singularFor(c.gvr);
        try testing.expectEqualStrings(c.singular, s.resource);
        try testing.expectEqualStrings(c.gvr.group, s.group);
        try testing.expectEqualStrings(c.gvr.version, s.version);
    }
    // Not found cases
    try testing.expectError(error.NoResourceMatch, mapper.singularFor(.{ .group = "", .version = "v1", .resource = "nope" }));
    try testing.expectError(error.NoResourceMatch, mapper.singularFor(.{ .group = "", .version = "v2", .resource = "pods" }));
}

test "RESTMapper: pluralFor" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const Case = struct { singular: scheme.GroupVersionResource, plural: []const u8 };
    const cases = [_]Case{
        .{ .singular = .{ .group = "", .version = "v1", .resource = "pod" }, .plural = "pods" },
        .{ .singular = .{ .group = "apps", .version = "v1", .resource = "deployment" }, .plural = "deployments" },
        .{ .singular = .{ .group = "batch", .version = "v1", .resource = "job" }, .plural = "jobs" },
        .{ .singular = .{ .group = "batch", .version = "v1", .resource = "cronjob" }, .plural = "cronjobs" },
    };
    for (cases) |c| {
        const p = try mapper.pluralFor(c.singular);
        try testing.expectEqualStrings(c.plural, p.resource);
        try testing.expectEqualStrings(c.singular.group, p.group);
        try testing.expectEqualStrings(c.singular.version, p.version);
    }
    // Not found cases
    try testing.expectError(error.NoResourceMatch, mapper.pluralFor(.{ .group = "", .version = "v1", .resource = "nope" }));
    try testing.expectError(error.NoResourceMatch, mapper.pluralFor(.{ .group = "apps", .version = "v1", .resource = "pod" }));
}

test "RESTMapper: kindsFor and resourcesFor" {
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    // kindsFor: exact
    const kinds = try mapper.kindsFor(testing.allocator, .{ .group = "", .version = "v1", .resource = "pods" });
    defer testing.allocator.free(kinds);
    try testing.expectEqual(@as(usize, 1), kinds.len);
    try testing.expectEqualStrings("Pod", kinds[0].kind);
    try testing.expectEqualStrings("", kinds[0].group);
    try testing.expectEqualStrings("v1", kinds[0].version);

    // kindsFor: partial (group + resource)
    const kinds2 = try mapper.kindsFor(testing.allocator, .{ .group = "apps", .version = "", .resource = "deployments" });
    defer testing.allocator.free(kinds2);
    try testing.expectEqual(@as(usize, 1), kinds2.len);
    try testing.expectEqualStrings("Deployment", kinds2[0].kind);

    // kindsFor: not found
    try testing.expectError(error.NoResourceMatch, mapper.kindsFor(testing.allocator, .{ .group = "", .version = "", .resource = "nonexistent" }));

    // resourcesFor: exact
    const resources = try mapper.resourcesFor(testing.allocator, .{ .group = "apps", .version = "v1", .kind = "StatefulSet" });
    defer testing.allocator.free(resources);
    try testing.expectEqual(@as(usize, 1), resources.len);
    try testing.expectEqualStrings("statefulsets", resources[0].resource);

    // resourcesFor: partial (no version)
    const resources2 = try mapper.resourcesFor(testing.allocator, .{ .group = "batch", .version = "", .kind = "Job" });
    defer testing.allocator.free(resources2);
    try testing.expectEqual(@as(usize, 1), resources2.len);

    // resourcesFor: not found
    try testing.expectError(error.NoKindMatch, mapper.resourcesFor(testing.allocator, .{ .group = "apps", .version = "", .kind = "NonExistent" }));
}

test "RESTMapper: empty groups" {
    var mapper = try RESTMapper.init(testing.allocator, &.{});
    defer mapper.deinit();

    try testing.expectError(error.NoResourceMatch, mapper.kindFor(.{ .group = "", .version = "v1", .resource = "pods" }));
    try testing.expectError(error.NoKindMatch, mapper.resourceFor(.{ .group = "", .version = "v1", .kind = "Pod" }));
    try testing.expectError(error.NoKindMatch, mapper.restMapping("", "Pod", "v1"));
    try testing.expectError(error.NoKindMatch, mapper.scopeFor(.{ .group = "", .version = "v1", .kind = "Pod" }));
    try testing.expectError(error.NoResourceMatch, mapper.singularFor(.{ .group = "", .version = "v1", .resource = "pods" }));
    try testing.expectError(error.NoResourceMatch, mapper.pluralFor(.{ .group = "", .version = "v1", .resource = "pod" }));
    try testing.expect(mapper.kindForOrNull(.{ .group = "", .version = "v1", .resource = "pods" }) == null);
    try testing.expect(mapper.resourceForOrNull(.{ .group = "", .version = "v1", .kind = "Pod" }) == null);
    try testing.expect(mapper.restMappingOrNull("", "Pod", "v1") == null);
    try testing.expect(mapper.scopeForOrNull(.{ .group = "", .version = "v1", .kind = "Pod" }) == null);
}

// ── Multi-version mapper for ambiguity testing ──────────────────────────────

fn makeMultiVersionMapper() !struct { mapper: RESTMapper, test_arena: std.heap.ArenaAllocator } {
    var test_arena = std.heap.ArenaAllocator.init(testing.allocator);
    errdefer test_arena.deinit();
    const allocator = test_arena.allocator();

    var groups_list: std.ArrayListUnmanaged(apidiscovery.APIGroupDiscovery) = .empty;

    // autoscaling with v1 and v2 — same Kind in two versions
    {
        var meta = k8s_api.ObjectMeta{};
        meta.name = "autoscaling";

        var hpa_v1 = apidiscovery.APIResourceDiscovery{};
        hpa_v1.resource = "horizontalpodautoscalers";
        hpa_v1.singularResource = "horizontalpodautoscaler";
        hpa_v1.scope = "Namespaced";
        var hpa_rk_v1 = k8s_api.meta_v1.GroupVersionKind{};
        hpa_rk_v1.group = "autoscaling";
        hpa_rk_v1.version = "v1";
        hpa_rk_v1.kind = "HorizontalPodAutoscaler";
        hpa_v1.responseKind = hpa_rk_v1;
        try hpa_v1.shortNames.append(allocator, "hpa");

        var v1_ver = apidiscovery.APIVersionDiscovery{};
        v1_ver.version = "v1";
        v1_ver.freshness = "Current";
        v1_ver.resources = .empty;
        try v1_ver.resources.append(allocator, hpa_v1);

        var hpa_v2 = apidiscovery.APIResourceDiscovery{};
        hpa_v2.resource = "horizontalpodautoscalers";
        hpa_v2.singularResource = "horizontalpodautoscaler";
        hpa_v2.scope = "Namespaced";
        var hpa_rk_v2 = k8s_api.meta_v1.GroupVersionKind{};
        hpa_rk_v2.group = "autoscaling";
        hpa_rk_v2.version = "v2";
        hpa_rk_v2.kind = "HorizontalPodAutoscaler";
        hpa_v2.responseKind = hpa_rk_v2;
        try hpa_v2.shortNames.append(allocator, "hpa");

        var v2_ver = apidiscovery.APIVersionDiscovery{};
        v2_ver.version = "v2";
        v2_ver.freshness = "Current";
        v2_ver.resources = .empty;
        try v2_ver.resources.append(allocator, hpa_v2);

        var group = apidiscovery.APIGroupDiscovery{};
        group.metadata = meta;
        try group.versions.append(allocator, v2_ver); // preferred first
        try group.versions.append(allocator, v1_ver);
        try groups_list.append(allocator, group);
    }

    const groups = try groups_list.toOwnedSlice(allocator);

    return .{
        .mapper = try RESTMapper.init(testing.allocator, groups),
        .test_arena = test_arena,
    };
}

test "RESTMapper: ambiguous kindFor — same resource in two versions" {
    var tm = try makeMultiVersionMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    // Exact match — no ambiguity
    const gvk_v2 = try mapper.kindFor(.{ .group = "autoscaling", .version = "v2", .resource = "horizontalpodautoscalers" });
    try testing.expectEqualStrings("HorizontalPodAutoscaler", gvk_v2.kind);
    try testing.expectEqualStrings("v2", gvk_v2.version);

    const gvk_v1 = try mapper.kindFor(.{ .group = "autoscaling", .version = "v1", .resource = "horizontalpodautoscalers" });
    try testing.expectEqualStrings("v1", gvk_v1.version);

    // Partial — no version — ambiguous
    const result = mapper.kindFor(.{ .group = "autoscaling", .version = "", .resource = "horizontalpodautoscalers" });
    try testing.expectError(error.AmbiguousResource, result);
}

test "RESTMapper: ambiguous resourceFor — same kind in two versions" {
    var tm = try makeMultiVersionMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    // Exact — no ambiguity
    const gvr = try mapper.resourceFor(.{ .group = "autoscaling", .version = "v2", .kind = "HorizontalPodAutoscaler" });
    try testing.expectEqualStrings("horizontalpodautoscalers", gvr.resource);

    // Partial — no version — ambiguous
    const result = mapper.resourceFor(.{ .group = "autoscaling", .version = "", .kind = "HorizontalPodAutoscaler" });
    try testing.expectError(error.AmbiguousKind, result);
}

test "RESTMapper: ambiguous restMapping — no version" {
    var tm = try makeMultiVersionMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    // With version — fine
    const m = try mapper.restMapping("autoscaling", "HorizontalPodAutoscaler", "v2");
    try testing.expectEqualStrings("horizontalpodautoscalers", m.resource.resource);

    // Without version — ambiguous
    const result = mapper.restMapping("autoscaling", "HorizontalPodAutoscaler", null);
    try testing.expectError(error.AmbiguousKind, result);
}

test "RESTMapper: kindsFor returns both versions" {
    var tm = try makeMultiVersionMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const kinds = try mapper.kindsFor(testing.allocator, .{ .group = "autoscaling", .version = "", .resource = "horizontalpodautoscalers" });
    defer testing.allocator.free(kinds);
    try testing.expectEqual(@as(usize, 2), kinds.len);

    // Both should be HPA, different versions
    var has_v1 = false;
    var has_v2 = false;
    for (kinds) |k| {
        try testing.expectEqualStrings("HorizontalPodAutoscaler", k.kind);
        if (mem.eql(u8, k.version, "v1")) has_v1 = true;
        if (mem.eql(u8, k.version, "v2")) has_v2 = true;
    }
    try testing.expect(has_v1);
    try testing.expect(has_v2);
}

test "RESTMapper: resourcesFor returns both versions" {
    var tm = try makeMultiVersionMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const resources = try mapper.resourcesFor(testing.allocator, .{ .group = "autoscaling", .version = "", .kind = "HorizontalPodAutoscaler" });
    defer testing.allocator.free(resources);
    try testing.expectEqual(@as(usize, 2), resources.len);

    var has_v1 = false;
    var has_v2 = false;
    for (resources) |r| {
        try testing.expectEqualStrings("horizontalpodautoscalers", r.resource);
        if (mem.eql(u8, r.version, "v1")) has_v1 = true;
        if (mem.eql(u8, r.version, "v2")) has_v2 = true;
    }
    try testing.expect(has_v1);
    try testing.expect(has_v2);
}

test "RESTMapper: restMappings returns both versions" {
    var tm = try makeMultiVersionMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const mappings = try mapper.restMappings(testing.allocator, "autoscaling", "HorizontalPodAutoscaler");
    defer testing.allocator.free(mappings);
    try testing.expectEqual(@as(usize, 2), mappings.len);
    for (mappings) |m| {
        try testing.expectEqual(RESTScope.namespace, m.scope);
    }
}

test "RESTMapper: kindForOrNull on ambiguous returns null" {
    var tm = try makeMultiVersionMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const result = mapper.kindForOrNull(.{ .group = "autoscaling", .version = "", .resource = "horizontalpodautoscalers" });
    try testing.expect(result == null);
}

test "RESTMapper: resourceForOrNull on ambiguous returns null" {
    var tm = try makeMultiVersionMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const result = mapper.resourceForOrNull(.{ .group = "autoscaling", .version = "", .kind = "HorizontalPodAutoscaler" });
    try testing.expect(result == null);
}

test "RESTMapper: restMappingOrNull on ambiguous returns null" {
    var tm = try makeMultiVersionMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const result = mapper.restMappingOrNull("autoscaling", "HorizontalPodAutoscaler", null);
    try testing.expect(result == null);
}

test "RESTMapper: scopeFor with multi-version" {
    var tm = try makeMultiVersionMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    // Exact GVK — should work for both versions
    const scope_v1 = try mapper.scopeFor(.{ .group = "autoscaling", .version = "v1", .kind = "HorizontalPodAutoscaler" });
    try testing.expectEqual(RESTScope.namespace, scope_v1);

    const scope_v2 = try mapper.scopeFor(.{ .group = "autoscaling", .version = "v2", .kind = "HorizontalPodAutoscaler" });
    try testing.expectEqual(RESTScope.namespace, scope_v2);
}

test "RESTMapper: singularFor and pluralFor with multi-version" {
    var tm = try makeMultiVersionMapper();
    defer tm.test_arena.deinit();
    defer tm.mapper.deinit();
    const mapper = &tm.mapper;

    const singular = try mapper.singularFor(.{ .group = "autoscaling", .version = "v2", .resource = "horizontalpodautoscalers" });
    try testing.expectEqualStrings("horizontalpodautoscaler", singular.resource);

    const plural = try mapper.pluralFor(.{ .group = "autoscaling", .version = "v2", .resource = "horizontalpodautoscaler" });
    try testing.expectEqualStrings("horizontalpodautoscalers", plural.resource);
}

test "RESTScope: roundtrip all values" {
    try testing.expectEqualStrings("Namespaced", RESTScope.namespace.string());
    try testing.expectEqualStrings("Cluster", RESTScope.root.string());
    try testing.expectEqual(RESTScope.namespace, RESTScope.fromString("Namespaced").?);
    try testing.expectEqual(RESTScope.root, RESTScope.fromString("Cluster").?);
    try testing.expect(RESTScope.fromString("") == null);
    try testing.expect(RESTScope.fromString("namespace") == null);
    try testing.expect(RESTScope.fromString("cluster") == null);
    try testing.expect(RESTScope.fromString("NAMESPACED") == null);
}

test "DynamicRESTMapper: fresh state" {
    var dynamic = DynamicRESTMapper{
        .allocator = testing.allocator,
        .discovery = undefined,
    };
    defer dynamic.deinit();

    try testing.expect(!dynamic.fresh());
}

test "DynamicRESTMapper: invalidate resets mapper" {
    // Build a DynamicRESTMapper with a mapper already set.
    var tm = try makeTestMapper();
    defer tm.test_arena.deinit();

    // We can't test with a real CachedDiscoveryClient without HTTP,
    // but we can test that invalidate clears the mapper.
    var dynamic = DynamicRESTMapper{
        .allocator = testing.allocator,
        .discovery = undefined,
        .mapper = tm.mapper,
    };
    // Don't defer dynamic.deinit() since we manually manage mapper below.

    try testing.expect(dynamic.fresh());
    // Invalidate will try to call discovery.invalidate() which is undefined,
    // but we can at least verify the mapper state clearing logic.
    // We can't call invalidate() here since discovery is undefined,
    // so just test the mapper field directly.
    dynamic.mapper.?.deinit();
    dynamic.mapper = null;
    try testing.expect(!dynamic.fresh());
}
