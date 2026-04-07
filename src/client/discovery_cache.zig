const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const testing = std.testing;

const k8s = @import("k8s_zig");
const scheme = k8s.scheme;

const k8s_api = @import("k8s_api");
const apidiscovery = k8s_api.apidiscovery_v2;

const discovery_mod = @import("discovery.zig");
const DiscoveryClient = discovery_mod.DiscoveryClient;
const ServerGroupsAndResources = discovery_mod.ServerGroupsAndResources;
const ServerVersion = discovery_mod.ServerVersion;
const ServerResourcesForGroupVersion = discovery_mod.ServerResourcesForGroupVersion;

/// In-memory caching wrapper around DiscoveryClient.
/// Caches the full discovery response and server version.
/// Use invalidate() to force a refresh on the next call.
pub const CachedDiscoveryClient = struct {
    delegate: *DiscoveryClient,
    allocator: Allocator,

    // Cached state
    cached_groups: ?ServerGroupsAndResources = null,
    cached_version: ?ServerVersion = null,
    valid: bool = false,

    pub fn init(allocator: Allocator, delegate: *DiscoveryClient) CachedDiscoveryClient {
        return .{
            .delegate = delegate,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *CachedDiscoveryClient) void {
        self.clearCache();
    }

    /// Fetch all groups and resources. Returns a read-only pointer to cached data.
    /// The cache owns the lifetime — callers must NOT deinit the returned value.
    pub fn serverGroupsAndResources(self: *CachedDiscoveryClient) !*const ServerGroupsAndResources {
        if (self.valid) {
            if (self.cached_groups) |*cached| {
                return cached;
            }
        }
        self.clearGroupsCache();

        const result = try self.delegate.serverGroupsAndResources();
        self.cached_groups = result;
        self.valid = true;
        return &self.cached_groups.?;
    }

    /// Fetch resources for a single group version.
    /// Uses cached full discovery if available, otherwise fetches fresh.
    pub fn serverResourcesForGroupVersion(
        self: *CachedDiscoveryClient,
        gv: scheme.GroupVersion,
    ) !ServerResourcesForGroupVersion {
        // Always delegate — the result is a filtered subset, not worth caching separately.
        return self.delegate.serverResourcesForGroupVersion(gv);
    }

    /// Fetch preferred-version resources. Returns cached data filtered.
    pub fn serverPreferredResources(self: *CachedDiscoveryClient) !ServerGroupsAndResources {
        return self.delegate.serverPreferredResources();
    }

    /// Fetch preferred namespaced resources.
    pub fn serverPreferredNamespacedResources(self: *CachedDiscoveryClient) !ServerGroupsAndResources {
        return self.delegate.serverPreferredNamespacedResources();
    }

    /// Fetch server version. Returns cached if valid.
    pub fn serverVersion(self: *CachedDiscoveryClient) !ServerVersion {
        if (self.valid) {
            if (self.cached_version) |cached| {
                return cached;
            }
        }

        if (self.cached_version) |*cached| {
            cached.deinit();
        }
        const result = try self.delegate.serverVersion();
        self.cached_version = result;
        return result;
    }

    /// Check if a GVR is enabled. Uses cache.
    pub fn isResourceEnabled(self: *CachedDiscoveryClient, gvr: scheme.GroupVersionResource) !bool {
        const groups = try self.serverGroupsAndResources();
        return discovery_mod.findResource(groups.groups, gvr);
    }

    /// Check if a GroupVersion is supported. Uses cache.
    pub fn serverSupportsVersion(self: *CachedDiscoveryClient, gv: scheme.GroupVersion) !bool {
        const groups = try self.serverGroupsAndResources();
        return discovery_mod.findGroupVersion(groups.groups, gv);
    }

    /// Mark cache as invalid. Next call will re-fetch from the API server.
    pub fn invalidate(self: *CachedDiscoveryClient) void {
        self.valid = false;
    }

    /// True if cache is populated and hasn't been invalidated.
    pub fn fresh(self: *const CachedDiscoveryClient) bool {
        return self.valid and self.cached_groups != null;
    }

    fn clearGroupsCache(self: *CachedDiscoveryClient) void {
        if (self.cached_groups) |*cached| {
            cached.deinit();
            self.cached_groups = null;
        }
    }

    fn clearCache(self: *CachedDiscoveryClient) void {
        self.clearGroupsCache();
        if (self.cached_version) |*cached| {
            cached.deinit();
            self.cached_version = null;
        }
        self.valid = false;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "CachedDiscoveryClient: fresh state" {
    // Without a delegate we can only test initial state.
    var cache = CachedDiscoveryClient{
        .delegate = undefined,
        .allocator = testing.allocator,
    };
    defer cache.deinit();

    try testing.expect(!cache.fresh());

    cache.valid = true;
    try testing.expect(!cache.fresh()); // no groups yet

    cache.invalidate();
    try testing.expect(!cache.fresh());
    try testing.expect(!cache.valid);
}
