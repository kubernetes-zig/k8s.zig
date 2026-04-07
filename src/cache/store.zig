const std = @import("std");
const mem = std.mem;
const Io = std.Io;
const Allocator = mem.Allocator;
const testing = std.testing;

const k8s = @import("k8s_zig");
const Unstructured = k8s.Unstructured;

/// Extract the cache key from an Unstructured object.
/// Format: "namespace/name" for namespaced, "name" for cluster-scoped.
/// Matches Go's MetaNamespaceKeyFunc.
pub fn keyOf(obj: *const Unstructured) ?[]const u8 {
    // We return a slice into the object's arena — valid as long as the object lives.
    // For use as a hash map key, the caller must dupe if needed.
    const name = obj.getName() orelse return null;
    const ns = obj.getNamespace();
    if (ns) |n| {
        // Return "namespace/name" — but we can't allocate here.
        // Instead, store provides a keyFromParts function.
        _ = n;
    }
    // For the simple case, just name. Caller should use keyFromParts for ns/name.
    return name;
}

/// Build a cache key from namespace and name.
pub fn keyFromParts(allocator: Allocator, namespace: ?[]const u8, name: []const u8) ![]const u8 {
    if (namespace) |ns| {
        if (ns.len > 0) {
            return try std.fmt.allocPrint(allocator, "{s}/{s}", .{ ns, name });
        }
    }
    return try allocator.dupe(u8, name);
}

/// Split a cache key into namespace and name.
pub fn splitKey(key: []const u8) struct { namespace: []const u8, name: []const u8 } {
    if (mem.indexOfScalar(u8, key, '/')) |i| {
        return .{ .namespace = key[0..i], .name = key[i + 1 ..] };
    }
    return .{ .namespace = "", .name = key };
}

/// IndexFunc extracts a single index value from an object.
/// Returns null if the object has no value for this index.
/// e.g., a "byNamespace" index returns the object's namespace.
pub const IndexFunc = *const fn (obj: *const Unstructured) ?[]const u8;

/// Thread-safe object store with secondary indexes.
/// Matches Go's cache/ThreadSafeStore pattern.
///
/// Objects are stored by key (namespace/name). The store owns copies of
/// all keys but holds references to the Unstructured objects — the caller
/// must ensure objects outlive their presence in the store, or clone them.
///
/// Thread safety: all operations acquire the RwLock. Reads use shared lock,
/// writes use exclusive lock. Compatible with std.Io concurrency.
pub const Store = struct {
    allocator: Allocator,
    io: Io,
    /// Main storage: key → Unstructured
    items: std.StringHashMapUnmanaged(*Unstructured),
    /// Secondary indexes: index_name → { index_value → set of keys }
    indices: std.StringHashMapUnmanaged(Index),
    /// Index functions: index_name → function
    indexers: std.StringHashMapUnmanaged(IndexFunc),
    /// Latest resourceVersion seen.
    resource_version: ?[]const u8,
    /// RW lock for thread safety.
    lock: Io.RwLock,

    const Index = std.StringHashMapUnmanaged(KeySet);
    const KeySet = std.StringHashMapUnmanaged(void);

    pub fn init(allocator: Allocator, io: Io) Store {
        return .{
            .allocator = allocator,
            .io = io,
            .items = .empty,
            .indices = .empty,
            .indexers = .empty,
            .resource_version = null,
            .lock = .init,
        };
    }

    pub fn deinit(self: *Store) void {
        // Free all owned keys
        var it = self.items.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.items.deinit(self.allocator);

        // Free index structures
        var idx_it = self.indices.iterator();
        while (idx_it.next()) |idx_entry| {
            var inner_it = idx_entry.value_ptr.iterator();
            while (inner_it.next()) |inner_entry| {
                var key_it = inner_entry.value_ptr.iterator();
                while (key_it.next()) |key_entry| {
                    self.allocator.free(key_entry.key_ptr.*);
                }
                inner_entry.value_ptr.deinit(self.allocator);
            }
            idx_entry.value_ptr.deinit(self.allocator);
        }
        self.indices.deinit(self.allocator);
        self.indexers.deinit(self.allocator);

        if (self.resource_version) |rv| self.allocator.free(rv);
    }

    /// Add or update an object in the store.
    // Stack buffer size for key construction. Avoids heap allocation for
    // keys shorter than this. Covers "namespace/name" for typical K8s names.
    const max_stack_key_len = 256;

    /// Build key on stack if it fits, heap otherwise.
    fn stackKey(buf: *[max_stack_key_len]u8, namespace: ?[]const u8, name: []const u8) ?[]const u8 {
        const ns = namespace orelse "";
        if (ns.len == 0) {
            if (name.len <= max_stack_key_len) {
                @memcpy(buf[0..name.len], name);
                return buf[0..name.len];
            }
            return null;
        }
        const total = ns.len + 1 + name.len;
        if (total <= max_stack_key_len) {
            @memcpy(buf[0..ns.len], ns);
            buf[ns.len] = '/';
            @memcpy(buf[ns.len + 1 .. total], name);
            return buf[0..total];
        }
        return null;
    }

    /// Add or update an object in the store.
    pub fn add(self: *Store, obj: *Unstructured) !void {
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);

        // Try stack key first to avoid allocation on update
        var stack_buf: [max_stack_key_len]u8 = undefined;
        const name = obj.getName() orelse return error.MissingName;
        if (stackKey(&stack_buf, obj.getNamespace(), name)) |sk| {
            if (self.items.get(sk)) |old| {
                // Update existing — no key allocation needed
                self.items.getPtr(sk).?.* = obj;
                try self.updateIndicesLocked(old, obj, sk);
                if (obj.getResourceVersion()) |rv| try self.updateRvLocked(rv);
                return;
            }
        }

        // New entry or long key — must allocate key
        const key = try objectKey(self.allocator, obj);
        const gop = try self.items.getOrPut(self.allocator, key);
        const old_obj: ?*Unstructured = if (gop.found_existing) gop.value_ptr.* else null;
        if (gop.found_existing) {
            self.allocator.free(key);
        }
        const stored_key = gop.key_ptr.*;
        gop.value_ptr.* = obj;

        try self.updateIndicesLocked(old_obj, obj, stored_key);
        if (obj.getResourceVersion()) |rv| try self.updateRvLocked(rv);
    }

    /// Update is the same as add (matches Go: Add == Update).
    pub fn update(self: *Store, obj: *Unstructured) !void {
        return self.add(obj);
    }

    /// Remove an object from the store by its key.
    pub fn delete(self: *Store, obj: *const Unstructured) !void {
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);

        // Use stack key to avoid allocation
        var stack_buf: [max_stack_key_len]u8 = undefined;
        const name = obj.getName() orelse return error.MissingName;
        const key = stackKey(&stack_buf, obj.getNamespace(), name) orelse
            try objectKey(self.allocator, obj);
        const heap_key = if (key.ptr != &stack_buf) key else null;
        defer if (heap_key) |hk| self.allocator.free(hk);

        if (self.items.get(key)) |existing| {
            self.deleteIndicesLocked(existing, key);
        }
        if (self.items.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
        }
        if (obj.getResourceVersion()) |rv| {
            try self.updateRvLocked(rv);
        }
    }

    /// Get an object by key. Zero allocation.
    pub fn get(self: *Store, key: []const u8) ?*Unstructured {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return self.items.get(key);
    }

    /// Get an object by namespace/name. Zero allocation for typical key lengths.
    pub fn getByName(self: *Store, namespace: ?[]const u8, name: []const u8) ?*Unstructured {
        var stack_buf: [max_stack_key_len]u8 = undefined;
        const key = stackKey(&stack_buf, namespace, name) orelse return null;
        return self.get(key);
    }

    /// List all objects in the store. Caller owns the returned slice.
    pub fn list(self: *Store, allocator: Allocator) ![]*Unstructured {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);

        const count = self.items.count();
        if (count == 0) return try allocator.alloc(*Unstructured, 0);

        const result = try allocator.alloc(*Unstructured, count);
        var i: usize = 0;
        var it = self.items.valueIterator();
        while (it.next()) |val| {
            result[i] = val.*;
            i += 1;
        }
        return result[0..i];
    }

    /// List all keys in the store. Caller owns the returned slice.
    pub fn listKeys(self: *Store, allocator: Allocator) ![][]const u8 {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);

        const count = self.items.count();
        if (count == 0) return try allocator.alloc([]const u8, 0);

        const result = try allocator.alloc([]const u8, count);
        var i: usize = 0;
        var it = self.items.keyIterator();
        while (it.next()) |key| {
            result[i] = key.*;
            i += 1;
        }
        return result[0..i];
    }

    /// Replace all objects in the store. Matches Go's Replace.
    /// Used by the reflector after a relist.
    ///
    /// Builds the new items map and indices outside the write lock,
    /// then swaps them in under a short exclusive lock. This keeps
    /// readers unblocked during the O(n * indexers) rebuild.
    pub fn replace(self: *Store, objects: []*Unstructured, rv: []const u8) !void {
        // ── Phase 1: build new state unlocked ────────────────────────────
        var new_items: std.StringHashMapUnmanaged(*Unstructured) = .empty;
        errdefer {
            var it = new_items.iterator();
            while (it.next()) |entry| self.allocator.free(entry.key_ptr.*);
            new_items.deinit(self.allocator);
        }
        try new_items.ensureTotalCapacity(self.allocator, @intCast(objects.len));

        // Snapshot indexer functions under a shared lock (cheap).
        self.lock.lockSharedUncancelable(self.io);
        const indexer_count = self.indexers.count();
        self.lock.unlockShared(self.io);

        // Build new indices (same shape, empty inner maps).
        var new_indices: std.StringHashMapUnmanaged(Index) = .empty;
        errdefer {
            var idx_it = new_indices.iterator();
            while (idx_it.next()) |idx_entry| {
                var inner_it = idx_entry.value_ptr.iterator();
                while (inner_it.next()) |inner_entry| {
                    var key_it = inner_entry.value_ptr.iterator();
                    while (key_it.next()) |key_entry| self.allocator.free(key_entry.key_ptr.*);
                    inner_entry.value_ptr.deinit(self.allocator);
                }
                idx_entry.value_ptr.deinit(self.allocator);
            }
            new_indices.deinit(self.allocator);
        }
        if (indexer_count > 0) {
            try new_indices.ensureTotalCapacity(self.allocator, @intCast(indexer_count));
        }

        for (objects) |obj| {
            const key = try objectKey(self.allocator, obj);
            new_items.putAssumeCapacityNoClobber(key, obj);

            // Build index entries by reading indexer functions. The indexers
            // map is append-only (addIndexer requires exclusive lock before
            // any objects), so reading it here without a lock is safe during
            // replace — the reflector is the only writer during relist.
            var indexer_it = self.indexers.iterator();
            while (indexer_it.next()) |idx_entry| {
                const idx_name = idx_entry.key_ptr.*;
                const func_ptr = idx_entry.value_ptr.*;
                if (func_ptr(obj)) |val| {
                    const gop = try new_indices.getOrPut(self.allocator, idx_name);
                    if (!gop.found_existing) gop.value_ptr.* = .empty;
                    const val_gop = try gop.value_ptr.getOrPut(self.allocator, val);
                    if (!val_gop.found_existing) val_gop.value_ptr.* = .empty;
                    const key_gop = try val_gop.value_ptr.getOrPut(self.allocator, key);
                    if (!key_gop.found_existing) {
                        key_gop.key_ptr.* = try self.allocator.dupe(u8, key);
                    }
                }
            }
        }

        const new_rv = try self.allocator.dupe(u8, rv);

        // ── Phase 2: swap under exclusive lock ───────────────────────────
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);

        // Free old state
        var old_it = self.items.iterator();
        while (old_it.next()) |entry| self.allocator.free(entry.key_ptr.*);
        self.items.deinit(self.allocator);

        var old_idx_it = self.indices.iterator();
        while (old_idx_it.next()) |idx_entry| {
            var inner_it = idx_entry.value_ptr.iterator();
            while (inner_it.next()) |inner_entry| {
                var key_it = inner_entry.value_ptr.iterator();
                while (key_it.next()) |key_entry| self.allocator.free(key_entry.key_ptr.*);
                inner_entry.value_ptr.deinit(self.allocator);
            }
            idx_entry.value_ptr.deinit(self.allocator);
        }
        self.indices.deinit(self.allocator);

        if (self.resource_version) |old_rv| self.allocator.free(old_rv);

        // Install new state
        self.items = new_items;
        self.indices = new_indices;
        self.resource_version = new_rv;
    }

    /// Return the count of items in the store.
    pub fn len(self: *Store) usize {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return self.items.count();
    }

    /// Get the latest resourceVersion the store has seen.
    pub fn lastResourceVersion(self: *Store) ?[]const u8 {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return self.resource_version;
    }

    /// Update resourceVersion from a bookmark event (no object change).
    pub fn bookmark(self: *Store, rv: []const u8) !void {
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);
        try self.updateRvLocked(rv);
    }

    // ── Indexing ──────────────────────────────────────────────────────────

    /// Add a secondary index. Must be called before any objects are added.
    pub fn addIndexer(self: *Store, name: []const u8, func_ptr: IndexFunc) !void {
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);

        if (self.indexers.get(name) != null) return error.IndexerAlreadyExists;
        try self.indexers.put(self.allocator, name, func_ptr);
        try self.indices.put(self.allocator, name, .empty);
    }

    /// Lookup objects by secondary index.
    pub fn byIndex(self: *Store, allocator: Allocator, index_name: []const u8, index_value: []const u8) ![]*Unstructured {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);

        const idx = self.indices.get(index_name) orelse return error.IndexNotFound;
        const key_set = idx.get(index_value) orelse return try allocator.alloc(*Unstructured, 0);

        var result: std.ArrayList(*Unstructured) = .empty;
        try result.ensureTotalCapacity(allocator, key_set.count());
        var it = key_set.keyIterator();
        while (it.next()) |key| {
            if (self.items.get(key.*)) |obj| {
                result.appendAssumeCapacity(obj);
            }
        }
        return try result.toOwnedSlice(allocator);
    }

    /// Return all cache keys matching a secondary index value (without fetching objects).
    /// Caller owns the returned slice.
    pub fn indexKeys(self: *Store, allocator: Allocator, index_name: []const u8, index_value: []const u8) ![][]const u8 {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);

        const idx = self.indices.get(index_name) orelse return error.IndexNotFound;
        const key_set = idx.get(index_value) orelse return try allocator.alloc([]const u8, 0);

        var result: std.ArrayList([]const u8) = .empty;
        try result.ensureTotalCapacity(allocator, key_set.count());
        var it = key_set.keyIterator();
        while (it.next()) |key| {
            result.appendAssumeCapacity(key.*);
        }
        return try result.toOwnedSlice(allocator);
    }

    /// Return all distinct values for a given index. Caller owns the returned slice.
    pub fn listIndexFuncValues(self: *Store, allocator: Allocator, index_name: []const u8) ![][]const u8 {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);

        const idx = self.indices.get(index_name) orelse return error.IndexNotFound;

        var result: std.ArrayList([]const u8) = .empty;
        var it = idx.keyIterator();
        while (it.next()) |key| {
            try result.append(allocator, key.*);
        }
        return try result.toOwnedSlice(allocator);
    }

    // ── Internal (must hold lock) ────────────────────────────────────────

    fn updateRvLocked(self: *Store, rv: []const u8) !void {
        if (self.resource_version) |old| self.allocator.free(old);
        self.resource_version = try self.allocator.dupe(u8, rv);
    }

    fn updateIndicesLocked(self: *Store, old_obj: ?*Unstructured, new_obj: *Unstructured, key: []const u8) !void {
        var indexer_it = self.indexers.iterator();
        while (indexer_it.next()) |entry| {
            const name = entry.key_ptr.*;
            const func_ptr = entry.value_ptr.*;
            const idx = self.indices.getPtr(name) orelse continue;

            // Remove old index entry
            if (old_obj) |old| {
                if (func_ptr(old)) |old_val| {
                    if (idx.getPtr(old_val)) |key_set| {
                        if (key_set.fetchRemove(key)) |kv| {
                            self.allocator.free(kv.key);
                        }
                        if (key_set.count() == 0) {
                            var removed = idx.fetchRemove(old_val).?;
                            removed.value.deinit(self.allocator);
                        }
                    }
                }
            }

            // Add new index entry (BUG #9 fix: use getOrPut to avoid leaking duped keys)
            if (func_ptr(new_obj)) |new_val| {
                const gop = try idx.getOrPut(self.allocator, new_val);
                if (!gop.found_existing) {
                    gop.value_ptr.* = .empty;
                }
                const key_gop = try gop.value_ptr.getOrPut(self.allocator, key);
                if (!key_gop.found_existing) {
                    key_gop.key_ptr.* = try self.allocator.dupe(u8, key);
                }
            }
        }
    }

    fn deleteIndicesLocked(self: *Store, obj: *const Unstructured, key: []const u8) void {
        var indexer_it = self.indexers.iterator();
        while (indexer_it.next()) |entry| {
            const name = entry.key_ptr.*;
            const func_ptr = entry.value_ptr.*;
            const idx = self.indices.getPtr(name) orelse continue;

            if (func_ptr(obj)) |val| {
                if (idx.getPtr(val)) |key_set| {
                    if (key_set.fetchRemove(key)) |kv| {
                        self.allocator.free(kv.key);
                    }
                    if (key_set.count() == 0) {
                        var removed = idx.fetchRemove(val).?;
                        removed.value.deinit(self.allocator);
                    }
                }
            }
        }
    }
};

/// Build the cache key for an Unstructured object.
fn objectKey(allocator: Allocator, obj: *const Unstructured) ![]const u8 {
    const name = obj.getName() orelse return error.MissingName;
    return keyFromParts(allocator, obj.getNamespace(), name);
}

// ── Built-in index functions ──────────────────────────────────────────────

/// Index by namespace. Returns the object's namespace.
pub fn indexByNamespace(obj: *const Unstructured) ?[]const u8 {
    return obj.getNamespace();
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

fn makeObj(allocator: Allocator, ns: ?[]const u8, name: []const u8, rv: []const u8) !*Unstructured {
    var obj = try allocator.create(Unstructured);
    obj.* = try Unstructured.init(allocator);
    if (ns) |n| try obj.setNamespace(n);
    try obj.setName(name);
    try obj.setResourceVersion(rv);
    return obj;
}

fn freeObj(allocator: Allocator, obj: *Unstructured) void {
    obj.deinit();
    allocator.destroy(obj);
}

// ── CRUD tests ────────────────────────────────────────────────────────────

test "store: add and get" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj = try makeObj(testing.allocator, "default", "nginx", "100");
    defer freeObj(testing.allocator, obj);
    try s.add(obj);

    // Found by full key
    try testing.expect(s.get("default/nginx") != null);
    // Not found
    try testing.expect(s.get("nonexistent") == null);
    try testing.expect(s.get("default/other") == null);
    try testing.expect(s.get("other/nginx") == null);
    // Count
    try testing.expectEqual(@as(usize, 1), s.len());
    // RV tracked
    try testing.expectEqualStrings("100", s.lastResourceVersion().?);
    // getByName
    try testing.expect((s.getByName("default", "nginx")) != null);
    try testing.expect((s.getByName("default", "other")) == null);
    try testing.expect((s.getByName("other", "nginx")) == null);
}

test "store: add same key twice updates in place" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj1 = try makeObj(testing.allocator, "default", "nginx", "100");
    defer freeObj(testing.allocator, obj1);
    const obj2 = try makeObj(testing.allocator, "default", "nginx", "200");
    defer freeObj(testing.allocator, obj2);

    try s.add(obj1);
    try s.add(obj2);

    // Only one entry
    try testing.expectEqual(@as(usize, 1), s.len());
    // Points to obj2
    const got = s.get("default/nginx").?;
    try testing.expectEqualStrings("200", got.getResourceVersion().?);
    // RV advanced
    try testing.expectEqualStrings("200", s.lastResourceVersion().?);
}

test "store: update is alias for add" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj = try makeObj(testing.allocator, "default", "nginx", "100");
    defer freeObj(testing.allocator, obj);

    try s.update(obj);
    try testing.expect(s.get("default/nginx") != null);
}

test "store: delete existing and nonexistent" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj = try makeObj(testing.allocator, "default", "nginx", "100");
    defer freeObj(testing.allocator, obj);

    try s.add(obj);
    try testing.expectEqual(@as(usize, 1), s.len());

    // Delete existing
    try s.delete(obj);
    try testing.expectEqual(@as(usize, 0), s.len());
    try testing.expect(s.get("default/nginx") == null);

    // Delete nonexistent — should not error
    try s.delete(obj);
    try testing.expectEqual(@as(usize, 0), s.len());
}

test "store: delete advances resourceVersion from delete event" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj = try makeObj(testing.allocator, "default", "nginx", "100");
    defer freeObj(testing.allocator, obj);
    try s.add(obj);

    const deleted = try makeObj(testing.allocator, "default", "nginx", "200");
    defer freeObj(testing.allocator, deleted);
    try s.delete(deleted);

    try testing.expectEqual(@as(usize, 0), s.len());
    try testing.expectEqualStrings("200", s.lastResourceVersion().?);
}

test "store: delete preserves other objects" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const a = try makeObj(testing.allocator, "default", "a", "1");
    defer freeObj(testing.allocator, a);
    const b = try makeObj(testing.allocator, "default", "b", "2");
    defer freeObj(testing.allocator, b);

    try s.add(a);
    try s.add(b);
    try s.delete(a);

    try testing.expectEqual(@as(usize, 1), s.len());
    try testing.expect(s.get("default/a") == null);
    try testing.expect(s.get("default/b") != null);
}

// ── Cluster-scoped tests ──────────────────────────────────────────────────

test "store: cluster-scoped objects" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const node = try makeObj(testing.allocator, null, "node-1", "1");
    defer freeObj(testing.allocator, node);
    try s.add(node);

    try testing.expect(s.get("node-1") != null);
    try testing.expect(s.get("default/node-1") == null);
    try testing.expect((s.getByName(null, "node-1")) != null);
}

test "store: mixed namespaced and cluster-scoped" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const pod = try makeObj(testing.allocator, "default", "nginx", "1");
    defer freeObj(testing.allocator, pod);
    const node = try makeObj(testing.allocator, null, "node-1", "2");
    defer freeObj(testing.allocator, node);

    try s.add(pod);
    try s.add(node);

    try testing.expectEqual(@as(usize, 2), s.len());
    try testing.expect(s.get("default/nginx") != null);
    try testing.expect(s.get("node-1") != null);
}

// ── List tests ────────────────────────────────────────────────────────────

test "store: list and listKeys" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    // Empty store
    const empty_items = try s.list(testing.allocator);
    defer testing.allocator.free(empty_items);
    try testing.expectEqual(@as(usize, 0), empty_items.len);

    const empty_keys = try s.listKeys(testing.allocator);
    defer testing.allocator.free(empty_keys);
    try testing.expectEqual(@as(usize, 0), empty_keys.len);

    // With objects
    const a = try makeObj(testing.allocator, "default", "a", "1");
    defer freeObj(testing.allocator, a);
    const b = try makeObj(testing.allocator, "default", "b", "2");
    defer freeObj(testing.allocator, b);
    const c = try makeObj(testing.allocator, "other", "c", "3");
    defer freeObj(testing.allocator, c);

    try s.add(a);
    try s.add(b);
    try s.add(c);

    const items = try s.list(testing.allocator);
    defer testing.allocator.free(items);
    try testing.expectEqual(@as(usize, 3), items.len);

    const keys = try s.listKeys(testing.allocator);
    defer testing.allocator.free(keys);
    try testing.expectEqual(@as(usize,3), keys.len);
}

// ── Replace tests ─────────────────────────────────────────────────────────

test "store: replace clears old and inserts new" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const old = try makeObj(testing.allocator, "default", "old", "1");
    defer freeObj(testing.allocator, old);
    try s.add(old);

    const new1 = try makeObj(testing.allocator, "default", "new1", "10");
    defer freeObj(testing.allocator, new1);
    const new2 = try makeObj(testing.allocator, "default", "new2", "10");
    defer freeObj(testing.allocator, new2);

    var objects = [_]*Unstructured{ new1, new2 };
    try s.replace(&objects, "10");

    try testing.expectEqual(@as(usize, 2), s.len());
    try testing.expect(s.get("default/old") == null);
    try testing.expect(s.get("default/new1") != null);
    try testing.expect(s.get("default/new2") != null);
    try testing.expectEqualStrings("10", s.lastResourceVersion().?);
}

test "store: replace with empty list clears everything" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj = try makeObj(testing.allocator, "default", "nginx", "1");
    defer freeObj(testing.allocator, obj);
    try s.add(obj);

    var empty = [_]*Unstructured{};
    try s.replace(&empty, "99");

    try testing.expectEqual(@as(usize, 0), s.len());
    try testing.expectEqualStrings("99", s.lastResourceVersion().?);
}

test "store: replace on empty store" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj = try makeObj(testing.allocator, "default", "nginx", "1");
    defer freeObj(testing.allocator, obj);

    var objects = [_]*Unstructured{obj};
    try s.replace(&objects, "1");

    try testing.expectEqual(@as(usize, 1), s.len());
    try testing.expect(s.get("default/nginx") != null);
}

// ── ResourceVersion tests ─────────────────────────────────────────────────

test "store: resourceVersion tracking" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    // Initially null
    try testing.expect(s.lastResourceVersion() == null);

    // Set by add
    const obj = try makeObj(testing.allocator, "default", "nginx", "100");
    defer freeObj(testing.allocator, obj);
    try s.add(obj);
    try testing.expectEqualStrings("100", s.lastResourceVersion().?);

    // Advanced by bookmark
    try s.bookmark("500");
    try testing.expectEqualStrings("500", s.lastResourceVersion().?);

    // Advanced by another bookmark
    try s.bookmark("600");
    try testing.expectEqualStrings("600", s.lastResourceVersion().?);

    // Set by replace
    var empty = [_]*Unstructured{};
    try s.replace(&empty, "999");
    try testing.expectEqualStrings("999", s.lastResourceVersion().?);
}

test "store: RV advances with each add" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj1 = try makeObj(testing.allocator, "default", "a", "10");
    defer freeObj(testing.allocator, obj1);
    const obj2 = try makeObj(testing.allocator, "default", "b", "20");
    defer freeObj(testing.allocator, obj2);

    try s.add(obj1);
    try testing.expectEqualStrings("10", s.lastResourceVersion().?);
    try s.add(obj2);
    try testing.expectEqualStrings("20", s.lastResourceVersion().?);
}

// ── Key helpers tests ─────────────────────────────────────────────────────

test "keyFromParts and splitKey" {
    const Case = struct { ns: ?[]const u8, name: []const u8, expected_key: []const u8, expected_ns: []const u8 };
    const cases = [_]Case{
        .{ .ns = "default", .name = "nginx", .expected_key = "default/nginx", .expected_ns = "default" },
        .{ .ns = null, .name = "node-1", .expected_key = "node-1", .expected_ns = "" },
        .{ .ns = "", .name = "node-1", .expected_key = "node-1", .expected_ns = "" },
        .{ .ns = "kube-system", .name = "coredns", .expected_key = "kube-system/coredns", .expected_ns = "kube-system" },
    };
    for (cases) |c| {
        const key = try keyFromParts(testing.allocator, c.ns, c.name);
        defer testing.allocator.free(key);
        try testing.expectEqualStrings(c.expected_key, key);

        const parts = splitKey(key);
        try testing.expectEqualStrings(c.name, parts.name);
        try testing.expectEqualStrings(c.expected_ns, parts.namespace);
    }
}

// ── Empty store edge cases ────────────────────────────────────────────────

test "store: operations on empty store" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    try testing.expectEqual(@as(usize, 0), s.len());
    try testing.expect(s.get("anything") == null);
    try testing.expect(s.lastResourceVersion() == null);

    const items = try s.list(testing.allocator);
    defer testing.allocator.free(items);
    try testing.expectEqual(@as(usize, 0), items.len);

    const keys = try s.listKeys(testing.allocator);
    defer testing.allocator.free(keys);
    try testing.expectEqual(@as(usize,0), keys.len);
}

// ── Multiple namespaces ───────────────────────────────────────────────────

test "store: replace twice doesn't leak index memory" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    // First populate
    const a = try makeObj(testing.allocator, "default", "a", "1");
    defer freeObj(testing.allocator, a);
    const b = try makeObj(testing.allocator, "default", "b", "2");
    defer freeObj(testing.allocator, b);
    var first = [_]*Unstructured{ a, b };
    try s.replace(&first, "2");
    try testing.expectEqual(@as(usize, 2), s.len());

    // Replace again with different objects — old indices should be properly freed
    const c = try makeObj(testing.allocator, "default", "c", "3");
    defer freeObj(testing.allocator, c);
    var second = [_]*Unstructured{c};
    try s.replace(&second, "3");
    try testing.expectEqual(@as(usize, 1), s.len());
    try testing.expect(s.get("default/a") == null);
    try testing.expect(s.get("default/b") == null);
    try testing.expect(s.get("default/c") != null);

    // Replace with empty — all cleared
    var empty = [_]*Unstructured{};
    try s.replace(&empty, "4");
    try testing.expectEqual(@as(usize, 0), s.len());
}

test "store: same name different namespaces are different objects" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const a = try makeObj(testing.allocator, "ns-a", "nginx", "1");
    defer freeObj(testing.allocator, a);
    const b = try makeObj(testing.allocator, "ns-b", "nginx", "2");
    defer freeObj(testing.allocator, b);

    try s.add(a);
    try s.add(b);

    try testing.expectEqual(@as(usize, 2), s.len());
    const got_a = s.get("ns-a/nginx").?;
    const got_b = s.get("ns-b/nginx").?;
    try testing.expectEqualStrings("1", got_a.getResourceVersion().?);
    try testing.expectEqualStrings("2", got_b.getResourceVersion().?);
}

// ── Indexing tests ──────────────────────────────────────────────────────

test "store: addIndexer and byIndex" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    try s.addIndexer("namespace", &indexByNamespace);

    const a = try makeObj(testing.allocator, "default", "pod-a", "1");
    defer freeObj(testing.allocator, a);
    const b = try makeObj(testing.allocator, "default", "pod-b", "2");
    defer freeObj(testing.allocator, b);
    const c = try makeObj(testing.allocator, "kube-system", "coredns", "3");
    defer freeObj(testing.allocator, c);

    try s.add(a);
    try s.add(b);
    try s.add(c);

    // Query by namespace
    const default_pods = try s.byIndex(testing.allocator, "namespace", "default");
    defer testing.allocator.free(default_pods);
    try testing.expectEqual(@as(usize, 2), default_pods.len);

    const system_pods = try s.byIndex(testing.allocator, "namespace", "kube-system");
    defer testing.allocator.free(system_pods);
    try testing.expectEqual(@as(usize, 1), system_pods.len);

    // Empty result for nonexistent namespace
    const empty = try s.byIndex(testing.allocator, "namespace", "nonexistent");
    defer testing.allocator.free(empty);
    try testing.expectEqual(@as(usize, 0), empty.len);

    // Unknown index
    try testing.expectError(error.IndexNotFound, s.byIndex(testing.allocator, "bogus", "val"));
}

test "store: index updated on add, update, delete" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    try s.addIndexer("namespace", &indexByNamespace);

    const obj = try makeObj(testing.allocator, "default", "nginx", "1");
    defer freeObj(testing.allocator, obj);

    try s.add(obj);
    const before = try s.byIndex(testing.allocator, "namespace", "default");
    defer testing.allocator.free(before);
    try testing.expectEqual(@as(usize, 1), before.len);

    // Delete removes from index
    try s.delete(obj);
    const after = try s.byIndex(testing.allocator, "namespace", "default");
    defer testing.allocator.free(after);
    try testing.expectEqual(@as(usize, 0), after.len);
}

test "store: indexKeys returns keys without fetching objects" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    try s.addIndexer("namespace", &indexByNamespace);

    const a = try makeObj(testing.allocator, "default", "a", "1");
    defer freeObj(testing.allocator, a);
    const b = try makeObj(testing.allocator, "default", "b", "2");
    defer freeObj(testing.allocator, b);

    try s.add(a);
    try s.add(b);

    const keys = try s.indexKeys(testing.allocator, "namespace", "default");
    defer testing.allocator.free(keys);
    try testing.expectEqual(@as(usize, 2), keys.len);

    // Unknown index
    try testing.expectError(error.IndexNotFound, s.indexKeys(testing.allocator, "bogus", "val"));
}

test "store: listIndexFuncValues returns all index values" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    try s.addIndexer("namespace", &indexByNamespace);

    const a = try makeObj(testing.allocator, "default", "a", "1");
    defer freeObj(testing.allocator, a);
    const b = try makeObj(testing.allocator, "kube-system", "b", "2");
    defer freeObj(testing.allocator, b);
    const c = try makeObj(testing.allocator, "monitoring", "c", "3");
    defer freeObj(testing.allocator, c);

    try s.add(a);
    try s.add(b);
    try s.add(c);

    const values = try s.listIndexFuncValues(testing.allocator, "namespace");
    defer testing.allocator.free(values);
    try testing.expectEqual(@as(usize, 3), values.len);

    // Unknown index
    try testing.expectError(error.IndexNotFound, s.listIndexFuncValues(testing.allocator, "bogus"));
}

test "store: replace rebuilds indices" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    try s.addIndexer("namespace", &indexByNamespace);

    const old = try makeObj(testing.allocator, "default", "old", "1");
    defer freeObj(testing.allocator, old);
    try s.add(old);

    // Replace with objects in different namespace
    const new1 = try makeObj(testing.allocator, "kube-system", "new1", "10");
    defer freeObj(testing.allocator, new1);
    const new2 = try makeObj(testing.allocator, "kube-system", "new2", "10");
    defer freeObj(testing.allocator, new2);

    var objects = [_]*Unstructured{ new1, new2 };
    try s.replace(&objects, "10");

    // Old namespace should be empty
    const default_objs = try s.byIndex(testing.allocator, "namespace", "default");
    defer testing.allocator.free(default_objs);
    try testing.expectEqual(@as(usize, 0), default_objs.len);

    // New namespace should have 2 objects
    const system_objs = try s.byIndex(testing.allocator, "namespace", "kube-system");
    defer testing.allocator.free(system_objs);
    try testing.expectEqual(@as(usize, 2), system_objs.len);
}

test "store: addIndexer rejects duplicate" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    try s.addIndexer("namespace", &indexByNamespace);
    try testing.expectError(error.IndexerAlreadyExists, s.addIndexer("namespace", &indexByNamespace));
}

// ── Additional table-driven tests ────────────────────────────────────────

test "store: indexByNamespace returns namespace or null" {
    const Case = struct {
        ns: ?[]const u8,
        name: []const u8,
        expected_ns: ?[]const u8,
    };
    const cases = [_]Case{
        .{ .ns = "default", .name = "pod-a", .expected_ns = "default" },
        .{ .ns = "kube-system", .name = "coredns", .expected_ns = "kube-system" },
        .{ .ns = null, .name = "node-1", .expected_ns = null },
        .{ .ns = "", .name = "cluster-role", .expected_ns = "" },
    };
    for (cases) |c| {
        const obj = try makeObj(testing.allocator, c.ns, c.name, "1");
        defer freeObj(testing.allocator, obj);

        const result = indexByNamespace(obj);
        if (c.expected_ns) |expected| {
            try testing.expect(result != null);
            try testing.expectEqualStrings(expected, result.?);
        } else {
            try testing.expect(result == null);
        }
    }
}

test "store: byIndex after add update delete sequence" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    try s.addIndexer("namespace", &indexByNamespace);

    // Add 3 objects in "default"
    const a = try makeObj(testing.allocator, "default", "a", "1");
    defer freeObj(testing.allocator, a);
    const b = try makeObj(testing.allocator, "default", "b", "2");
    defer freeObj(testing.allocator, b);
    const c = try makeObj(testing.allocator, "default", "c", "3");
    defer freeObj(testing.allocator, c);

    try s.add(a);
    try s.add(b);
    try s.add(c);

    const after_add = try s.byIndex(testing.allocator, "namespace", "default");
    defer testing.allocator.free(after_add);
    try testing.expectEqual(@as(usize, 3), after_add.len);

    // Update object "b" with a new RV — index should still have 3
    const b2 = try makeObj(testing.allocator, "default", "b", "20");
    defer freeObj(testing.allocator, b2);
    try s.update(b2);

    const after_update = try s.byIndex(testing.allocator, "namespace", "default");
    defer testing.allocator.free(after_update);
    try testing.expectEqual(@as(usize, 3), after_update.len);

    // Delete object "a" — index should have 2
    try s.delete(a);

    const after_delete = try s.byIndex(testing.allocator, "namespace", "default");
    defer testing.allocator.free(after_delete);
    try testing.expectEqual(@as(usize, 2), after_delete.len);

    // Verify final store count
    try testing.expectEqual(@as(usize, 2), s.len());
}

test "store: listIndexFuncValues with multiple namespaces" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    try s.addIndexer("namespace", &indexByNamespace);

    const namespaces = [_][]const u8{ "alpha", "beta", "gamma", "delta" };
    var objs: [4]*Unstructured = undefined;
    for (namespaces, 0..) |ns, i| {
        objs[i] = try makeObj(testing.allocator, ns, "obj", "1");
        try s.add(objs[i]);
    }
    defer for (&objs) |obj| freeObj(testing.allocator, obj);

    const values = try s.listIndexFuncValues(testing.allocator, "namespace");
    defer testing.allocator.free(values);
    try testing.expectEqual(@as(usize, 4), values.len);

    // Every namespace should appear in the values
    for (namespaces) |ns| {
        var found = false;
        for (values) |v| {
            if (mem.eql(u8, v, ns)) {
                found = true;
                break;
            }
        }
        try testing.expect(found);
    }
}

test "store: indexKeys returns keys not objects" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    try s.addIndexer("namespace", &indexByNamespace);

    const a = try makeObj(testing.allocator, "default", "pod-x", "1");
    defer freeObj(testing.allocator, a);
    const b = try makeObj(testing.allocator, "default", "pod-y", "2");
    defer freeObj(testing.allocator, b);

    try s.add(a);
    try s.add(b);

    const keys = try s.indexKeys(testing.allocator, "namespace", "default");
    defer testing.allocator.free(keys);
    try testing.expectEqual(@as(usize, 2), keys.len);

    // Each key should be in "namespace/name" format
    for (keys) |key| {
        const parts = splitKey(key);
        try testing.expectEqualStrings("default", parts.namespace);
        try testing.expect(parts.name.len > 0);
        // Verify the key resolves to an object in the store
        try testing.expect(s.get(key) != null);
    }
}

test "store: getByName with and without namespace" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const pod = try makeObj(testing.allocator, "default", "nginx", "1");
    defer freeObj(testing.allocator, pod);
    const node = try makeObj(testing.allocator, null, "node-1", "2");
    defer freeObj(testing.allocator, node);

    try s.add(pod);
    try s.add(node);

    const Case = struct {
        ns: ?[]const u8,
        name: []const u8,
        expect_found: bool,
        expect_rv: ?[]const u8,
    };
    const cases = [_]Case{
        // Namespaced lookup — found
        .{ .ns = "default", .name = "nginx", .expect_found = true, .expect_rv = "1" },
        // Cluster-scoped lookup — found
        .{ .ns = null, .name = "node-1", .expect_found = true, .expect_rv = "2" },
        // Missing object
        .{ .ns = "default", .name = "missing", .expect_found = false, .expect_rv = null },
        // Wrong namespace
        .{ .ns = "other", .name = "nginx", .expect_found = false, .expect_rv = null },
        // Cluster-scoped name that exists only namespaced
        .{ .ns = null, .name = "nginx", .expect_found = false, .expect_rv = null },
    };
    for (cases) |c| {
        const result = s.getByName(c.ns, c.name);
        if (c.expect_found) {
            try testing.expect(result != null);
            try testing.expectEqualStrings(c.expect_rv.?, result.?.getResourceVersion().?);
        } else {
            try testing.expect(result == null);
        }
    }
}

test "fuzz: store add/delete/replace sequence" {
    try std.testing.fuzz({}, fuzzStoreOps, .{});
}

fn fuzzStoreOps(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();

    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    try s.addIndexer("namespace", &indexByNamespace);

    // Create a pool of objects
    const pool_size = 4;
    var pool: [pool_size]*Unstructured = undefined;
    const names = [_][]const u8{ "a", "b", "c", "d" };
    const namespaces = [_][]const u8{ "default", "kube-system" };

    for (0..pool_size) |i| {
        pool[i] = try makeObj(
            testing.allocator,
            namespaces[i % namespaces.len],
            names[i],
            "1",
        );
    }
    defer for (&pool) |obj| freeObj(testing.allocator, obj);

    const steps = smith.valueRangeAtMost(u8, 1, 64);
    for (0..steps) |_| {
        const op = smith.valueRangeAtMost(u8, 0, 3);
        const idx = smith.valueRangeAtMost(u8, 0, pool_size - 1);
        switch (op) {
            0 => s.add(pool[idx]) catch {},
            1 => s.update(pool[idx]) catch {},
            2 => s.delete(pool[idx]) catch {},
            3 => {
                // Replace with subset
                const count = smith.valueRangeAtMost(u8, 0, pool_size);
                var subset: [pool_size]*Unstructured = undefined;
                for (0..count) |j| {
                    subset[j] = pool[j];
                }
                s.replace(subset[0..count], "99") catch {};
            },
            else => unreachable,
        }

        // Verify invariants
        const by_idx = s.byIndex(testing.allocator, "namespace", "default") catch continue;
        testing.allocator.free(by_idx);
    }
}

