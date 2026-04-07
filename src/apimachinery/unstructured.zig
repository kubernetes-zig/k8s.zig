const std = @import("std");
const mem = std.mem;
const json = std.json;
const testing = std.testing;
const Allocator = mem.Allocator;
const scheme = @import("scheme.zig");

/// Unstructured represents a Kubernetes object as a dynamic JSON tree.
/// It wraps std.json.Value and provides navigation, mutation, and traversal.
///
/// All parsed data is owned by an internal arena allocator. Call `deinit()`
/// to free everything at once.
pub const Unstructured = struct {
    arena: *std.heap.ArenaAllocator,
    data: json.ObjectMap,
    allocator: Allocator,

    /// Parse JSON bytes into an Unstructured object.
    /// The returned object owns all parsed data via an internal arena.
    pub fn fromJson(allocator: Allocator, bytes: []const u8) !Unstructured {
        const arena = try allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(allocator);
        errdefer {
            arena.deinit();
            allocator.destroy(arena);
        }
        const parsed = try json.parseFromSliceLeaky(json.Value, arena.allocator(), bytes, .{});
        if (parsed != .object) return error.NotAnObject;
        return .{
            .arena = arena,
            .data = parsed.object,
            .allocator = allocator,
        };
    }

    /// Create an empty Unstructured object.
    pub fn init(allocator: Allocator) !Unstructured {
        const arena = try allocator.create(std.heap.ArenaAllocator);
        errdefer allocator.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(allocator);
        return .{
            .arena = arena,
            .data = json.ObjectMap.init(arena.allocator()),
            .allocator = allocator,
        };
    }

    /// Create from a pre-parsed json.Value (must be an object).
    /// Deep-copies the value into a new arena — the source value can be freed after.
    pub fn fromJsonValue(allocator: Allocator, value: json.Value) !Unstructured {
        if (value != .object) return error.NotAnObject;
        const arena = try allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(allocator);
        errdefer {
            arena.deinit();
            allocator.destroy(arena);
        }
        const duped = try dupeValue(arena.allocator(), value);
        return .{
            .arena = arena,
            .data = duped.object,
            .allocator = allocator,
        };
    }

    /// Free all memory owned by this Unstructured object.
    pub fn deinit(self: *Unstructured) void {
        self.arena.deinit();
        self.allocator.destroy(self.arena);
    }

    // ── Navigation (Nav pattern) ──────────────────────────────────────────

    /// Begin navigating from the root object.
    pub fn field(self: *const Unstructured, key: []const u8) Nav {
        return Nav.fromObject(&self.data, key);
    }

    // ── Mutation (segment paths) ──────────────────────────────────────────

    /// Set a value at the given path, creating intermediate objects as needed.
    /// Path segments are object keys. Use `setAt` for array index mutation.
    pub fn set(self: *Unstructured, path: []const []const u8, value: json.Value) !void {
        if (path.len == 0) return;
        const alloc = self.arena.allocator();

        var current = &self.data;
        for (path[0 .. path.len - 1]) |key| {
            const duped_key = try alloc.dupe(u8, key);
            const gop = try current.getOrPut(duped_key);
            if (!gop.found_existing or gop.value_ptr.* != .object) {
                gop.value_ptr.* = .{ .object = json.ObjectMap.init(alloc)
 };
            }
            current = &gop.value_ptr.object;
        }
        const final_key = try alloc.dupe(u8, path[path.len - 1]);
        const gop = try current.getOrPut(final_key);
        gop.value_ptr.* = try dupeValue(alloc, value);
    }

    /// Set a string value at the given path.
    pub fn setString(self: *Unstructured, path: []const []const u8, value: []const u8) !void {
        const duped = try self.arena.allocator().dupe(u8, value);
        try self.set(path, .{ .string = duped });
    }

    /// Set an explicit null at the given path.
    pub fn setNull(self: *Unstructured, path: []const []const u8) !void {
        try self.set(path, .null);
    }

    /// Remove a key at the given path. Returns true if the key existed.
    pub fn remove(self: *Unstructured, path: []const []const u8) bool {
        if (path.len == 0) return false;

        var current = &self.data;
        for (path[0 .. path.len - 1]) |key| {
            const val = current.get(key) orelse return false;
            if (val != .object) return false;
            current = &current.getPtr(key).?.object;
        }
        return current.orderedRemove(path[path.len - 1]);
    }

    /// Append a value to an array at the given path.
    /// Creates the array if it doesn't exist.
    pub fn append(self: *Unstructured, path: []const []const u8, value: json.Value) !void {
        if (path.len == 0) return error.TypeMismatch;
        const alloc = self.arena.allocator();

        var current = &self.data;
        // Navigate intermediate segments, creating objects as needed.
        for (path[0 .. path.len - 1]) |key| {
            const duped_key = try alloc.dupe(u8, key);
            const gop = try current.getOrPut(duped_key);
            if (!gop.found_existing) {
                gop.value_ptr.* = .{ .object = json.ObjectMap.init(alloc) };
            }
            switch (gop.value_ptr.*) {
                .object => current = &gop.value_ptr.object,
                else => return error.TypeMismatch,
            }
        }

        // Handle the last segment: append to existing array or create one.
        const last_key = try alloc.dupe(u8, path[path.len - 1]);
        const gop = try current.getOrPut(last_key);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .array = json.Array.init(alloc) };
        }
        switch (gop.value_ptr.*) {
            .array => {
                const duped = try dupeValue(alloc, value);
                try gop.value_ptr.array.append(duped);
            },
            else => return error.TypeMismatch,
        }
    }

    /// Replace an array element at the given path and index.
    pub fn setAt(self: *Unstructured, path: []const []const u8, index: usize, value: json.Value) !void {
        const alloc = self.arena.allocator();
        const nav_val = self.fieldPath(path);
        if (nav_val) |v| {
            if (v.* == .array) {
                if (index >= v.array.items.len) return error.IndexOutOfBounds;
                v.array.items[index] = try dupeValue(alloc, value);
                return;
            }
        }
        return error.TypeMismatch;
    }

    /// Remove an array element at the given path and index.
    pub fn removeAt(self: *Unstructured, path: []const []const u8, index: usize) !void {
        const nav_val = self.fieldPath(path);
        if (nav_val) |v| {
            if (v.* == .array) {
                if (index >= v.array.items.len) return error.IndexOutOfBounds;
                _ = v.array.orderedRemove(index);
                return;
            }
        }
        return error.TypeMismatch;
    }

    // ── Serialization ─────────────────────────────────────────────────────

    /// Serialize to JSON bytes. Caller owns the returned slice.
    pub fn toJson(self: *const Unstructured, allocator: Allocator) ![]u8 {
        const root = json.Value{ .object = self.data };
        return try json.Stringify.valueAlloc(allocator, root, .{});
    }

    // ── Deep operations ───────────────────────────────────────────────────

    /// Deep merge another Unstructured into this one.
    /// Objects are merged recursively: keys from `other` are added or overwrite
    /// existing keys. Arrays and scalars from `other` replace the value in self.
    /// This matches K8s strategic merge patch semantics for objects.
    pub fn merge(self: *Unstructured, other: *const Unstructured) !void {
        try mergeObjects(self.arena.allocator(), &self.data, &other.data);
    }

    fn mergeObjects(alloc: Allocator, dst: *json.ObjectMap, src: *const json.ObjectMap) !void {
        var it = src.iterator();
        while (it.next()) |entry| {
            // Check existing first without duping key
            if (dst.getPtr(entry.key_ptr.*)) |existing| {
                if (existing.* == .object and entry.value_ptr.* == .object) {
                    try mergeObjects(alloc, &existing.object, &entry.value_ptr.object);
                    continue;
                }
                // Overwrite existing value — key already in map, no dupe needed
                existing.* = try dupeValue(alloc, entry.value_ptr.*);
            } else {
                // New key — must dupe
                const key = try alloc.dupe(u8, entry.key_ptr.*);
                try dst.put(key, try dupeValue(alloc, entry.value_ptr.*));
            }
        }
    }

    /// Deep copy this Unstructured into a new allocation.
    pub fn clone(self: *const Unstructured, allocator: Allocator) !Unstructured {
        const bytes = try self.toJson(allocator);
        defer allocator.free(bytes);
        return try Unstructured.fromJson(allocator, bytes);
    }

    // ── Walk / Transform ──────────────────────────────────────────────────

    /// Walk every leaf value in the tree, calling the callback with the
    /// path segments and the value.
    pub fn walk(self: *const Unstructured, context: anytype, callback: fn (@TypeOf(context), []const []const u8, json.Value) void) void {
        var path_buf: [32][]const u8 = undefined;
        walkValue(json.Value{ .object = self.data }, &path_buf, 0, context, callback);
    }

    /// Walk every string value in the tree. Useful for finding CEL expressions.
    pub fn walkStrings(self: *const Unstructured, context: anytype, callback: fn (@TypeOf(context), []const []const u8, []const u8) void) void {
        var path_buf: [32][]const u8 = undefined;
        walkStringsValue(json.Value{ .object = self.data }, &path_buf, 0, context, callback);
    }

    /// Transform the tree in-place. The callback receives each leaf path and value,
    /// and returns a replacement value or null to keep the original.
    pub fn transform(self: *Unstructured, context: anytype, callback: fn (@TypeOf(context), []const []const u8, json.Value) ?json.Value) !void {
        var path_buf: [32][]const u8 = undefined;
        try transformValue(self.arena.allocator(), &json.Value{ .object = self.data }, &path_buf, 0, context, callback);
    }

    // ── Metadata: TypeMeta ─────────────────────────────────────────────────

    pub fn getApiVersion(self: *const Unstructured) ?[]const u8 {
        return self.field("apiVersion").str();
    }

    pub fn getKind(self: *const Unstructured) ?[]const u8 {
        return self.field("kind").str();
    }

    /// Returns the GVK parsed from apiVersion + kind fields.
    pub fn getGVK(self: *const Unstructured) ?scheme.GroupVersionKind {
        const api_version = self.getApiVersion() orelse return null;
        const kind = self.getKind() orelse return null;
        const gv = scheme.GroupVersion.parse(api_version) orelse return null;
        return gv.withKind(kind);
    }

    pub fn setApiVersion(self: *Unstructured, api_version: []const u8) !void {
        try self.setString(&.{"apiVersion"}, api_version);
    }

    pub fn setKind(self: *Unstructured, kind: []const u8) !void {
        try self.setString(&.{"kind"}, kind);
    }

    pub fn setGVK(self: *Unstructured, gvk: scheme.GroupVersionKind) !void {
        var buf: [256]u8 = undefined;
        const api_version = try gvk.groupVersion().string(&buf);
        try self.setApiVersion(api_version);
        try self.setKind(gvk.kind);
    }

    // ── Metadata: ObjectMeta identity ────────────────────────────────────

    pub fn getName(self: *const Unstructured) ?[]const u8 {
        return self.field("metadata").field("name").str();
    }

    pub fn getGenerateName(self: *const Unstructured) ?[]const u8 {
        return self.field("metadata").field("generateName").str();
    }

    pub fn getNamespace(self: *const Unstructured) ?[]const u8 {
        return self.field("metadata").field("namespace").str();
    }

    pub fn getUid(self: *const Unstructured) ?[]const u8 {
        return self.field("metadata").field("uid").str();
    }

    pub fn getResourceVersion(self: *const Unstructured) ?[]const u8 {
        return self.field("metadata").field("resourceVersion").str();
    }

    pub fn getGeneration(self: *const Unstructured) ?i64 {
        return self.field("metadata").field("generation").int();
    }

    pub fn getSelfLink(self: *const Unstructured) ?[]const u8 {
        return self.field("metadata").field("selfLink").str();
    }

    pub fn setName(self: *Unstructured, name: []const u8) !void {
        try self.setString(&.{ "metadata", "name" }, name);
    }

    pub fn setGenerateName(self: *Unstructured, name: []const u8) !void {
        try self.setString(&.{ "metadata", "generateName" }, name);
    }

    pub fn setNamespace(self: *Unstructured, namespace: []const u8) !void {
        try self.setString(&.{ "metadata", "namespace" }, namespace);
    }

    pub fn setUid(self: *Unstructured, uid: []const u8) !void {
        try self.setString(&.{ "metadata", "uid" }, uid);
    }

    pub fn setResourceVersion(self: *Unstructured, rv: []const u8) !void {
        try self.setString(&.{ "metadata", "resourceVersion" }, rv);
    }

    pub fn setGeneration(self: *Unstructured, generation: i64) !void {
        try self.set(&.{ "metadata", "generation" }, .{ .integer = generation });
    }

    // ── Metadata: timestamps ─────────────────────────────────────────────

    pub fn getCreationTimestamp(self: *const Unstructured) ?[]const u8 {
        return self.field("metadata").field("creationTimestamp").str();
    }

    pub fn getDeletionTimestamp(self: *const Unstructured) ?[]const u8 {
        return self.field("metadata").field("deletionTimestamp").str();
    }

    pub fn getDeletionGracePeriodSeconds(self: *const Unstructured) ?i64 {
        return self.field("metadata").field("deletionGracePeriodSeconds").int();
    }

    pub fn setCreationTimestamp(self: *Unstructured, ts: []const u8) !void {
        try self.setString(&.{ "metadata", "creationTimestamp" }, ts);
    }

    pub fn setDeletionTimestamp(self: *Unstructured, ts: []const u8) !void {
        try self.setString(&.{ "metadata", "deletionTimestamp" }, ts);
    }

    pub fn setDeletionGracePeriodSeconds(self: *Unstructured, seconds: i64) !void {
        try self.set(&.{ "metadata", "deletionGracePeriodSeconds" }, .{ .integer = seconds });
    }

    // ── Metadata: labels ─────────────────────────────────────────────────

    /// Returns a Nav over the labels map. Use `.iter()` to iterate or `.field(key)` to get a value.
    pub fn getLabels(self: *const Unstructured) Nav {
        return self.field("metadata").field("labels");
    }

    /// Returns a string label value by key.
    pub fn getLabel(self: *const Unstructured, key: []const u8) ?[]const u8 {
        return self.field("metadata").field("labels").field(key).str();
    }

    pub fn setLabel(self: *Unstructured, key: []const u8, value: []const u8) !void {
        try self.setString(&.{ "metadata", "labels", key }, value);
    }

    pub fn removeLabel(self: *Unstructured, key: []const u8) bool {
        const meta_ptr = self.data.getPtr("metadata") orelse return false;
        if (meta_ptr.* != .object) return false;
        const labels_ptr = meta_ptr.object.getPtr("labels") orelse return false;
        if (labels_ptr.* != .object) return false;
        return labels_ptr.object.orderedRemove(key);
    }

    /// Replace all labels at once. Pass null to remove the labels field entirely.
    pub fn setLabels(self: *Unstructured, labels: ?[]const struct { key: []const u8, value: []const u8 }) !void {
        if (labels == null) {
            _ = self.remove(&.{ "metadata", "labels" });
            return;
        }
        const alloc = self.arena.allocator();
        var obj = json.ObjectMap.init(alloc);
        for (labels.?) |entry| {
            const k = try alloc.dupe(u8, entry.key);
            const v = try alloc.dupe(u8, entry.value);
            try obj.put(k, .{ .string = v });
        }
        try self.set(&.{ "metadata", "labels" }, .{ .object = obj });
    }

    // ── Metadata: annotations ────────────────────────────────────────────

    /// Returns a Nav over the annotations map.
    pub fn getAnnotations(self: *const Unstructured) Nav {
        return self.field("metadata").field("annotations");
    }

    /// Returns a string annotation value by key.
    pub fn getAnnotation(self: *const Unstructured, key: []const u8) ?[]const u8 {
        return self.field("metadata").field("annotations").field(key).str();
    }

    pub fn setAnnotation(self: *Unstructured, key: []const u8, value: []const u8) !void {
        try self.setString(&.{ "metadata", "annotations", key }, value);
    }

    pub fn removeAnnotation(self: *Unstructured, key: []const u8) bool {
        const meta_ptr = self.data.getPtr("metadata") orelse return false;
        if (meta_ptr.* != .object) return false;
        const ann_ptr = meta_ptr.object.getPtr("annotations") orelse return false;
        if (ann_ptr.* != .object) return false;
        return ann_ptr.object.orderedRemove(key);
    }

    /// Replace all annotations at once. Pass null to remove the annotations field entirely.
    pub fn setAnnotations(self: *Unstructured, annotations: ?[]const struct { key: []const u8, value: []const u8 }) !void {
        if (annotations == null) {
            _ = self.remove(&.{ "metadata", "annotations" });
            return;
        }
        const alloc = self.arena.allocator();
        var obj = json.ObjectMap.init(alloc);
        for (annotations.?) |entry| {
            const k = try alloc.dupe(u8, entry.key);
            const v = try alloc.dupe(u8, entry.value);
            try obj.put(k, .{ .string = v });
        }
        try self.set(&.{ "metadata", "annotations" }, .{ .object = obj });
    }

    // ── Metadata: finalizers ─────────────────────────────────────────────

    /// Returns the finalizers as an iterable Nav.
    pub fn getFinalizers(self: *const Unstructured) Nav {
        return self.field("metadata").field("finalizers");
    }

    /// Check if a specific finalizer is present.
    pub fn hasFinalizer(self: *const Unstructured, finalizer: []const u8) bool {
        var it = self.field("metadata").field("finalizers").iter();
        while (it.next()) |item| {
            if (item.str()) |s| {
                if (mem.eql(u8, s, finalizer)) return true;
            }
        }
        return false;
    }

    /// Add a finalizer if not already present.
    pub fn addFinalizer(self: *Unstructured, finalizer: []const u8) !void {
        if (self.hasFinalizer(finalizer)) return;
        const alloc = self.arena.allocator();
        const duped = try alloc.dupe(u8, finalizer);

        const meta_gop = try self.data.getOrPut("metadata");
        if (!meta_gop.found_existing or meta_gop.value_ptr.* != .object) {
            meta_gop.value_ptr.* = .{ .object = json.ObjectMap.init(alloc) };
        }
        var meta = &meta_gop.value_ptr.object;

        const fin_gop = try meta.getOrPut("finalizers");
        if (!fin_gop.found_existing or fin_gop.value_ptr.* != .array) {
            fin_gop.value_ptr.* = .{ .array = json.Array.init(alloc) };
        }
        try fin_gop.value_ptr.array.append(.{ .string = duped });
    }

    /// Remove a finalizer. Returns true if it was present.
    pub fn removeFinalizer(self: *Unstructured, finalizer: []const u8) bool {
        const meta_ptr = self.data.getPtr("metadata") orelse return false;
        if (meta_ptr.* != .object) return false;
        const fin_ptr = meta_ptr.object.getPtr("finalizers") orelse return false;
        if (fin_ptr.* != .array) return false;

        var i: usize = 0;
        while (i < fin_ptr.array.items.len) {
            if (fin_ptr.array.items[i] == .string and mem.eql(u8, fin_ptr.array.items[i].string, finalizer)) {
                _ = fin_ptr.array.orderedRemove(i);
                return true;
            }
            i += 1;
        }
        return false;
    }

    // ── Metadata: ownerReferences ────────────────────────────────────────

    /// OwnerReference represents a single owner reference entry.
    pub const OwnerReference = struct {
        api_version: []const u8,
        kind: []const u8,
        name: []const u8,
        uid: []const u8,
        controller: ?bool = null,
        block_owner_deletion: ?bool = null,
    };

    /// Returns owner references parsed from the metadata.
    pub fn getOwnerReferences(self: *const Unstructured, allocator: Allocator) !?[]OwnerReference {
        const nav = self.field("metadata").field("ownerReferences");
        const count = nav.len() orelse return null;
        if (count == 0) return &[_]OwnerReference{};

        var refs = try allocator.alloc(OwnerReference, count);
        var i: usize = 0;
        var it = nav.iter();
        while (it.next()) |item| {
            if (i >= count) break;
            refs[i] = .{
                .api_version = item.field("apiVersion").str() orelse "",
                .kind = item.field("kind").str() orelse "",
                .name = item.field("name").str() orelse "",
                .uid = item.field("uid").str() orelse "",
                .controller = item.field("controller").boolean(),
                .block_owner_deletion = item.field("blockOwnerDeletion").boolean(),
            };
            i += 1;
        }
        return refs[0..i];
    }

    /// Set all owner references at once.
    pub fn setOwnerReferences(self: *Unstructured, refs: []const OwnerReference) !void {
        const alloc = self.arena.allocator();
        var arr = json.Array.init(alloc);
        for (refs) |ref| {
            var obj = json.ObjectMap.init(alloc);
            try obj.put(try alloc.dupe(u8, "apiVersion"), .{ .string = try alloc.dupe(u8, ref.api_version) });
            try obj.put(try alloc.dupe(u8, "kind"), .{ .string = try alloc.dupe(u8, ref.kind) });
            try obj.put(try alloc.dupe(u8, "name"), .{ .string = try alloc.dupe(u8, ref.name) });
            try obj.put(try alloc.dupe(u8, "uid"), .{ .string = try alloc.dupe(u8, ref.uid) });
            if (ref.controller) |c| {
                try obj.put(try alloc.dupe(u8, "controller"), .{ .bool = c });
            }
            if (ref.block_owner_deletion) |b| {
                try obj.put(try alloc.dupe(u8, "blockOwnerDeletion"), .{ .bool = b });
            }
            try arr.append(.{ .object = obj });
        }
        try self.set(&.{ "metadata", "ownerReferences" }, .{ .array = arr });
    }

    /// Add a single owner reference.
    pub fn addOwnerReference(self: *Unstructured, ref: OwnerReference) !void {
        const alloc = self.arena.allocator();
        var obj = json.ObjectMap.init(alloc);
        try obj.put(try alloc.dupe(u8, "apiVersion"), .{ .string = try alloc.dupe(u8, ref.api_version) });
        try obj.put(try alloc.dupe(u8, "kind"), .{ .string = try alloc.dupe(u8, ref.kind) });
        try obj.put(try alloc.dupe(u8, "name"), .{ .string = try alloc.dupe(u8, ref.name) });
        try obj.put(try alloc.dupe(u8, "uid"), .{ .string = try alloc.dupe(u8, ref.uid) });
        if (ref.controller) |c| {
            try obj.put(try alloc.dupe(u8, "controller"), .{ .bool = c });
        }
        if (ref.block_owner_deletion) |b| {
            try obj.put(try alloc.dupe(u8, "blockOwnerDeletion"), .{ .bool = b });
        }

        // Ensure metadata.ownerReferences exists as array
        const meta_gop = try self.data.getOrPut("metadata");
        if (!meta_gop.found_existing or meta_gop.value_ptr.* != .object) {
            meta_gop.value_ptr.* = .{ .object = json.ObjectMap.init(alloc) };
        }
        var meta = &meta_gop.value_ptr.object;
        const or_gop = try meta.getOrPut("ownerReferences");
        if (!or_gop.found_existing or or_gop.value_ptr.* != .array) {
            or_gop.value_ptr.* = .{ .array = json.Array.init(alloc) };
        }
        try or_gop.value_ptr.array.append(.{ .object = obj });
    }

    /// Remove an owner reference by uid. Returns true if found and removed.
    pub fn removeOwnerReference(self: *Unstructured, uid: []const u8) bool {
        const meta_ptr = self.data.getPtr("metadata") orelse return false;
        if (meta_ptr.* != .object) return false;
        const or_ptr = meta_ptr.object.getPtr("ownerReferences") orelse return false;
        if (or_ptr.* != .array) return false;

        var i: usize = 0;
        while (i < or_ptr.array.items.len) {
            const item = or_ptr.array.items[i];
            if (item == .object) {
                if (item.object.get("uid")) |uid_val| {
                    if (uid_val == .string and mem.eql(u8, uid_val.string, uid)) {
                        _ = or_ptr.array.orderedRemove(i);
                        return true;
                    }
                }
            }
            i += 1;
        }
        return false;
    }

    /// Check if this object is owned by a controller (any ownerRef with controller=true).
    pub fn isControlled(self: *const Unstructured) bool {
        var it = self.field("metadata").field("ownerReferences").iter();
        while (it.next()) |ref| {
            if (ref.field("controller").boolean()) |c| {
                if (c) return true;
            }
        }
        return false;
    }

    /// Returns the controller owner reference (the one with controller=true), if any.
    pub fn getControllerOwner(self: *const Unstructured) ?Nav {
        var it = self.field("metadata").field("ownerReferences").iter();
        while (it.next()) |ref| {
            if (ref.field("controller").boolean()) |c| {
                if (c) return ref;
            }
        }
        return null;
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    /// Navigate to a mutable value pointer by segment path.
    fn fieldPath(self: *Unstructured, path: []const []const u8) ?*json.Value {
        if (path.len == 0) return null;
        var obj = &self.data;
        for (path[0 .. path.len - 1]) |key| {
            const ptr = obj.getPtr(key) orelse return null;
            if (ptr.* == .object) {
                obj = &ptr.object;
            } else {
                return null;
            }
        }
        return obj.getPtr(path[path.len - 1]);
    }

    fn walkValue(value: json.Value, path_buf: [][]const u8, depth: usize, context: anytype, callback: fn (@TypeOf(context), []const []const u8, json.Value) void) void {
        switch (value) {
            .object => |obj| {
                var it = obj.iterator();
                while (it.next()) |entry| {
                    if (depth < path_buf.len) {
                        path_buf[depth] = entry.key_ptr.*;
                        walkValue(entry.value_ptr.*, path_buf, depth + 1, context, callback);
                    }
                }
            },
            .array => |arr| {
                for (arr.items) |item| {
                    walkValue(item, path_buf, depth, context, callback);
                }
            },
            else => {
                callback(context, path_buf[0..depth], value);
            },
        }
    }

    fn walkStringsValue(value: json.Value, path_buf: [][]const u8, depth: usize, context: anytype, callback: fn (@TypeOf(context), []const []const u8, []const u8) void) void {
        switch (value) {
            .object => |obj| {
                var it = obj.iterator();
                while (it.next()) |entry| {
                    if (depth < path_buf.len) {
                        path_buf[depth] = entry.key_ptr.*;
                        walkStringsValue(entry.value_ptr.*, path_buf, depth + 1, context, callback);
                    }
                }
            },
            .array => |arr| {
                for (arr.items) |item| {
                    walkStringsValue(item, path_buf, depth, context, callback);
                }
            },
            .string => |s| {
                callback(context, path_buf[0..depth], s);
            },
            else => {},
        }
    }

    fn transformValue(alloc: Allocator, value: *json.Value, path_buf: [][]const u8, depth: usize, context: anytype, callback: fn (@TypeOf(context), []const []const u8, json.Value) ?json.Value) !void {
        switch (value.*) {
            .object => |*obj| {
                var it = obj.iterator();
                while (it.next()) |entry| {
                    if (depth < path_buf.len) {
                        path_buf[depth] = entry.key_ptr.*;
                        try transformValue(alloc, entry.value_ptr, path_buf, depth + 1, context, callback);
                    }
                }
            },
            .array => |*arr| {
                for (arr.items) |*item| {
                    try transformValue(alloc, item, path_buf, depth, context, callback);
                }
            },
            else => {
                if (callback(context, path_buf[0..depth], value.*)) |replacement| {
                    value.* = try dupeValue(alloc, replacement);
                }
            },
        }
    }
};

/// Nav provides null-propagating navigation through a JSON value tree.
/// If any step in the chain encounters a missing field, wrong type, or null,
/// subsequent steps continue to return a "miss" Nav without error.
pub const Nav = struct {
    /// The state of navigation.
    state: State,

    const State = union(enum) {
        /// Successfully navigated to a value (may be json .null).
        value: json.Value,
        /// The field/index was not found in the parent.
        not_found,
        /// Navigation was applied to the wrong type (e.g., .field() on an array).
        type_mismatch,
    };

    const not_found = Nav{ .state = .not_found };
    const type_mismatch = Nav{ .state = .type_mismatch };

    fn fromObject(obj: *const json.ObjectMap, key: []const u8) Nav {
        if (obj.get(key)) |v| {
            return .{ .state = .{ .value = v } };
        }
        return not_found;
    }

    // ── Navigate ──────────────────────────────────────────────────────────

    /// Navigate into an object field by key.
    /// Returns not_found if key doesn't exist, type_mismatch if current value isn't an object.
    pub fn field(self: Nav, key: []const u8) Nav {
        switch (self.state) {
            .value => |v| switch (v) {
                .object => |obj| {
                    if (obj.get(key)) |child| {
                        return .{ .state = .{ .value = child } };
                    }
                    return not_found;
                },
                .null => return not_found,
                else => return type_mismatch,
            },
            .not_found => return not_found,
            .type_mismatch => return type_mismatch,
        }
    }

    /// Navigate into an array element by index.
    /// Returns not_found if index out of bounds, type_mismatch if current value isn't an array.
    pub fn at(self: Nav, index: usize) Nav {
        switch (self.state) {
            .value => |v| switch (v) {
                .array => |arr| {
                    if (index < arr.items.len) {
                        return .{ .state = .{ .value = arr.items[index] } };
                    }
                    return not_found;
                },
                .null => return not_found,
                else => return type_mismatch,
            },
            .not_found => return not_found,
            .type_mismatch => return type_mismatch,
        }
    }

    /// Find an object in an array where `key` equals `value`.
    /// Returns not_found if no match, type_mismatch if current value isn't an array.
    pub fn find(self: Nav, key: []const u8, value: []const u8) Nav {
        switch (self.state) {
            .value => |v| switch (v) {
                .array => |arr| {
                    for (arr.items) |item| {
                        if (item == .object) {
                            if (item.object.get(key)) |field_val| {
                                if (field_val == .string and mem.eql(u8, field_val.string, value)) {
                                    return .{ .state = .{ .value = item } };
                                }
                            }
                        }
                    }
                    return not_found;
                },
                else => return type_mismatch,
            },
            .not_found => return not_found,
            .type_mismatch => return type_mismatch,
        }
    }

    // ── Read terminals ────────────────────────────────────────────────────

    /// Get the string value, or null if not a string or not found.
    pub fn str(self: Nav) ?[]const u8 {
        return switch (self.state) {
            .value => |v| switch (v) {
                .string => |s| s,
                else => null,
            },
            .not_found, .type_mismatch => null,
        };
    }

    /// Get the integer value, or null if not an integer or not found.
    pub fn int(self: Nav) ?i64 {
        return switch (self.state) {
            .value => |v| switch (v) {
                .integer => |i| i,
                else => null,
            },
            .not_found, .type_mismatch => null,
        };
    }

    /// Get the float value, or null if not a float or not found.
    pub fn float(self: Nav) ?f64 {
        return switch (self.state) {
            .value => |v| switch (v) {
                .float => |f| f,
                else => null,
            },
            .not_found, .type_mismatch => null,
        };
    }

    /// Get the boolean value, or null if not a boolean or not found.
    pub fn boolean(self: Nav) ?bool {
        return switch (self.state) {
            .value => |v| switch (v) {
                .bool => |b| b,
                else => null,
            },
            .not_found, .type_mismatch => null,
        };
    }

    /// Get the raw JSON value, or null if not found.
    /// Returns .null for explicit JSON nulls — use `isNull()` to distinguish.
    pub fn raw(self: Nav) ?json.Value {
        return switch (self.state) {
            .value => |v| v,
            .not_found, .type_mismatch => null,
        };
    }

    // ── Inspection ────────────────────────────────────────────────────────

    /// Returns true if the navigation reached a value (including JSON null).
    pub fn exists(self: Nav) bool {
        return self.state == .value;
    }

    /// Returns true only if the value is an explicit JSON null.
    pub fn isNull(self: Nav) bool {
        return switch (self.state) {
            .value => |v| v == .null,
            .not_found, .type_mismatch => false,
        };
    }

    /// Returns true if navigation failed due to a type mismatch
    /// (e.g., .field() on an array, .at() on an object).
    pub fn isTypeMismatch(self: Nav) bool {
        return self.state == .type_mismatch;
    }

    /// Returns true if the field/index simply doesn't exist.
    pub fn isNotFound(self: Nav) bool {
        return self.state == .not_found;
    }

    /// Returns the number of elements in an array or object, or null.
    pub fn len(self: Nav) ?usize {
        return switch (self.state) {
            .value => |v| switch (v) {
                .array => |arr| arr.items.len,
                .object => |obj| obj.count(),
                else => null,
            },
            .not_found, .type_mismatch => null,
        };
    }

    // ── Iteration ─────────────────────────────────────────────────────────

    /// Returns an iterator over array elements or object entries.
    /// Each item is a Nav. For objects, use `.key()` on the iterator to
    /// get the current key.
    pub fn iter(self: Nav) Iterator {
        return switch (self.state) {
            .value => |v| switch (v) {
                .array => |arr| .{ .inner = .{ .array = .{ .items = arr.items, .index = 0 } } },
                .object => |obj| .{ .inner = .{ .object = obj.iterator() } },
                else => .{ .inner = .empty },
            },
            .not_found, .type_mismatch => .{ .inner = .empty },
        };
    }

    pub const Iterator = struct {
        inner: Inner,
        current_key: ?[]const u8 = null,

        const Inner = union(enum) {
            array: struct {
                items: []const json.Value,
                index: usize,
            },
            object: json.ObjectMap.Iterator,
            empty,
        };

        /// Get the next value as a Nav.
        pub fn next(self: *Iterator) ?Nav {
            switch (self.inner) {
                .array => |*a| {
                    if (a.index < a.items.len) {
                        const val = a.items[a.index];
                        a.index += 1;
                        return .{ .state = .{ .value = val } };
                    }
                    return null;
                },
                .object => |*o| {
                    if (o.next()) |entry| {
                        self.current_key = entry.key_ptr.*;
                        return .{ .state = .{ .value = entry.value_ptr.* } };
                    }
                    return null;
                },
                .empty => return null,
            }
        }

        /// Get the current key (only valid for object iteration).
        pub fn key(self: *const Iterator) ?[]const u8 {
            return self.current_key;
        }
    };
};

// ── Value duplication ─────────────────────────────────────────────────────

/// Deep-copy a json.Value into the given allocator.
fn dupeValue(alloc: Allocator, value: json.Value) !json.Value {
    return switch (value) {
        .null => .null,
        .bool => |b| .{ .bool = b },
        .integer => |i| .{ .integer = i },
        .float => |f| .{ .float = f },
        .number_string => |s| .{ .number_string = try alloc.dupe(u8, s) },
        .string => |s| .{ .string = try alloc.dupe(u8, s) },
        .array => |arr| blk: {
            var new_arr = json.Array.init(alloc);
            try new_arr.ensureTotalCapacity(arr.items.len);
            for (arr.items) |item| {
                try new_arr.append(try dupeValue(alloc, item));
            }
            break :blk .{ .array = new_arr };
        },
        .object => |obj| blk: {
            var new_obj = json.ObjectMap.init(alloc)
;
            var it = obj.iterator();
            while (it.next()) |entry| {
                const new_key = try alloc.dupe(u8, entry.key_ptr.*);
                try new_obj.put(new_key, try dupeValue(alloc, entry.value_ptr.*));
            }
            break :blk .{ .object = new_obj };
        },
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const test_json =
    \\{
    \\  "apiVersion": "apps/v1",
    \\  "kind": "Deployment",
    \\  "metadata": {
    \\    "name": "nginx",
    \\    "namespace": "default",
    \\    "uid": "abc-123",
    \\    "resourceVersion": "12345",
    \\    "generation": 3,
    \\    "labels": {
    \\      "app": "nginx",
    \\      "app.kubernetes.io/name": "nginx-ingress"
    \\    },
    \\    "annotations": {
    \\      "note": "test"
    \\    },
    \\    "finalizers": ["my.io/cleanup", "other.io/guard"]
    \\  },
    \\  "spec": {
    \\    "replicas": 3,
    \\    "paused": false,
    \\    "template": {
    \\      "spec": {
    \\        "containers": [
    \\          {"name": "nginx", "image": "nginx:1.21"},
    \\          {"name": "sidecar", "image": "busybox"}
    \\        ]
    \\      }
    \\    }
    \\  },
    \\  "nullField": null
    \\}
;

// ── Nav read tests ────────────────────────────────────────────────────────

test "Nav: read typed values" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    // String fields
    const str_cases = .{
        .{ "metadata.name", "nginx" },
        .{ "metadata.namespace", "default" },
        .{ "apiVersion", "apps/v1" },
        .{ "kind", "Deployment" },
        .{ "metadata.uid", "abc-123" },
    };
    inline for (str_cases) |c| {
        const fields = comptime splitPath(c[0]);
        const nav = navigatePath(obj, &fields);
        try testing.expectEqualStrings(c[1], nav.str().?);
    }

    // Integer
    try testing.expectEqual(@as(i64, 3), obj.field("spec").field("replicas").int().?);
    try testing.expectEqual(@as(i64, 3), obj.field("metadata").field("generation").int().?);

    // Boolean
    try testing.expectEqual(false, obj.field("spec").field("paused").boolean().?);
}

fn splitPath(comptime path: []const u8) [countDots(path) + 1][]const u8 {
    var result: [countDots(path) + 1][]const u8 = undefined;
    var i: usize = 0;
    var start: usize = 0;
    for (path, 0..) |c, j| {
        if (c == '.') {
            result[i] = path[start..j];
            i += 1;
            start = j + 1;
        }
    }
    result[i] = path[start..];
    return result;
}

fn countDots(comptime s: []const u8) usize {
    var n: usize = 0;
    for (s) |c| {
        if (c == '.') n += 1;
    }
    return n;
}

fn navigatePath(obj: Unstructured, fields: []const []const u8) Nav {
    var nav = obj.field(fields[0]);
    for (fields[1..]) |f| {
        nav = nav.field(f);
    }
    return nav;
}

test "Nav: three-state distinction" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    const State = struct { exists: bool, is_null: bool, not_found: bool, type_mismatch: bool };
    const cases = .{
        // Existing value
        .{ obj.field("metadata").field("name"), State{ .exists = true, .is_null = false, .not_found = false, .type_mismatch = false } },
        // Explicit null
        .{ obj.field("nullField"), State{ .exists = true, .is_null = true, .not_found = false, .type_mismatch = false } },
        // Missing field
        .{ obj.field("metadata").field("nonexistent"), State{ .exists = false, .is_null = false, .not_found = true, .type_mismatch = false } },
        // Type mismatch: .field() on array
        .{ obj.field("spec").field("template").field("spec").field("containers").field("bogus"), State{ .exists = false, .is_null = false, .not_found = false, .type_mismatch = true } },
        // Type mismatch: .at() on object
        .{ obj.field("metadata").at(0), State{ .exists = false, .is_null = false, .not_found = false, .type_mismatch = true } },
    };
    inline for (cases) |c| {
        try testing.expectEqual(c[1].exists, c[0].exists());
        try testing.expectEqual(c[1].is_null, c[0].isNull());
        try testing.expectEqual(c[1].not_found, c[0].isNotFound());
        try testing.expectEqual(c[1].type_mismatch, c[0].isTypeMismatch());
    }
}

test "Nav: state propagation through chains" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    // not_found stays not_found
    try testing.expect(obj.field("nonexistent").field("deep").field("path").isNotFound());
    // type_mismatch stays type_mismatch
    try testing.expect(obj.field("metadata").at(0).field("name").isTypeMismatch());
    // wrong type terminal returns null without crashing
    try testing.expect(obj.field("spec").field("replicas").str() == null);
    try testing.expect(obj.field("metadata").field("name").int() == null);
}

test "Nav: array access" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    const containers = obj.field("spec").field("template").field("spec").field("containers");

    // Index access
    try testing.expectEqualStrings("nginx:1.21", containers.at(0).field("image").str().?);
    try testing.expectEqualStrings("sidecar", containers.at(1).field("name").str().?);

    // Out of bounds
    try testing.expect(containers.at(99).isNotFound());

    // Find by field value
    try testing.expectEqualStrings("nginx:1.21", containers.find("name", "nginx").field("image").str().?);
    try testing.expect(!containers.find("name", "nonexistent").exists());

    // Len
    try testing.expectEqual(@as(usize, 2), containers.len().?);

    // Labels with dots in keys
    try testing.expectEqualStrings("nginx-ingress", obj.field("metadata").field("labels").field("app.kubernetes.io/name").str().?);
}

test "Nav: len" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    try testing.expectEqual(@as(usize, 2), obj.field("metadata").field("labels").len().?);
    try testing.expect(obj.field("metadata").field("name").len() == null);
    try testing.expect(obj.field("nonexistent").len() == null);
}

// ── Iterator tests ────────────────────────────────────────────────────────

test "Nav: iterate array" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    var buf: [8][]const u8 = undefined;
    var count: usize = 0;
    var it = obj.field("spec").field("template").field("spec").field("containers").iter();
    while (it.next()) |c| {
        if (c.field("name").str()) |n| {
            buf[count] = n;
            count += 1;
        }
    }
    try testing.expectEqual(@as(usize, 2), count);
    try testing.expectEqualStrings("nginx", buf[0]);
    try testing.expectEqualStrings("sidecar", buf[1]);
}

test "Nav: iterate object keys" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    var count: usize = 0;
    var it = obj.field("metadata").field("labels").iter();
    while (it.next()) |_| {
        _ = it.key().?;
        count += 1;
    }
    try testing.expectEqual(@as(usize, 2), count);
}

test "Nav: iterate missing/empty" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    var it = obj.field("nonexistent").iter();
    try testing.expect(it.next() == null);
}

// ── Metadata read tests ──────────────────────────────────────────────────

test "metadata: read identity fields" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    const cases = .{
        .{ obj.getName(), "nginx" },
        .{ obj.getNamespace(), "default" },
        .{ obj.getUid(), "abc-123" },
        .{ obj.getResourceVersion(), "12345" },
        .{ obj.getApiVersion(), "apps/v1" },
        .{ obj.getKind(), "Deployment" },
    };
    inline for (cases) |c| {
        try testing.expectEqualStrings(c[1], c[0].?);
    }
    try testing.expectEqual(@as(i64, 3), obj.getGeneration().?);
    try testing.expect(obj.getGenerateName() == null);
    try testing.expect(obj.getSelfLink() == null);
}

test "metadata: getGVK" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    const gvk = obj.getGVK().?;
    try testing.expectEqualStrings("apps", gvk.group);
    try testing.expectEqualStrings("v1", gvk.version);
    try testing.expectEqualStrings("Deployment", gvk.kind);
}

test "metadata: labels and annotations" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    try testing.expectEqualStrings("nginx", obj.getLabel("app").?);
    try testing.expectEqualStrings("nginx-ingress", obj.getLabel("app.kubernetes.io/name").?);
    try testing.expect(obj.getLabel("nonexistent") == null);
    try testing.expectEqualStrings("test", obj.getAnnotation("note").?);
}

test "metadata: finalizers" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    const cases = .{
        .{ "my.io/cleanup", true },
        .{ "other.io/guard", true },
        .{ "nope.io/missing", false },
    };
    inline for (cases) |c| {
        try testing.expectEqual(c[1], obj.hasFinalizer(c[0]));
    }
}

// ── Mutation tests ────────────────────────────────────────────────────────

test "set: create, overwrite, deep nested" {
    var obj = try Unstructured.init(testing.allocator);
    defer obj.deinit();

    // Create nested path
    try obj.setString(&.{ "metadata", "name" }, "test-pod");
    try testing.expectEqualStrings("test-pod", obj.getName().?);

    // Overwrite
    try obj.setString(&.{ "metadata", "name" }, "updated");
    try testing.expectEqualStrings("updated", obj.getName().?);

    // Deep auto-create
    try obj.setString(&.{ "a", "b", "c", "d" }, "deep");
    try testing.expectEqualStrings("deep", obj.field("a").field("b").field("c").field("d").str().?);

    // Explicit null
    try obj.setNull(&.{ "metadata", "deletionTimestamp" });
    try testing.expect(obj.field("metadata").field("deletionTimestamp").isNull());
    try testing.expect(obj.field("metadata").field("deletionTimestamp").exists());
}

test "remove" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    try testing.expect(obj.remove(&.{ "metadata", "annotations" }));
    try testing.expect(!obj.field("metadata").field("annotations").exists());
    try testing.expect(!obj.remove(&.{ "metadata", "nonexistent" }));
    try testing.expect(!obj.remove(&.{}));
}

// ── Finalizer mutation tests ──────────────────────────────────────────────

test "finalizers: add, dedup, remove" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    // Add new
    try obj.addFinalizer("new.io/fin");
    try testing.expect(obj.hasFinalizer("new.io/fin"));
    try testing.expect(obj.hasFinalizer("my.io/cleanup"));

    // Duplicate is noop
    const before = obj.field("metadata").field("finalizers").len().?;
    try obj.addFinalizer("my.io/cleanup");
    try testing.expectEqual(before, obj.field("metadata").field("finalizers").len().?);

    // Remove existing
    try testing.expect(obj.removeFinalizer("my.io/cleanup"));
    try testing.expect(!obj.hasFinalizer("my.io/cleanup"));
    try testing.expect(obj.hasFinalizer("other.io/guard"));

    // Remove non-existing
    try testing.expect(!obj.removeFinalizer("nope.io/missing"));
}

test "finalizers: create from empty" {
    var obj = try Unstructured.init(testing.allocator);
    defer obj.deinit();

    try obj.addFinalizer("new.io/fin");
    try testing.expect(obj.hasFinalizer("new.io/fin"));
}

// ── Label/annotation mutation tests ───────────────────────────────────────

test "labels: set, remove, bulk" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    // Set new label, existing preserved
    try obj.setLabel("tier", "frontend");
    try testing.expectEqualStrings("frontend", obj.getLabel("tier").?);
    try testing.expectEqualStrings("nginx", obj.getLabel("app").?);

    // Remove
    try testing.expect(obj.removeLabel("app"));
    try testing.expect(obj.getLabel("app") == null);
    try testing.expect(!obj.removeLabel("nonexistent"));
}

test "labels: bulk set and clear" {
    var obj = try Unstructured.init(testing.allocator);
    defer obj.deinit();

    try obj.setLabels(&.{
        .{ .key = "app", .value = "nginx" },
        .{ .key = "env", .value = "prod" },
    });
    try testing.expectEqualStrings("nginx", obj.getLabel("app").?);
    try testing.expectEqualStrings("prod", obj.getLabel("env").?);

    try obj.setLabels(null);
    try testing.expect(obj.getLabel("app") == null);
}

test "annotations: set, remove, bulk" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    try obj.setAnnotation("key", "value");
    try testing.expectEqualStrings("value", obj.getAnnotation("key").?);

    try testing.expect(obj.removeAnnotation("note"));
    try testing.expect(obj.getAnnotation("note") == null);
}

test "annotations: bulk set" {
    var obj = try Unstructured.init(testing.allocator);
    defer obj.deinit();

    try obj.setAnnotations(&.{ .{ .key = "note", .value = "hello" } });
    try testing.expectEqualStrings("hello", obj.getAnnotation("note").?);
}

test "setGVK" {
    const cases = .{
        .{ scheme.GroupVersionKind{ .group = "apps", .version = "v1", .kind = "Deployment" }, "apps/v1", "Deployment" },
        .{ scheme.GroupVersionKind{ .group = "", .version = "v1", .kind = "Pod" }, "v1", "Pod" },
    };
    inline for (cases) |c| {
        var obj = try Unstructured.init(testing.allocator);
        defer obj.deinit();
        try obj.setGVK(c[0]);
        try testing.expectEqualStrings(c[1], obj.getApiVersion().?);
        try testing.expectEqualStrings(c[2], obj.getKind().?);
    }
}

// ── Deep merge tests ──────────────────────────────────────────────────────

test "merge: adds new fields" {
    var base = try Unstructured.fromJson(testing.allocator, \\{"a": "1"}
    );
    defer base.deinit();

    var patch = try Unstructured.fromJson(testing.allocator, \\{"b": "2"}
    );
    defer patch.deinit();

    try base.merge(&patch);
    try testing.expectEqualStrings("1", base.field("a").str().?);
    try testing.expectEqualStrings("2", base.field("b").str().?);
}

test "merge: overwrites scalars" {
    var base = try Unstructured.fromJson(testing.allocator, \\{"a": "old"}
    );
    defer base.deinit();

    var patch = try Unstructured.fromJson(testing.allocator, \\{"a": "new"}
    );
    defer patch.deinit();

    try base.merge(&patch);
    try testing.expectEqualStrings("new", base.field("a").str().?);
}

test "merge: recursively merges objects" {
    var base = try Unstructured.fromJson(testing.allocator,
        \\{"metadata": {"name": "nginx", "labels": {"app": "nginx"}}}
    );
    defer base.deinit();

    var patch = try Unstructured.fromJson(testing.allocator,
        \\{"metadata": {"labels": {"tier": "frontend"}, "namespace": "default"}}
    );
    defer patch.deinit();

    try base.merge(&patch);
    try testing.expectEqualStrings("nginx", base.field("metadata").field("name").str().?);
    try testing.expectEqualStrings("nginx", base.field("metadata").field("labels").field("app").str().?);
    try testing.expectEqualStrings("frontend", base.field("metadata").field("labels").field("tier").str().?);
    try testing.expectEqualStrings("default", base.field("metadata").field("namespace").str().?);
}

test "merge: array replacement not recursive" {
    var base = try Unstructured.fromJson(testing.allocator,
        \\{"items": [1, 2, 3]}
    );
    defer base.deinit();

    var patch = try Unstructured.fromJson(testing.allocator,
        \\{"items": [4, 5]}
    );
    defer patch.deinit();

    try base.merge(&patch);
    try testing.expectEqual(@as(usize, 2), base.field("items").len().?);
}

// ── Serialization tests ───────────────────────────────────────────────────

test "toJson roundtrip and clone independence" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    // Roundtrip
    const bytes = try obj.toJson(testing.allocator);
    defer testing.allocator.free(bytes);
    var obj2 = try Unstructured.fromJson(testing.allocator, bytes);
    defer obj2.deinit();
    try testing.expectEqualStrings("nginx", obj2.getName().?);
    try testing.expectEqual(@as(i64, 3), obj2.field("spec").field("replicas").int().?);

    // Clone is independent
    var copy = try obj.clone(testing.allocator);
    defer copy.deinit();
    try obj.setString(&.{ "metadata", "name" }, "changed");
    try testing.expectEqualStrings("nginx", copy.getName().?);
    try testing.expectEqualStrings("changed", obj.getName().?);
}

// ── Walk tests ────────────────────────────────────────────────────────────

test "walk and walkStrings" {
    const WalkCounter = struct {
        var count: usize = 0;
        fn countAll(_: void, _: []const []const u8, _: json.Value) void { count += 1; }
    };
    const StringCounter = struct {
        var count: usize = 0;
        fn countStrings(_: void, _: []const []const u8, _: []const u8) void { count += 1; }
    };

    var obj = try Unstructured.fromJson(testing.allocator,
        \\{"a": "x", "b": {"c": 1, "d": true}, "e": "y"}
    );
    defer obj.deinit();

    WalkCounter.count = 0;
    obj.walk({}, WalkCounter.countAll);
    try testing.expectEqual(@as(usize, 4), WalkCounter.count); // x, 1, true, y

    StringCounter.count = 0;
    obj.walkStrings({}, StringCounter.countStrings);
    try testing.expectEqual(@as(usize, 2), StringCounter.count); // x, y
}

// ── Init and error tests ─────────────────────────────────────────────────

test "init and fromJson errors" {
    // Empty object
    var obj = try Unstructured.init(testing.allocator);
    defer obj.deinit();
    try testing.expect(obj.getName() == null);
    try testing.expect(obj.getKind() == null);

    // Non-object inputs
    try testing.expectError(error.NotAnObject, Unstructured.fromJson(testing.allocator, "\"string\""));
    try testing.expectError(error.NotAnObject, Unstructured.fromJson(testing.allocator, "[1,2,3]"));
}

// ── Metadata setters ─────────────────────────────────────────────────────

test "metadata: identity setters" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    const string_setters = .{
        .{ "setUid", "new-uid", "getUid" },
        .{ "setResourceVersion", "99999", "getResourceVersion" },
        .{ "setGenerateName", "nginx-", "getGenerateName" },
    };
    _ = string_setters;

    // uid
    try obj.setUid("new-uid");
    try testing.expectEqualStrings("new-uid", obj.getUid().?);

    // resourceVersion
    try obj.setResourceVersion("99999");
    try testing.expectEqualStrings("99999", obj.getResourceVersion().?);

    // generation
    try obj.setGeneration(5);
    try testing.expectEqual(@as(i64, 5), obj.getGeneration().?);

    // generateName
    try obj.setGenerateName("nginx-");
    try testing.expectEqualStrings("nginx-", obj.getGenerateName().?);
}

test "metadata: timestamps set/get" {
    var obj = try Unstructured.init(testing.allocator);
    defer obj.deinit();

    // All null initially
    try testing.expect(obj.getCreationTimestamp() == null);
    try testing.expect(obj.getDeletionTimestamp() == null);
    try testing.expect(obj.getDeletionGracePeriodSeconds() == null);

    // Set and verify
    try obj.setCreationTimestamp("2024-01-01T00:00:00Z");
    try obj.setDeletionTimestamp("2024-01-02T00:00:00Z");
    try obj.setDeletionGracePeriodSeconds(30);

    try testing.expectEqualStrings("2024-01-01T00:00:00Z", obj.getCreationTimestamp().?);
    try testing.expectEqualStrings("2024-01-02T00:00:00Z", obj.getDeletionTimestamp().?);
    try testing.expectEqual(@as(i64, 30), obj.getDeletionGracePeriodSeconds().?);
}

// ── OwnerReference tests ──────────────────────────────────────────────────

test "ownerReferences: full lifecycle" {
    var obj = try Unstructured.init(testing.allocator);
    defer obj.deinit();

    // Empty by default
    try testing.expect((try obj.getOwnerReferences(testing.allocator)) == null);
    try testing.expect(!obj.isControlled());
    try testing.expect(obj.getControllerOwner() == null);

    // Add non-controller ref
    try obj.addOwnerReference(.{ .api_version = "v1", .kind = "Pod", .name = "a", .uid = "uid-a" });
    try testing.expect(!obj.isControlled());

    // Add controller ref
    try obj.addOwnerReference(.{
        .api_version = "apps/v1",
        .kind = "Deployment",
        .name = "nginx",
        .uid = "uid-ctrl",
        .controller = true,
        .block_owner_deletion = true,
    });
    try testing.expect(obj.isControlled());

    // Get and verify
    const refs = (try obj.getOwnerReferences(testing.allocator)).?;
    defer testing.allocator.free(refs);
    try testing.expectEqual(@as(usize, 2), refs.len);
    try testing.expectEqualStrings("uid-a", refs[0].uid);
    try testing.expectEqualStrings("uid-ctrl", refs[1].uid);
    try testing.expectEqual(true, refs[1].controller.?);
    try testing.expectEqual(true, refs[1].block_owner_deletion.?);

    // Controller owner via Nav
    const owner = obj.getControllerOwner().?;
    try testing.expectEqualStrings("nginx", owner.field("name").str().?);

    // Remove by uid
    try testing.expect(obj.removeOwnerReference("uid-a"));
    try testing.expect(!obj.removeOwnerReference("uid-nonexistent"));
    const remaining = (try obj.getOwnerReferences(testing.allocator)).?;
    defer testing.allocator.free(remaining);
    try testing.expectEqual(@as(usize, 1), remaining.len);
}

// ── fieldPath tests ─────────────────────────────────────────────────────

test "fieldPath: resolves nested paths including object-typed leaves" {
    var obj = try Unstructured.init(testing.allocator);
    defer obj.deinit();

    try obj.setString(&.{ "spec", "selector", "app" }, "web");

    const cases = .{
        .{ .path = &.{ "spec", "selector", "app" }, .tag = .string, .found = true },
        .{ .path = &.{ "spec", "selector" }, .tag = .object, .found = true },
        .{ .path = &.{"spec"}, .tag = .object, .found = true },
        .{ .path = &.{"nonexistent"}, .tag = .object, .found = false },
        .{ .path = &.{ "spec", "missing" }, .tag = .object, .found = false },
        .{ .path = &[_][]const u8{}, .tag = .object, .found = false },
    };

    inline for (cases) |c| {
        const ptr = obj.fieldPath(c.path);
        if (c.found) {
            try testing.expect(ptr != null);
            try testing.expect(ptr.?.* == c.tag);
        } else {
            try testing.expect(ptr == null);
        }
    }
}

// ── append / setAt / removeAt tests ─────────────────────────────────────

test "append: creates arrays, appends, and rejects invalid paths" {
    var obj = try Unstructured.init(testing.allocator);
    defer obj.deinit();

    // Create new array at nested path
    try obj.append(&.{ "spec", "containers" }, .{ .string = "nginx" });
    try obj.append(&.{ "spec", "containers" }, .{ .string = "sidecar" });
    try testing.expectEqual(@as(usize, 2), obj.field("spec").field("containers").len().?);
    try testing.expectEqualStrings("nginx", obj.field("spec").field("containers").at(0).str().?);
    try testing.expectEqualStrings("sidecar", obj.field("spec").field("containers").at(1).str().?);

    // Deep nested path auto-creates intermediates
    try obj.append(&.{ "a", "b", "c" }, .{ .integer = 1 });
    try obj.append(&.{ "a", "b", "c" }, .{ .integer = 2 });
    try testing.expectEqual(@as(usize, 2), obj.field("a").field("b").field("c").len().?);

    // Error cases
    try testing.expectError(error.TypeMismatch, obj.append(&.{}, .{ .string = "x" }));
    try obj.setString(&.{"scalar"}, "value");
    try testing.expectError(error.TypeMismatch, obj.append(&.{ "scalar", "items" }, .{ .string = "x" }));
}

test "append: appends to pre-existing array from JSON" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    const before = obj.field("metadata").field("finalizers").len().?;
    try obj.append(&.{ "metadata", "finalizers" }, .{ .string = "new.io/fin" });
    try testing.expectEqual(before + 1, obj.field("metadata").field("finalizers").len().?);
}

test "setAt and removeAt: mutate array elements by index" {
    var obj = try Unstructured.init(testing.allocator);
    defer obj.deinit();

    try obj.append(&.{"items"}, .{ .string = "a" });
    try obj.append(&.{"items"}, .{ .string = "b" });
    try obj.append(&.{"items"}, .{ .string = "c" });

    // setAt replaces in place
    try obj.setAt(&.{"items"}, 1, .{ .string = "B" });
    try testing.expectEqualStrings("a", obj.field("items").at(0).str().?);
    try testing.expectEqualStrings("B", obj.field("items").at(1).str().?);
    try testing.expectEqualStrings("c", obj.field("items").at(2).str().?);

    // removeAt shifts elements
    try obj.removeAt(&.{"items"}, 0);
    try testing.expectEqual(@as(usize, 2), obj.field("items").len().?);
    try testing.expectEqualStrings("B", obj.field("items").at(0).str().?);
    try testing.expectEqualStrings("c", obj.field("items").at(1).str().?);

    // Deep nested setAt
    try obj.append(&.{ "a", "b", "c" }, .{ .integer = 1 });
    try obj.append(&.{ "a", "b", "c" }, .{ .integer = 2 });
    try obj.setAt(&.{ "a", "b", "c" }, 0, .{ .integer = 99 });
    try testing.expectEqual(@as(i64, 99), obj.field("a").field("b").field("c").at(0).int().?);

    // Error cases
    try testing.expectError(error.IndexOutOfBounds, obj.setAt(&.{"items"}, 99, .{ .string = "x" }));
    try testing.expectError(error.IndexOutOfBounds, obj.removeAt(&.{"items"}, 99));
    try obj.setString(&.{"scalar"}, "val");
    try testing.expectError(error.TypeMismatch, obj.setAt(&.{"scalar"}, 0, .{ .string = "x" }));
    try testing.expectError(error.TypeMismatch, obj.removeAt(&.{"scalar"}, 0));
}

test "ownerReferences: setOwnerReferences replaces all" {
    var obj = try Unstructured.init(testing.allocator);
    defer obj.deinit();

    try obj.addOwnerReference(.{ .api_version = "v1", .kind = "Pod", .name = "old", .uid = "old-uid" });
    try obj.setOwnerReferences(&.{
        .{ .api_version = "apps/v1", .kind = "ReplicaSet", .name = "new", .uid = "new-uid" },
    });

    const refs = (try obj.getOwnerReferences(testing.allocator)).?;
    defer testing.allocator.free(refs);
    try testing.expectEqual(@as(usize, 1), refs.len);
    try testing.expectEqualStrings("new-uid", refs[0].uid);
}

// ── fromJsonValue table-driven tests ─────────────────────────────────────

test "fromJsonValue: table-driven" {
    // Case 1: valid object
    {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        var obj_map = json.ObjectMap.init(alloc);
        try obj_map.put("key", .{ .string = "value" });
        const val = json.Value{ .object = obj_map };

        var u = try Unstructured.fromJsonValue(testing.allocator, val);
        defer u.deinit();
        try testing.expectEqualStrings("value", u.field("key").str().?);
    }

    // Case 2: string → NotAnObject
    try testing.expectError(error.NotAnObject, Unstructured.fromJsonValue(testing.allocator, .{ .string = "hello" }));

    // Case 3: array → NotAnObject
    {
        var arr = json.Array.init(testing.allocator);
        defer arr.deinit();
        try arr.append(.{ .integer = 1 });
        try testing.expectError(error.NotAnObject, Unstructured.fromJsonValue(testing.allocator, .{ .array = arr }));
    }

    // Case 4: null → NotAnObject
    try testing.expectError(error.NotAnObject, Unstructured.fromJsonValue(testing.allocator, .null));

    // Case 5: integer → NotAnObject
    try testing.expectError(error.NotAnObject, Unstructured.fromJsonValue(testing.allocator, .{ .integer = 42 }));

    // Case 6: nested object with deep copy verification
    {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        const alloc = arena.allocator();

        var inner_map = json.ObjectMap.init(alloc);
        try inner_map.put("nested", .{ .string = "deep" });
        var outer_map = json.ObjectMap.init(alloc);
        try outer_map.put("inner", .{ .object = inner_map });
        const val = json.Value{ .object = outer_map };

        var u = try Unstructured.fromJsonValue(testing.allocator, val);
        defer u.deinit();

        // Free the source arena — the Unstructured should still work (deep copy)
        arena.deinit();

        try testing.expectEqualStrings("deep", u.field("inner").field("nested").str().?);
    }
}

// ── Nav.raw table-driven tests ──────────────────────────────────────────

test "Nav.raw: returns underlying json.Value" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    // string field → .string
    {
        const v = obj.field("metadata").field("name").raw().?;
        try testing.expect(v == .string);
        try testing.expectEqualStrings("nginx", v.string);
    }

    // integer field → .integer
    {
        const v = obj.field("spec").field("replicas").raw().?;
        try testing.expect(v == .integer);
        try testing.expectEqual(@as(i64, 3), v.integer);
    }

    // missing field → null
    {
        try testing.expect(obj.field("nonexistent").raw() == null);
    }

    // null field → .null
    {
        const v = obj.field("nullField").raw().?;
        try testing.expect(v == .null);
    }

    // nested object → .object
    {
        const v = obj.field("metadata").field("labels").raw().?;
        try testing.expect(v == .object);
    }
}

// ── Nav.find table-driven tests ─────────────────────────────────────────

test "Nav.find: searches array of objects" {
    var obj = try Unstructured.fromJson(testing.allocator, test_json);
    defer obj.deinit();

    const containers = obj.field("spec").field("template").field("spec").field("containers");

    // match found
    {
        const found = containers.find("name", "nginx");
        try testing.expect(found.exists());
        try testing.expectEqualStrings("nginx:1.21", found.field("image").str().?);
    }

    // no match
    {
        const found = containers.find("name", "nonexistent");
        try testing.expect(!found.exists());
        try testing.expect(found.isNotFound());
    }

    // empty array
    {
        var empty_obj = try Unstructured.fromJson(testing.allocator,
            \\{"items": []}
        );
        defer empty_obj.deinit();
        const found = empty_obj.field("items").find("name", "anything");
        try testing.expect(!found.exists());
        try testing.expect(found.isNotFound());
    }

    // non-array input → type_mismatch
    {
        const found = obj.field("metadata").find("name", "nginx");
        try testing.expect(!found.exists());
        try testing.expect(found.isTypeMismatch());
    }
}

// ── Fuzz tests ──────────────────────────────────────────────────────────

test "fuzz: fromJson never crashes on arbitrary input" {
    try std.testing.fuzz({}, fuzzFromJson, .{});
}

fn fuzzFromJson(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    const len = smith.valueRangeAtMost(u16, 0, 4096);
    var buf: [4096]u8 = undefined;
    for (buf[0..len]) |*b| {
        b.* = smith.valueRangeAtMost(u8, 0, 255);
    }
    // Must not crash — errors are fine
    if (Unstructured.fromJson(testing.allocator, buf[0..len])) |obj_val| {
        var obj = obj_val;
        defer obj.deinit();
        // Exercise navigation on parsed object
        _ = obj.getName();
        _ = obj.getNamespace();
        _ = obj.getResourceVersion();
        _ = obj.field("metadata").field("labels").str();
    } else |_| {}
}

test "fuzz: fromJsonValue never crashes" {
    try std.testing.fuzz({}, fuzzFromJsonValue, .{});
}

fn fuzzFromJsonValue(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    const len = smith.valueRangeAtMost(u16, 0, 2048);
    var buf: [2048]u8 = undefined;
    for (buf[0..len]) |*b| {
        b.* = smith.valueRangeAtMost(u8, 0, 255);
    }
    // Parse as json.Value first
    const parsed = json.parseFromSlice(json.Value, testing.allocator, buf[0..len], .{}) catch return;
    defer parsed.deinit();
    // Try to create Unstructured from value — must not crash
    if (Unstructured.fromJsonValue(testing.allocator, parsed.value)) |obj_val| {
        var obj = obj_val;
        obj.deinit();
    } else |_| {}
}
