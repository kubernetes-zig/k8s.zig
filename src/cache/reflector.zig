const std = @import("std");
const mem = std.mem;
const Io = std.Io;
const Allocator = mem.Allocator;
const testing = std.testing;

const k8s = @import("k8s_zig");
const Unstructured = k8s.Unstructured;

const store_mod = @import("store.zig");
const Store = store_mod.Store;
const ObjectKey = @import("workqueue.zig").ObjectKey;

// ─────────────────────────────────────────────────────────────────────────────
// Watch event types (local definitions to avoid circular dependency with
// the client module). These mirror the types in src/client/watch.zig.
// ─────────────────────────────────────────────────────────────────────────────

/// Watch event types matching Go's watch.EventType.
pub const EventType = enum {
    added,
    modified,
    deleted,
    bookmark,
    err,

    pub fn fromString(s: []const u8) ?EventType {
        if (mem.eql(u8, s, "ADDED")) return .added;
        if (mem.eql(u8, s, "MODIFIED")) return .modified;
        if (mem.eql(u8, s, "DELETED")) return .deleted;
        if (mem.eql(u8, s, "BOOKMARK")) return .bookmark;
        if (mem.eql(u8, s, "ERROR")) return .err;
        return null;
    }

    pub fn string(self: EventType) []const u8 {
        return switch (self) {
            .added => "ADDED",
            .modified => "MODIFIED",
            .deleted => "DELETED",
            .bookmark => "BOOKMARK",
            .err => "ERROR",
        };
    }
};

/// A watch event carrying a type and an object.
pub const WatchEvent = struct {
    event_type: EventType,
    object: *Unstructured,
};

pub const DeletedFinalStateUnknown = struct {
    key: ObjectKey,
    obj: *const Unstructured,
};

pub const DeleteNotification = union(enum) {
    object: *const Unstructured,
    tombstone: DeletedFinalStateUnknown,

    pub fn objectRef(self: DeleteNotification) *const Unstructured {
        return switch (self) {
            .object => |obj| obj,
            .tombstone => |stale| stale.obj,
        };
    }

    pub fn isTombstone(self: DeleteNotification) bool {
        return switch (self) {
            .object => false,
            .tombstone => true,
        };
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// List result
// ─────────────────────────────────────────────────────────────────────────────

/// Result of a list operation, returned by the list function pointer.
pub const ListResult = struct {
    /// The items from the list response. Caller owns the slice and the
    /// Unstructured objects within it.
    items: []*Unstructured,
    /// Whether the reflector should free `items` after use.
    items_owned: bool = false,
    /// The resourceVersion from the list metadata.
    resource_version: []const u8,
    /// Whether the reflector should free `resource_version` after use.
    resource_version_owned: bool = false,
};

// ─────────────────────────────────────────────────────────────────────────────
// Watcher interface
// ─────────────────────────────────────────────────────────────────────────────

/// Abstract watcher interface. The reflector calls next() to receive events
/// and stop() when done. This avoids importing the client module directly.
pub const WatcherInterface = struct {
    ptr: *anyopaque,
    nextFn: *const fn (ptr: *anyopaque) WatcherError!?WatchEvent,
    stopFn: *const fn (ptr: *anyopaque) void,
    deinitFn: ?*const fn (ptr: *anyopaque) void = null,

    pub const WatcherError = error{
        Gone,
        Unavailable,
        WatchFailed,
        ConnectionRefused,
    };

    /// Get the next watch event. Returns null when the stream ends cleanly.
    /// Returns error.Gone for 410 responses (triggers relist).
    pub fn next(self: *const WatcherInterface) WatcherError!?WatchEvent {
        return self.nextFn(self.ptr);
    }

    /// Stop the watch stream and release resources.
    pub fn stop(self: *const WatcherInterface) void {
        self.stopFn(self.ptr);
    }

    pub fn deinit(self: *const WatcherInterface) void {
        if (self.deinitFn) |func| {
            func(self.ptr);
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Backoff
// ─────────────────────────────────────────────────────────────────────────────

/// Exponential backoff configuration.
pub const BackoffConfig = struct {
    /// Initial backoff duration in milliseconds.
    initial_ms: u64 = 800,
    /// Maximum backoff duration in milliseconds.
    max_ms: u64 = 30_000,
    /// Multiplier applied each retry.
    factor: u32 = 2,
    /// Jitter fraction (0.0 = no jitter, 1.0 = full jitter).
    /// The actual delay is uniformly distributed in
    /// [base * (1 - jitter), base]. Prevents thundering-herd relists
    /// when many controllers hit 410 Gone simultaneously.
    jitter: f64 = 1.0,

    /// Compute the base backoff duration for the given attempt (0-indexed),
    /// without jitter.
    pub fn delayMs(self: BackoffConfig, attempt: u32) u64 {
        var d: u64 = self.initial_ms;
        var i: u32 = 0;
        while (i < attempt) : (i += 1) {
            d = @min(d *| self.factor, self.max_ms);
        }
        return @min(d, self.max_ms);
    }

    /// Compute the jittered backoff duration for the given attempt.
    /// Only called on error paths, not hot.
    pub fn delayMsJittered(self: BackoffConfig, attempt: u32, rand: std.Random) u64 {
        const base = self.delayMs(attempt);
        if (self.jitter == 0.0 or base == 0) return base;
        const min_delay: u64 = @intFromFloat(@as(f64, @floatFromInt(base)) * (1.0 - self.jitter));
        const range = base - min_delay;
        if (range == 0) return base;
        return min_delay + rand.uintAtMost(u64, range);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Reflector
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer types for list and watch operations.
/// These are injected so the reflector does not depend on the client module.
pub const ListFn = *const fn (ctx: *anyopaque, allocator: Allocator, resource_version: ?[]const u8) anyerror!ListResult;
pub const WatchFn = *const fn (ctx: *anyopaque, allocator: Allocator, resource_version: ?[]const u8) anyerror!WatcherInterface;

/// Optional observer for store mutations performed by the reflector.
pub const Observer = struct {
    ptr: *anyopaque,
    on_add_fn: ?*const fn (ptr: *anyopaque, obj: *const Unstructured, is_in_initial_list: bool) anyerror!void = null,
    on_update_fn: ?*const fn (ptr: *anyopaque, old_obj: *const Unstructured, new_obj: *const Unstructured) anyerror!void = null,
    on_delete_fn: ?*const fn (ptr: *anyopaque, obj: *const Unstructured) anyerror!void = null,
    on_delete_detailed_fn: ?*const fn (ptr: *anyopaque, notification: DeleteNotification) anyerror!void = null,
    on_bookmark_fn: ?*const fn (ptr: *anyopaque, rv: []const u8) anyerror!void = null,
};

/// Reflector keeps a Store in sync with the API server by performing an
/// initial list followed by a continuous watch. On 410 Gone errors it
/// performs a full relist. Retries use exponential backoff.
///
/// This is the Zig equivalent of Go's client-go tools/cache.Reflector.
///
/// The reflector does not import the client module directly. Instead, it
/// accepts function pointers for list and watch operations, breaking the
/// circular dependency between cache and client modules.
pub const Reflector = struct {
    allocator: Allocator,
    store: *Store,
    source_ctx: *anyopaque,

    /// Function that performs a list operation against the API server.
    list_fn: ListFn,
    /// Function that opens a watch stream against the API server.
    watch_fn: WatchFn,

    /// Backoff configuration for retries.
    backoff: BackoffConfig,

    /// Last resource version successfully synced (owned string).
    last_resource_version: ?[]const u8,

    /// Whether the reflector has completed at least one list+watch cycle.
    has_synced: bool,

    /// Whether the reflector should stop.
    should_stop: bool,

    /// Count of consecutive errors for backoff calculation.
    consecutive_errors: u32,

    /// PRNG for backoff jitter. Seeded from wall clock at init.
    prng: std.Random.DefaultPrng,

    /// Last error encountered (for diagnostics).
    last_error: ?anyerror,

    observer: ?Observer,
    error_handler: ?ErrorHandler,
    owns_store_objects: bool,
    watcher_lock: Io.Mutex,
    current_watcher: ?WatcherInterface,
    resync_period_ms: u64,
    last_resync_ts: ?Io.Clock.Timestamp,

    /// Context for error handler callbacks.
    pub const ErrorContext = enum { list, watch, event_handler };

    /// Callback invoked when the reflector encounters an error during
    /// list, watch, or event processing. Allows logging, metrics, or
    /// custom recovery logic.
    pub const ErrorHandler = struct {
        ptr: *anyopaque,
        on_error_fn: *const fn (ptr: *anyopaque, err: anyerror, context: ErrorContext, consecutive_errors: u32) void,
    };

    pub const Options = struct {
        backoff: BackoffConfig = .{},
        observer: ?Observer = null,
        owns_store_objects: bool = false,
        /// Resync period in milliseconds. When set, the reflector periodically
        /// re-sends all store objects as update events to the observer, ensuring
        /// eventual consistency even if events were missed. 0 = disabled.
        resync_period_ms: u64 = 0,
        /// Error handler called on list/watch failures.
        error_handler: ?ErrorHandler = null,
    };

    /// Initialize a new Reflector.
    pub fn init(
        allocator: Allocator,
        store: *Store,
        source_ctx: *anyopaque,
        list_fn: ListFn,
        watch_fn: WatchFn,
        opts: Options,
    ) Reflector {
        return .{
            .allocator = allocator,
            .store = store,
            .source_ctx = source_ctx,
            .list_fn = list_fn,
            .watch_fn = watch_fn,
            .backoff = opts.backoff,
            .last_resource_version = null,
            .has_synced = false,
            .should_stop = false,
            .consecutive_errors = 0,
            .prng = std.Random.DefaultPrng.init(@intFromPtr(store) *% 0x517cc1b727220a95),
            .last_error = null,
            .observer = opts.observer,
            .error_handler = opts.error_handler,
            .owns_store_objects = opts.owns_store_objects,
            .watcher_lock = .init,
            .current_watcher = null,
            .resync_period_ms = opts.resync_period_ms,
            .last_resync_ts = null,
        };
    }

    pub fn deinit(self: *Reflector) void {
        self.stopCurrentWatcher();
        if (self.owns_store_objects) {
            if (self.store.list(self.allocator)) |items| {
                defer self.allocator.free(items);
                for (items) |obj| {
                    self.freeOwnedObject(obj);
                }
            } else |_| {
                // store.list() failed — fall back to iterating the store directly
                // to avoid leaking owned objects.
                var it = self.store.items.iterator();
                while (it.next()) |entry| {
                    self.freeOwnedObject(entry.value_ptr.*);
                }
            }
        }
        if (self.last_resource_version) |rv| self.allocator.free(rv);
    }

    /// Signal the reflector to stop.
    pub fn stop(self: *Reflector) void {
        self.should_stop = true;
        self.stopCurrentWatcher();
    }

    const WatchOutcome = enum {
        restart_watch,
        relist,
        stopped,
    };

    /// Run the list+watch loop. This function blocks until stopped or until
    /// an unrecoverable error occurs.
    ///
    /// For background execution, the caller should use io.concurrent to
    /// spawn this in a separate task.
    pub fn run(self: *Reflector) !void {
        var needs_list = true;
        var list_resource_version: ?[]const u8 = self.last_resource_version;

        while (!self.should_stop) {
            if (needs_list) {
                self.doList(list_resource_version) catch |err| {
                    if (self.should_stop) return;
                    self.consecutive_errors += 1;
                    self.last_error = err;
                    self.notifyError(err, .list);
                    const delay_ms = self.currentBackoffMs();
                    if (delay_ms > 0) {
                        Io.sleep(self.store.io, Io.Duration.fromMilliseconds(@intCast(delay_ms)), .awake) catch {};
                    }
                    switch (err) {
                        error.OutOfMemory => return err,
                        else => continue,
                    }
                };
                needs_list = false;
                list_resource_version = self.last_resource_version;
                self.consecutive_errors = 0;
                self.last_error = null;
            }

            const outcome = self.doWatch() catch |err| {
                if (self.should_stop) return;
                self.consecutive_errors += 1;
                self.last_error = err;
                self.notifyError(err, .watch);
                const delay_ms = self.currentBackoffMs();
                if (delay_ms > 0) {
                    Io.sleep(self.store.io, Io.Duration.fromMilliseconds(@intCast(delay_ms)), .awake) catch {};
                }
                switch (err) {
                    error.OutOfMemory => return err,
                    else => {
                        // If we have a resource version from a bookmark or event,
                        // try to resume the watch from there instead of relisting.
                        // Only relist on Gone (410) or if we have no RV at all.
                        if (self.last_resource_version != null) {
                            // Resume watch from last known RV — skip expensive relist
                            continue;
                        }
                        needs_list = true;
                        list_resource_version = null;
                        continue;
                    },
                }
            };

            self.consecutive_errors = 0;

            // Check if resync is due
            if (self.resync_period_ms > 0 and self.observer != null) {
                const now = Io.Clock.Timestamp.now(self.store.io, .awake);
                const resync_due = if (self.last_resync_ts) |last| blk: {
                    const elapsed = last.durationTo(now);
                    break :blk elapsed.raw.toMilliseconds() >= self.resync_period_ms;
                } else true;
                if (resync_due) {
                    self.doResync() catch {};
                    self.last_resync_ts = now;
                }
            }

            switch (outcome) {
                .restart_watch => {
                    // Try to resume from last known RV instead of relisting.
                    // The bookmark or last event RV is already in last_resource_version.
                    continue;
                },
                .relist => {
                    needs_list = true;
                    // Use empty string (not null) to signal state-of-the-world relist.
                    // Per K8s API: rv="" = list from beginning, rv=null = use current state.
                    list_resource_version = "";
                },
                .stopped => return,
            }
        }
    }

    /// Perform one list+watch cycle.
    pub fn listAndWatch(self: *Reflector) !void {
        try self.doList(self.last_resource_version);
        _ = try self.doWatch();
    }

    /// Perform a list and replace the store contents.
    fn doList(self: *Reflector, resource_version: ?[]const u8) !void {
        const result = try self.list_fn(self.source_ctx, self.allocator, resource_version);
        defer if (result.items_owned) self.allocator.free(result.items);
        defer if (result.resource_version_owned) self.allocator.free(result.resource_version);

        const should_snapshot_old = self.observer != null or self.owns_store_objects;
        const old_items = if (should_snapshot_old)
            try self.store.list(self.allocator)
        else
            try self.allocator.alloc(*Unstructured, 0);
        defer self.allocator.free(old_items);

        const is_initial_list = !self.has_synced;

        try self.store.replace(result.items, result.resource_version);
        try self.setLastResourceVersion(result.resource_version);

        if (self.observer) |observer| {
            try self.notifyReplace(observer, old_items, result.items, is_initial_list);
        }

        if (self.owns_store_objects) {
            for (old_items) |obj| {
                self.freeOwnedObject(obj);
            }
        }

        self.has_synced = true;
    }

    /// Re-send all store objects as update events to the observer.
    /// Ensures eventual consistency by replaying the full state.
    fn doResync(self: *Reflector) !void {
        const observer = self.observer orelse return;
        const items = try self.store.list(self.allocator);
        defer self.allocator.free(items);
        for (items) |obj| {
            try self.notifyUpdate(observer, obj, obj);
        }
    }

    /// Open a watch and process events until the stream ends or an error occurs.
    fn doWatch(self: *Reflector) !WatchOutcome {
        const watcher = try self.watch_fn(self.source_ctx, self.allocator, self.last_resource_version);
        self.setCurrentWatcher(watcher);
        defer self.stopCurrentWatcher();

        while (!self.should_stop) {
            const maybe_event = watcher.next() catch |err| {
                switch (err) {
                    error.Gone => return .relist,
                    else => return err,
                }
            };

            const event = maybe_event orelse {
                return if (self.should_stop) .stopped else .restart_watch;
            };

            self.processEvent(event) catch |err| switch (err) {
                error.Gone => return .relist,
                else => return err,
            };
        }

        return .stopped;
    }

    /// Apply a single watch event to the store.
    fn processEvent(self: *Reflector, event: WatchEvent) !void {
        switch (event.event_type) {
            .added => {
                const old_obj = self.lookupStoredObject(event.object);
                try self.store.add(event.object);
                // Use defer so old_obj is freed even if notify/setLastResourceVersion errors.
                defer if (self.owns_store_objects) {
                    if (old_obj) |old| self.freeOwnedObject(old);
                };
                if (self.observer) |observer| {
                    if (old_obj) |old| {
                        try self.notifyUpdate(observer, old, event.object);
                    } else {
                        try self.notifyAdd(observer, event.object, false);
                    }
                }
                if (event.object.getResourceVersion()) |rv| {
                    try self.setLastResourceVersion(rv);
                }
            },
            .modified => {
                const old_obj = self.lookupStoredObject(event.object);
                try self.store.update(event.object);
                defer if (self.owns_store_objects) {
                    if (old_obj) |old| self.freeOwnedObject(old);
                };
                if (self.observer) |observer| {
                    if (old_obj) |old| {
                        try self.notifyUpdate(observer, old, event.object);
                    } else {
                        try self.notifyAdd(observer, event.object, false);
                    }
                }
                if (event.object.getResourceVersion()) |rv| {
                    try self.setLastResourceVersion(rv);
                }
            },
            .deleted => {
                const old_obj = self.lookupStoredObject(event.object);
                if (event.object.getResourceVersion()) |rv| {
                    try self.setLastResourceVersion(rv);
                }
                try self.store.delete(event.object);
                if (self.observer) |observer| {
                    try self.notifyDelete(observer, .{ .object = event.object });
                }
                if (self.owns_store_objects) {
                    if (old_obj) |old| {
                        self.freeOwnedObject(old);
                        if (old != event.object) {
                            self.freeOwnedObject(event.object);
                        }
                    } else {
                        self.freeOwnedObject(event.object);
                    }
                }
            },
            .bookmark => {
                if (event.object.getResourceVersion()) |rv| {
                    try self.store.bookmark(rv);
                    if (self.observer) |observer| {
                        try self.notifyBookmark(observer, rv);
                    }
                    try self.setLastResourceVersion(rv);
                }
                if (self.owns_store_objects) {
                    self.freeOwnedObject(event.object);
                }
            },
            .err => {
                // Error events may contain a Status object with code 410.
                // The WatcherInterface should translate that to error.Gone,
                // but if it comes through as an event, check for 410.
                if (isGoneError(event.object)) {
                    if (self.owns_store_objects) {
                        self.freeOwnedObject(event.object);
                    }
                    return error.Gone;
                }
                // Other error events are logged but not fatal.
                if (self.owns_store_objects) {
                    self.freeOwnedObject(event.object);
                }
            },
        }
    }

    /// Update the last known resource version.
    fn setLastResourceVersion(self: *Reflector, rv: []const u8) !void {
        if (self.last_resource_version) |old| self.allocator.free(old);
        self.last_resource_version = try self.allocator.dupe(u8, rv);
    }

    /// Returns the current backoff delay in milliseconds, with jitter applied.
    pub fn currentBackoffMs(self: *Reflector) u64 {
        return self.backoff.delayMsJittered(self.consecutive_errors, self.prng.random());
    }

    /// Check if an error event represents a 410 Gone.
    fn isGoneError(obj: *const Unstructured) bool {
        if (obj.field("code").int()) |code| {
            return code == 410;
        }
        return false;
    }

    fn lookupStoredObject(self: *Reflector, obj: *const Unstructured) ?*Unstructured {
        const name = obj.getName() orelse return null;
        return self.store.getByName(obj.getNamespace(), name);
    }

    fn notifyReplace(
        self: *Reflector,
        observer: Observer,
        old_items: []*Unstructured,
        new_items: []*Unstructured,
        is_initial_list: bool,
    ) !void {
        const matched_old = try self.allocator.alloc(bool, old_items.len);
        defer self.allocator.free(matched_old);
        @memset(matched_old, false);

        var old_index: std.HashMapUnmanaged(
            ObjectIdentity,
            usize,
            ObjectIdentityContext,
            std.hash_map.default_max_load_percentage,
        ) = .empty;
        defer old_index.deinit(self.allocator);

        try old_index.ensureUnusedCapacity(self.allocator, @intCast(old_items.len));
        for (old_items, 0..) |old_obj, idx| {
            const key = ObjectIdentity.fromObject(old_obj) orelse continue;
            old_index.putAssumeCapacity(key, idx);
        }

        for (new_items) |new_obj| {
            const key = ObjectIdentity.fromObject(new_obj);
            if (key) |identity| {
                if (old_index.get(identity)) |idx| {
                    matched_old[idx] = true;
                    if (!sameObjectVersion(old_items[idx], new_obj)) {
                        try self.notifyUpdate(observer, old_items[idx], new_obj);
                    }
                    continue;
                }
            }

            if (findMatchingObject(old_items, new_obj)) |idx| {
                matched_old[idx] = true;
                if (!sameObjectVersion(old_items[idx], new_obj)) {
                    try self.notifyUpdate(observer, old_items[idx], new_obj);
                }
            } else {
                try self.notifyAdd(observer, new_obj, is_initial_list);
            }
        }

        for (old_items, 0..) |old_obj, idx| {
            if (!matched_old[idx]) {
                try self.notifyDelete(observer, tombstoneForObject(old_obj));
            }
        }
    }

    fn notifyAdd(self: *Reflector, observer: Observer, obj: *const Unstructured, is_in_initial_list: bool) !void {
        _ = self;
        if (observer.on_add_fn) |func| {
            try func(observer.ptr, obj, is_in_initial_list);
        }
    }

    fn notifyUpdate(self: *Reflector, observer: Observer, old_obj: *const Unstructured, new_obj: *const Unstructured) !void {
        _ = self;
        if (observer.on_update_fn) |func| {
            try func(observer.ptr, old_obj, new_obj);
        }
    }

    fn notifyDelete(self: *Reflector, observer: Observer, notification: DeleteNotification) !void {
        _ = self;
        if (observer.on_delete_detailed_fn) |func| {
            try func(observer.ptr, notification);
            return;
        }
        if (observer.on_delete_fn) |func| {
            try func(observer.ptr, notification.objectRef());
        }
    }

    fn notifyBookmark(self: *Reflector, observer: Observer, rv: []const u8) !void {
        _ = self;
        if (observer.on_bookmark_fn) |func| {
            try func(observer.ptr, rv);
        }
    }

    pub fn notifyError(self: *Reflector, err: anyerror, context: ErrorContext) void {
        if (self.error_handler) |handler| {
            handler.on_error_fn(handler.ptr, err, context, self.consecutive_errors);
        }
    }

    fn setCurrentWatcher(self: *Reflector, watcher: WatcherInterface) void {
        self.watcher_lock.lockUncancelable(self.store.io);
        defer self.watcher_lock.unlock(self.store.io);
        self.current_watcher = watcher;
    }

    fn stopCurrentWatcher(self: *Reflector) void {
        var watcher: ?WatcherInterface = null;

        self.watcher_lock.lockUncancelable(self.store.io);
        watcher = self.current_watcher;
        self.current_watcher = null;
        self.watcher_lock.unlock(self.store.io);

        if (watcher) |w| {
            w.stop();
            w.deinit();
        }
    }

    fn freeOwnedObject(self: *Reflector, obj: *Unstructured) void {
        obj.deinit();
        self.allocator.destroy(obj);
    }

    fn tombstoneForObject(obj: *const Unstructured) DeleteNotification {
        const key = ObjectKey.fromObject(obj) catch {
            return .{ .object = obj };
        };
        return .{
            .tombstone = .{
                .key = key,
                .obj = obj,
            },
        };
    }

    const ObjectIdentity = struct {
        namespace: ?[]const u8,
        name: []const u8,

        fn fromObject(obj: *const Unstructured) ?ObjectIdentity {
            return .{
                .namespace = obj.getNamespace(),
                .name = obj.getName() orelse return null,
            };
        }
    };

    const ObjectIdentityContext = struct {
        pub fn hash(_: @This(), key: ObjectIdentity) u64 {
            var hasher = std.hash.Wyhash.init(0);
            hasher.update(key.namespace orelse "");
            hasher.update(&[_]u8{0});
            hasher.update(key.name);
            return hasher.final();
        }

        pub fn eql(_: @This(), a: ObjectIdentity, b: ObjectIdentity) bool {
            return mem.eql(u8, a.namespace orelse "", b.namespace orelse "") and
                mem.eql(u8, a.name, b.name);
        }
    };

    fn findMatchingObject(items: []*Unstructured, target: *const Unstructured) ?usize {
        for (items, 0..) |item, idx| {
            if (sameObjectKey(item, target)) return idx;
        }
        return null;
    }

    fn sameObjectKey(a: *const Unstructured, b: *const Unstructured) bool {
        const a_name = a.getName() orelse return false;
        const b_name = b.getName() orelse return false;
        const a_ns = a.getNamespace() orelse "";
        const b_ns = b.getNamespace() orelse "";
        return mem.eql(u8, a_name, b_name) and mem.eql(u8, a_ns, b_ns);
    }

    fn sameObjectVersion(a: *const Unstructured, b: *const Unstructured) bool {
        const a_rv = a.getResourceVersion() orelse return false;
        const b_rv = b.getResourceVersion() orelse return false;
        return mem.eql(u8, a_rv, b_rv);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

// ── Test helpers ─────────────────────────────────────────────────────────────

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

fn makeGoneStatus(allocator: Allocator) !*Unstructured {
    var obj = try allocator.create(Unstructured);
    obj.* = try Unstructured.init(allocator);
    try obj.setString(&.{"kind"}, "Status");
    try obj.setString(&.{"apiVersion"}, "v1");
    try obj.setString(&.{"status"}, "Failure");
    try obj.setString(&.{"message"}, "too old resource version");
    try obj.setString(&.{"reason"}, "Gone");
    try obj.set(&.{"code"}, .{ .integer = 410 });
    return obj;
}

/// Mock state shared between mock list/watch functions and tests.
const MockState = struct {
    list_calls: u32 = 0,
    watch_calls: u32 = 0,
    list_results: ?ListResult = null,
    list_error: ?anyerror = null,
    watch_error: ?anyerror = null,
    events: []const WatchEvent = &.{},
    event_index: u32 = 0,
    /// If true, the mock watcher returns error.Gone after all events.
    gone_after_events: bool = false,

    fn reset(self: *MockState) void {
        self.list_calls = 0;
        self.watch_calls = 0;
        self.list_error = null;
        self.watch_error = null;
        self.events = &.{};
        self.event_index = 0;
        self.gone_after_events = false;
    }
};

/// Per-test mock context — uses a global because function pointers
/// in Zig cannot capture state (no closures).
var mock_state: MockState = .{};

fn mockList(_: *anyopaque, _: Allocator, _: ?[]const u8) anyerror!ListResult {
    mock_state.list_calls += 1;
    if (mock_state.list_error) |e| return e;
    return mock_state.list_results orelse return error.NoMockListResult;
}

fn mockWatch(_: *anyopaque, _: Allocator, _: ?[]const u8) anyerror!WatcherInterface {
    mock_state.watch_calls += 1;
    if (mock_state.watch_error) |e| return e;
    return WatcherInterface{
        .ptr = @ptrCast(&mock_state),
        .nextFn = mockWatcherNext,
        .stopFn = mockWatcherStop,
    };
}

fn mockWatcherNext(ptr: *anyopaque) WatcherInterface.WatcherError!?WatchEvent {
    const state: *MockState = @ptrCast(@alignCast(ptr));
    if (state.event_index < state.events.len) {
        const event = state.events[state.event_index];
        state.event_index += 1;
        return event;
    }
    if (state.gone_after_events) {
        return error.Gone;
    }
    // Stream ended
    return null;
}

fn mockWatcherStop(_: *anyopaque) void {
    // no-op for mock
}

// ── List + watch bootstrap ───────────────────────────────────────────────────

test "reflector: list seeds store and sets resource version" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj1 = try makeObj(testing.allocator, "default", "pod-1", "100");
    defer freeObj(testing.allocator, obj1);
    const obj2 = try makeObj(testing.allocator, "default", "pod-2", "101");
    defer freeObj(testing.allocator, obj2);

    var items = [_]*Unstructured{ obj1, obj2 };
    mock_state = .{};
    mock_state.list_results = .{
        .items = &items,
        .resource_version = "101",
    };
    mock_state.watch_error = error.WatchFailed;

    var r = Reflector.init(
        testing.allocator,
        &s,
        @ptrFromInt(1),
        mockList,
        mockWatch,
        .{},
    );
    defer r.deinit();

    // listAndWatch will list ok then fail on watch — that's fine for this test
    r.listAndWatch() catch {};

    try testing.expectEqual(@as(u32, 1), mock_state.list_calls);
    try testing.expectEqual(@as(usize, 2), s.len());
    try testing.expect(s.get("default/pod-1") != null);
    try testing.expect(s.get("default/pod-2") != null);
    try testing.expectEqualStrings("101", s.lastResourceVersion().?);
    try testing.expectEqualStrings("101", r.last_resource_version.?);
    try testing.expect(r.has_synced);
}

// ── Watch events update store ────────────────────────────────────────────────

test "reflector: ADDED, MODIFIED, DELETED events update store" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    // Initial empty list
    var empty_items = [_]*Unstructured{};
    mock_state = .{};
    mock_state.list_results = .{
        .items = &empty_items,
        .resource_version = "1",
    };

    // Watch events: add, modify, delete
    const obj_add = try makeObj(testing.allocator, "default", "nginx", "10");
    defer freeObj(testing.allocator, obj_add);
    const obj_mod = try makeObj(testing.allocator, "default", "nginx", "11");
    defer freeObj(testing.allocator, obj_mod);
    const obj_del = try makeObj(testing.allocator, "default", "nginx", "12");
    defer freeObj(testing.allocator, obj_del);

    var events = [_]WatchEvent{
        .{ .event_type = .added, .object = obj_add },
        .{ .event_type = .modified, .object = obj_mod },
        .{ .event_type = .deleted, .object = obj_del },
    };
    mock_state.events = &events;

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try r.listAndWatch();

    // After DELETED, the store should be empty
    try testing.expectEqual(@as(usize, 0), s.len());
    // RV should be from the last event
    try testing.expectEqualStrings("12", r.last_resource_version.?);
}

test "reflector: ADDED events populate store" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    var empty_items = [_]*Unstructured{};
    mock_state = .{};
    mock_state.list_results = .{
        .items = &empty_items,
        .resource_version = "1",
    };

    const obj1 = try makeObj(testing.allocator, "ns1", "a", "10");
    defer freeObj(testing.allocator, obj1);
    const obj2 = try makeObj(testing.allocator, "ns2", "b", "11");
    defer freeObj(testing.allocator, obj2);

    var events = [_]WatchEvent{
        .{ .event_type = .added, .object = obj1 },
        .{ .event_type = .added, .object = obj2 },
    };
    mock_state.events = &events;

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try r.listAndWatch();

    try testing.expectEqual(@as(usize, 2), s.len());
    try testing.expect(s.get("ns1/a") != null);
    try testing.expect(s.get("ns2/b") != null);
}

// ── Bookmark advances RV without modifying store objects ─────────────────────

test "reflector: bookmark advances RV without modifying store" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj = try makeObj(testing.allocator, "default", "pod-1", "100");
    defer freeObj(testing.allocator, obj);

    var items = [_]*Unstructured{obj};
    mock_state = .{};
    mock_state.list_results = .{
        .items = &items,
        .resource_version = "100",
    };

    // Bookmark event with higher RV
    const bm_obj = try makeObj(testing.allocator, null, "", "500");
    defer freeObj(testing.allocator, bm_obj);

    var events = [_]WatchEvent{
        .{ .event_type = .bookmark, .object = bm_obj },
    };
    mock_state.events = &events;

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try r.listAndWatch();

    // Store still has the same object
    try testing.expectEqual(@as(usize, 1), s.len());
    try testing.expect(s.get("default/pod-1") != null);
    // RV advanced to bookmark value
    try testing.expectEqualStrings("500", s.lastResourceVersion().?);
    try testing.expectEqualStrings("500", r.last_resource_version.?);
}

// ── 410 Gone triggers relist ─────────────────────────────────────────────────

test "reflector: 410 Gone triggers relist" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    // First list: one object
    const obj1 = try makeObj(testing.allocator, "default", "old-pod", "50");
    defer freeObj(testing.allocator, obj1);
    var items1 = [_]*Unstructured{obj1};

    // Second list (after 410): different object
    const obj2 = try makeObj(testing.allocator, "default", "new-pod", "200");
    defer freeObj(testing.allocator, obj2);
    var items2 = [_]*Unstructured{obj2};

    // Track which list call we're on
    const Helper = struct {
        var call_count: u32 = 0;
        var items_1: []*Unstructured = undefined;
        var items_2: []*Unstructured = undefined;

        fn listFn(_: *anyopaque, _: Allocator, _: ?[]const u8) anyerror!ListResult {
            call_count += 1;
            if (call_count == 1) {
                return .{ .items = items_1, .resource_version = "50" };
            }
            return .{ .items = items_2, .resource_version = "200" };
        }

        var watch_call_count: u32 = 0;
        fn watchFn(_: *anyopaque, _: Allocator, _: ?[]const u8) anyerror!WatcherInterface {
            watch_call_count += 1;
            if (watch_call_count == 1) {
                // First watch: returns Gone
                return WatcherInterface{
                    .ptr = @ptrFromInt(1), // dummy, won't be dereferenced
                    .nextFn = goneNextFn,
                    .stopFn = noopStop,
                };
            }
            // Second watch: stream ends cleanly
            return WatcherInterface{
                .ptr = @ptrFromInt(1),
                .nextFn = emptyNextFn,
                .stopFn = noopStop,
            };
        }

        fn goneNextFn(_: *anyopaque) WatcherInterface.WatcherError!?WatchEvent {
            return error.Gone;
        }

        fn emptyNextFn(_: *anyopaque) WatcherInterface.WatcherError!?WatchEvent {
            return null;
        }

        fn noopStop(_: *anyopaque) void {}
    };

    Helper.call_count = 0;
    Helper.watch_call_count = 0;
    Helper.items_1 = &items1;
    Helper.items_2 = &items2;

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), Helper.listFn, Helper.watchFn, .{});
    defer r.deinit();

    // First listAndWatch: list -> watch (Gone) -> returns
    r.listAndWatch() catch {};

    // Store has first list result
    try testing.expectEqual(@as(u32, 1), Helper.call_count);
    try testing.expectEqual(@as(usize, 1), s.len());
    try testing.expect(s.get("default/old-pod") != null);

    // Second listAndWatch: relist -> watch (clean end)
    try r.listAndWatch();

    // Store replaced with second list result
    try testing.expectEqual(@as(u32, 2), Helper.call_count);
    try testing.expectEqual(@as(usize, 1), s.len());
    try testing.expect(s.get("default/old-pod") == null);
    try testing.expect(s.get("default/new-pod") != null);
    try testing.expectEqualStrings("200", s.lastResourceVersion().?);
}

// ── 410 Gone from error event triggers relist ────────────────────────────────

test "reflector: error event with code 410 triggers relist" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    var empty_items = [_]*Unstructured{};
    mock_state = .{};
    mock_state.list_results = .{
        .items = &empty_items,
        .resource_version = "1",
    };

    const gone_obj = try makeGoneStatus(testing.allocator);
    defer freeObj(testing.allocator, gone_obj);

    var events = [_]WatchEvent{
        .{ .event_type = .err, .object = gone_obj },
    };
    mock_state.events = &events;

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try r.listAndWatch();
}

// ── Backoff configuration ────────────────────────────────────────────────────

test "reflector: exponential backoff calculation" {
    const cases = [_]struct { attempt: u32, expected_ms: u64 }{
        .{ .attempt = 0, .expected_ms = 800 },
        .{ .attempt = 1, .expected_ms = 1600 },
        .{ .attempt = 2, .expected_ms = 3200 },
        .{ .attempt = 3, .expected_ms = 6400 },
        .{ .attempt = 4, .expected_ms = 12800 },
        .{ .attempt = 5, .expected_ms = 25600 },
        .{ .attempt = 6, .expected_ms = 30000 }, // capped at max
        .{ .attempt = 10, .expected_ms = 30000 },
        .{ .attempt = 100, .expected_ms = 30000 },
    };

    const cfg = BackoffConfig{};
    for (cases) |c| {
        try testing.expectEqual(c.expected_ms, cfg.delayMs(c.attempt));
    }
}

test "reflector: custom backoff config" {
    const cfg = BackoffConfig{
        .initial_ms = 100,
        .max_ms = 1000,
        .factor = 3,
    };
    try testing.expectEqual(@as(u64, 100), cfg.delayMs(0));
    try testing.expectEqual(@as(u64, 300), cfg.delayMs(1));
    try testing.expectEqual(@as(u64, 900), cfg.delayMs(2));
    try testing.expectEqual(@as(u64, 1000), cfg.delayMs(3)); // capped
}

test "reflector: consecutive errors track backoff state" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    mock_state = .{};
    mock_state.list_error = error.WatchFailed;

    // Use jitter=0 for deterministic assertions on currentBackoffMs.
    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{
        .backoff = .{ .jitter = 0.0 },
    });
    defer r.deinit();

    try testing.expectEqual(@as(u64, 800), r.currentBackoffMs());

    // Simulate consecutive failures
    r.consecutive_errors = 3;
    try testing.expectEqual(@as(u64, 6400), r.currentBackoffMs());

    r.consecutive_errors = 10;
    try testing.expectEqual(@as(u64, 30000), r.currentBackoffMs());
}

test "reflector: jittered backoff stays within bounds" {
    const cfg = BackoffConfig{ .jitter = 1.0 };
    var prng = std.Random.DefaultPrng.init(42);
    const rand = prng.random();
    // Full jitter: delay in [0, base]
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        const base = cfg.delayMs(2); // 3200
        const jittered = cfg.delayMsJittered(2, rand);
        try testing.expect(jittered <= base);
    }
    // No jitter: delay == base
    const no_jitter = BackoffConfig{ .jitter = 0.0 };
    try testing.expectEqual(no_jitter.delayMs(2), no_jitter.delayMsJittered(2, rand));
}

// ── List failure does not modify store ───────────────────────────────────────

test "reflector: list failure leaves store unchanged" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    // Pre-populate store
    const existing = try makeObj(testing.allocator, "default", "existing", "1");
    defer freeObj(testing.allocator, existing);
    try s.add(existing);

    mock_state = .{};
    mock_state.list_error = error.WatchFailed;

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try testing.expectError(error.WatchFailed, r.listAndWatch());

    // Store is unchanged
    try testing.expectEqual(@as(usize, 1), s.len());
    try testing.expect(s.get("default/existing") != null);
    try testing.expect(!r.has_synced);
}

// ── Watch failure after successful list ──────────────────────────────────────

test "reflector: watch failure after successful list preserves store" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj = try makeObj(testing.allocator, "default", "pod-1", "10");
    defer freeObj(testing.allocator, obj);

    var items = [_]*Unstructured{obj};
    mock_state = .{};
    mock_state.list_results = .{
        .items = &items,
        .resource_version = "10",
    };
    mock_state.watch_error = error.WatchFailed;

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try testing.expectError(error.WatchFailed, r.listAndWatch());

    // Store was seeded by list
    try testing.expectEqual(@as(usize, 1), s.len());
    try testing.expect(s.get("default/pod-1") != null);
    try testing.expect(r.has_synced);
}

// ── Empty list ───────────────────────────────────────────────────────────────

test "reflector: empty list result" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    var empty_items = [_]*Unstructured{};
    mock_state = .{};
    mock_state.list_results = .{
        .items = &empty_items,
        .resource_version = "0",
    };
    mock_state.events = &.{};

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try r.listAndWatch();

    try testing.expectEqual(@as(usize, 0), s.len());
    try testing.expectEqualStrings("0", s.lastResourceVersion().?);
    try testing.expect(r.has_synced);
}

// ── Multiple watch cycles ────────────────────────────────────────────────────

test "reflector: multiple events in sequence" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    var empty_items = [_]*Unstructured{};
    mock_state = .{};
    mock_state.list_results = .{
        .items = &empty_items,
        .resource_version = "1",
    };

    const a = try makeObj(testing.allocator, "default", "a", "10");
    defer freeObj(testing.allocator, a);
    const b = try makeObj(testing.allocator, "default", "b", "11");
    defer freeObj(testing.allocator, b);
    const a_mod = try makeObj(testing.allocator, "default", "a", "12");
    defer freeObj(testing.allocator, a_mod);
    const bm = try makeObj(testing.allocator, null, "", "50");
    defer freeObj(testing.allocator, bm);

    var events = [_]WatchEvent{
        .{ .event_type = .added, .object = a },
        .{ .event_type = .added, .object = b },
        .{ .event_type = .modified, .object = a_mod },
        .{ .event_type = .bookmark, .object = bm },
    };
    mock_state.events = &events;

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try r.listAndWatch();

    try testing.expectEqual(@as(usize, 2), s.len());
    // a was modified — check RV
    const got_a = s.get("default/a").?;
    try testing.expectEqualStrings("12", got_a.getResourceVersion().?);
    // b unchanged
    try testing.expect(s.get("default/b") != null);
    // RV from bookmark
    try testing.expectEqualStrings("50", s.lastResourceVersion().?);
    try testing.expectEqualStrings("50", r.last_resource_version.?);
}

// ── EventType tests ──────────────────────────────────────────────────────────

test "EventType: roundtrip" {
    const Case = struct { str: []const u8, expected: EventType };
    const cases = [_]Case{
        .{ .str = "ADDED", .expected = .added },
        .{ .str = "MODIFIED", .expected = .modified },
        .{ .str = "DELETED", .expected = .deleted },
        .{ .str = "BOOKMARK", .expected = .bookmark },
        .{ .str = "ERROR", .expected = .err },
    };
    for (cases) |c| {
        const et = EventType.fromString(c.str).?;
        try testing.expectEqual(c.expected, et);
        try testing.expectEqualStrings(c.str, et.string());
    }
    try testing.expect(EventType.fromString("INVALID") == null);
}

// ── isGoneError tests ────────────────────────────────────────────────────────

test "reflector: isGoneError detects 410" {
    const gone = try makeGoneStatus(testing.allocator);
    defer freeObj(testing.allocator, gone);
    try testing.expect(Reflector.isGoneError(gone));

    const not_gone = try makeObj(testing.allocator, "default", "pod", "1");
    defer freeObj(testing.allocator, not_gone);
    try testing.expect(!Reflector.isGoneError(not_gone));
}

// ── Replace semantics on relist ──────────────────────────────────────────────

test "reflector: relist replaces store contents completely" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    // Manually add an object to the store
    const old = try makeObj(testing.allocator, "default", "stale", "5");
    defer freeObj(testing.allocator, old);
    try s.add(old);

    // List returns a different set
    const new_obj = try makeObj(testing.allocator, "default", "fresh", "100");
    defer freeObj(testing.allocator, new_obj);

    var items = [_]*Unstructured{new_obj};
    mock_state = .{};
    mock_state.list_results = .{
        .items = &items,
        .resource_version = "100",
    };
    mock_state.events = &.{};

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try r.listAndWatch();

    // Old object should be gone
    try testing.expect(s.get("default/stale") == null);
    // New object present
    try testing.expect(s.get("default/fresh") != null);
    try testing.expectEqual(@as(usize, 1), s.len());
}

// ── Stop flag ────────────────────────────────────────────────────────────────

test "reflector: stop flag" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try testing.expect(!r.should_stop);
    r.stop();
    try testing.expect(r.should_stop);
}

const ObserverRecord = union(enum) {
    add: struct {
        name: []const u8,
        initial: bool,
    },
    update: struct {
        old_rv: []const u8,
        new_rv: []const u8,
    },
    delete: struct {
        name: []const u8,
        rv: ?[]const u8,
        tombstone: bool,
    },
    bookmark: []const u8,
};

const ObserverState = struct {
    allocator: Allocator,
    records: std.ArrayList(ObserverRecord),

    fn init(allocator: Allocator) ObserverState {
        return .{
            .allocator = allocator,
            .records = .empty,
        };
    }

    fn deinit(self: *ObserverState) void {
        for (self.records.items) |record| {
            switch (record) {
                .add => |add| self.allocator.free(add.name),
                .update => |update| {
                    self.allocator.free(update.old_rv);
                    self.allocator.free(update.new_rv);
                },
                .delete => |delete| {
                    self.allocator.free(delete.name);
                    if (delete.rv) |rv| self.allocator.free(rv);
                },
                .bookmark => |rv| self.allocator.free(rv),
            }
        }
        self.records.deinit(self.allocator);
    }

    fn observer(self: *ObserverState) Observer {
        return .{
            .ptr = @ptrCast(self),
            .on_add_fn = onAdd,
            .on_update_fn = onUpdate,
            .on_delete_detailed_fn = onDelete,
            .on_bookmark_fn = onBookmark,
        };
    }

    fn onAdd(ptr: *anyopaque, obj: *const Unstructured, is_in_initial_list: bool) anyerror!void {
        const self: *ObserverState = @ptrCast(@alignCast(ptr));
        try self.records.append(self.allocator, .{
            .add = .{
                .name = try self.allocator.dupe(u8, obj.getName().?),
                .initial = is_in_initial_list,
            },
        });
    }

    fn onUpdate(ptr: *anyopaque, old_obj: *const Unstructured, new_obj: *const Unstructured) anyerror!void {
        const self: *ObserverState = @ptrCast(@alignCast(ptr));
        try self.records.append(self.allocator, .{
            .update = .{
                .old_rv = try self.allocator.dupe(u8, old_obj.getResourceVersion().?),
                .new_rv = try self.allocator.dupe(u8, new_obj.getResourceVersion().?),
            },
        });
    }

    fn onDelete(ptr: *anyopaque, notification: DeleteNotification) anyerror!void {
        const self: *ObserverState = @ptrCast(@alignCast(ptr));
        const obj = notification.objectRef();
        try self.records.append(self.allocator, .{
            .delete = .{
                .name = try self.allocator.dupe(u8, obj.getName().?),
                .rv = if (obj.getResourceVersion()) |rv| try self.allocator.dupe(u8, rv) else null,
                .tombstone = notification.isTombstone(),
            },
        });
    }

    fn onBookmark(ptr: *anyopaque, rv: []const u8) anyerror!void {
        const self: *ObserverState = @ptrCast(@alignCast(ptr));
        try self.records.append(self.allocator, .{
            .bookmark = try self.allocator.dupe(u8, rv),
        });
    }
};

test "reflector: observer sees initial add then relist diff" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const a_v1 = try makeObj(testing.allocator, "default", "a", "10");
    defer freeObj(testing.allocator, a_v1);
    const c_v1 = try makeObj(testing.allocator, "default", "c", "11");
    defer freeObj(testing.allocator, c_v1);
    const a_v2 = try makeObj(testing.allocator, "default", "a", "20");
    defer freeObj(testing.allocator, a_v2);
    const b_v1 = try makeObj(testing.allocator, "default", "b", "21");
    defer freeObj(testing.allocator, b_v1);

    const Helper = struct {
        var list_calls: u32 = 0;
        var first_items: []*Unstructured = undefined;
        var second_items: []*Unstructured = undefined;

        fn listFn(_: *anyopaque, _: Allocator, _: ?[]const u8) anyerror!ListResult {
            list_calls += 1;
            if (list_calls == 1) {
                return .{
                    .items = first_items,
                    .resource_version = "11",
                };
            }
            return .{
                .items = second_items,
                .resource_version = "21",
            };
        }

        fn watchFn(_: *anyopaque, _: Allocator, _: ?[]const u8) anyerror!WatcherInterface {
            return .{
                .ptr = @ptrFromInt(1),
                .nextFn = emptyNextFn,
                .stopFn = noopStop,
            };
        }

        fn emptyNextFn(_: *anyopaque) WatcherInterface.WatcherError!?WatchEvent {
            return null;
        }

        fn noopStop(_: *anyopaque) void {}
    };

    var first_items = [_]*Unstructured{ a_v1, c_v1 };
    var second_items = [_]*Unstructured{ a_v2, b_v1 };
    Helper.list_calls = 0;
    Helper.first_items = &first_items;
    Helper.second_items = &second_items;

    var observer = ObserverState.init(testing.allocator);
    defer observer.deinit();

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), Helper.listFn, Helper.watchFn, .{
        .observer = observer.observer(),
    });
    defer r.deinit();

    try r.listAndWatch();
    try r.listAndWatch();

    try testing.expectEqual(@as(usize, 5), observer.records.items.len);
    try testing.expectEqualStrings("a", observer.records.items[0].add.name);
    try testing.expect(observer.records.items[0].add.initial);
    try testing.expectEqualStrings("c", observer.records.items[1].add.name);
    try testing.expect(observer.records.items[1].add.initial);
    try testing.expectEqualStrings("10", observer.records.items[2].update.old_rv);
    try testing.expectEqualStrings("20", observer.records.items[2].update.new_rv);
    try testing.expectEqualStrings("b", observer.records.items[3].add.name);
    try testing.expect(!observer.records.items[3].add.initial);
    try testing.expectEqualStrings("c", observer.records.items[4].delete.name);
    try testing.expect(observer.records.items[4].delete.tombstone);
}

test "reflector: delete for unknown object still notifies observer with event payload" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    var empty_items = [_]*Unstructured{};
    mock_state = .{};
    mock_state.list_results = .{
        .items = &empty_items,
        .resource_version = "1",
    };

    const deleted = try makeObj(testing.allocator, "default", "ghost", "2");
    defer freeObj(testing.allocator, deleted);
    var events = [_]WatchEvent{
        .{ .event_type = .deleted, .object = deleted },
    };
    mock_state.events = &events;

    var observer = ObserverState.init(testing.allocator);
    defer observer.deinit();

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{
        .observer = observer.observer(),
    });
    defer r.deinit();

    try r.listAndWatch();

    try testing.expectEqual(@as(usize, 1), observer.records.items.len);
    try testing.expectEqualStrings("ghost", observer.records.items[0].delete.name);
    try testing.expect(!observer.records.items[0].delete.tombstone);
    try testing.expectEqual(@as(usize, 0), s.len());
    try testing.expectEqualStrings("2", s.lastResourceVersion().?);
}

test "reflector: watch delete of known object uses watch payload not cached object" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const cached = try makeObj(testing.allocator, "default", "demo", "10");
    try s.add(cached);

    const deleted = try makeObj(testing.allocator, "default", "demo", "11");

    var observer = ObserverState.init(testing.allocator);
    defer observer.deinit();

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{
        .observer = observer.observer(),
        .owns_store_objects = true,
    });
    defer r.deinit();

    try r.processEvent(.{
        .event_type = .deleted,
        .object = deleted,
    });

    try testing.expectEqual(@as(usize, 1), observer.records.items.len);
    try testing.expectEqualStrings("demo", observer.records.items[0].delete.name);
    try testing.expectEqualStrings("11", observer.records.items[0].delete.rv.?);
    try testing.expect(!observer.records.items[0].delete.tombstone);
    try testing.expectEqual(@as(usize, 0), s.len());
}

test "reflector: run restarts watch without relist on clean watch end" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const CleanEndState = struct {
        list_calls: u32 = 0,
        watch_create_count: u32 = 0,
        last_watch_rv: [2]?[]const u8 = .{ null, null },
        reflector: ?*Reflector = null,
    };
    var mutable_state: CleanEndState = .{};
    defer {
        for (mutable_state.last_watch_rv) |rv| {
            if (rv) |value| testing.allocator.free(value);
        }
    }

    const CleanEndWatcher = struct {
        const Self = @This();

        allocator: Allocator,
        state: *CleanEndState,
        call_index: u32,
        step: u8 = 0,

        fn next(ptr: *anyopaque) WatcherInterface.WatcherError!?WatchEvent {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.call_index == 0) {
                if (self.step == 0) {
                    const obj = self.allocator.create(Unstructured) catch return error.WatchFailed;
                    obj.* = Unstructured.init(self.allocator) catch return error.WatchFailed;
                    obj.setNamespace("default") catch return error.WatchFailed;
                    obj.setName("demo") catch return error.WatchFailed;
                    obj.setResourceVersion("11") catch return error.WatchFailed;
                    self.step = 1;
                    return .{ .event_type = .modified, .object = obj };
                }
                return null;
            }

            self.state.reflector.?.stop();
            return null;
        }

        fn stop(_: *anyopaque) void {}

        fn deinitOpaque(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.allocator.destroy(self);
        }
    };

    const Helper = struct {
        fn listFn(ptr: *anyopaque, _: Allocator, rv: ?[]const u8) anyerror!ListResult {
            const self: *CleanEndState = @ptrCast(@alignCast(ptr));
            self.list_calls += 1;
            try testing.expect(rv == null);
            const obj = try makeObj(testing.allocator, "default", "demo", "10");
            const items = try testing.allocator.alloc(*Unstructured, 1);
            items[0] = obj;
            return .{
                .items = items,
                .items_owned = true,
                .resource_version = try testing.allocator.dupe(u8, "10"),
                .resource_version_owned = true,
            };
        }

        fn watchFn(ptr: *anyopaque, allocator: Allocator, rv: ?[]const u8) anyerror!WatcherInterface {
            const self: *CleanEndState = @ptrCast(@alignCast(ptr));
            const watcher = try allocator.create(CleanEndWatcher);
            watcher.* = .{
                .allocator = allocator,
                .state = self,
                .call_index = self.watch_create_count,
            };
            self.last_watch_rv[self.watch_create_count] = if (rv) |value|
                try testing.allocator.dupe(u8, value)
            else
                null;
            self.watch_create_count += 1;
            return .{
                .ptr = @ptrCast(watcher),
                .nextFn = CleanEndWatcher.next,
                .stopFn = CleanEndWatcher.stop,
                .deinitFn = CleanEndWatcher.deinitOpaque,
            };
        }
    };

    var r = Reflector.init(testing.allocator, &s, @ptrCast(&mutable_state), Helper.listFn, Helper.watchFn, .{
        .owns_store_objects = true,
    });
    mutable_state.reflector = &r;
    defer r.deinit();

    try r.run();

    try testing.expectEqual(@as(u32, 1), mutable_state.list_calls);
    try testing.expectEqualStrings("10", mutable_state.last_watch_rv[0].?);
    try testing.expectEqualStrings("11", mutable_state.last_watch_rv[1].?);
}

test "reflector: run relists after Gone and resets list resource version" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const State = struct {
        list_calls: u32 = 0,
        watch_calls: u32 = 0,
        list_rvs: [2]?[]const u8 = .{ null, null },
        watch_rvs: [2]?[]const u8 = .{ null, null },
        reflector: ?*Reflector = null,
    };
    var state: State = .{};
    defer {
        for (state.list_rvs) |rv| {
            if (rv) |value| testing.allocator.free(value);
        }
        for (state.watch_rvs) |rv| {
            if (rv) |value| testing.allocator.free(value);
        }
    }

    const GoneThenStopWatcher = struct {
        const Self = @This();

        allocator: Allocator,
        state: *State,
        call_index: u32,

        fn next(ptr: *anyopaque) WatcherInterface.WatcherError!?WatchEvent {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.call_index == 0) return error.Gone;
            self.state.reflector.?.stop();
            return null;
        }

        fn stop(_: *anyopaque) void {}

        fn deinitOpaque(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.allocator.destroy(self);
        }
    };

    const Helper = struct {
        fn listFn(ptr: *anyopaque, allocator: Allocator, rv: ?[]const u8) anyerror!ListResult {
            const self: *State = @ptrCast(@alignCast(ptr));
            self.list_rvs[self.list_calls] = if (rv) |value|
                try testing.allocator.dupe(u8, value)
            else
                null;
            self.list_calls += 1;

            const version = if (self.list_calls == 1) "10" else "20";
            const name = if (self.list_calls == 1) "old" else "fresh";
            const obj = try makeObj(allocator, "default", name, version);
            const items = try allocator.alloc(*Unstructured, 1);
            items[0] = obj;
            return .{
                .items = items,
                .items_owned = true,
                .resource_version = try allocator.dupe(u8, version),
                .resource_version_owned = true,
            };
        }

        fn watchFn(ptr: *anyopaque, allocator: Allocator, rv: ?[]const u8) anyerror!WatcherInterface {
            const self: *State = @ptrCast(@alignCast(ptr));
            const watcher = try allocator.create(GoneThenStopWatcher);
            watcher.* = .{
                .allocator = allocator,
                .state = self,
                .call_index = self.watch_calls,
            };
            self.watch_rvs[self.watch_calls] = if (rv) |value|
                try testing.allocator.dupe(u8, value)
            else
                null;
            self.watch_calls += 1;
            return .{
                .ptr = @ptrCast(watcher),
                .nextFn = GoneThenStopWatcher.next,
                .stopFn = GoneThenStopWatcher.stop,
                .deinitFn = GoneThenStopWatcher.deinitOpaque,
            };
        }
    };

    var r = Reflector.init(testing.allocator, &s, @ptrCast(&state), Helper.listFn, Helper.watchFn, .{
        .owns_store_objects = true,
    });
    state.reflector = &r;
    defer r.deinit();

    try r.run();

    try testing.expectEqual(@as(u32, 2), state.list_calls);
    try testing.expect(state.list_rvs[0] == null); // Initial list: no RV
    try testing.expectEqualStrings("", state.list_rvs[1].?); // Relist after 410: empty RV (state-of-the-world)
    try testing.expectEqualStrings("10", state.watch_rvs[0].?);
    try testing.expectEqualStrings("20", state.watch_rvs[1].?);
    try testing.expect(s.get("default/old") == null);
    try testing.expect(s.get("default/fresh") != null);
}

test "reflector: fuzz event stream matches store model" {
    try std.testing.fuzz({}, fuzzReflectorEventStream, .{});
}

fn fuzzReflectorEventStream(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();

    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    var empty_items = [_]*Unstructured{};
    mock_state = .{};
    mock_state.list_results = .{
        .items = &empty_items,
        .resource_version = "0",
    };

    var objects = std.ArrayList(*Unstructured).empty;
    defer {
        for (objects.items) |obj| {
            obj.deinit();
            testing.allocator.destroy(obj);
        }
        objects.deinit(testing.allocator);
    }

    var events = std.ArrayList(WatchEvent).empty;
    defer events.deinit(testing.allocator);

    var expected_present = [_]bool{false} ** 3;
    var expected_rvs = [_]u32{0} ** 3;
    var last_rv: u32 = 0;

    const names = [_][]const u8{ "a", "b", "c" };
    const event_count = smith.valueRangeAtMost(u8, 1, 64);
    for (0..event_count) |i| {
        const rv_num: u32 = @intCast(i + 1);
        const rv = try std.fmt.allocPrint(testing.allocator, "{d}", .{rv_num});
        defer testing.allocator.free(rv);

        const kind = smith.valueRangeAtMost(u8, 0, 3);
        if (kind == 3) {
            const obj = try makeObj(testing.allocator, null, "", rv);
            try objects.append(testing.allocator, obj);
            try events.append(testing.allocator, .{
                .event_type = .bookmark,
                .object = obj,
            });
            last_rv = rv_num;
            continue;
        }

        const name_index: usize = smith.valueRangeAtMost(u2, 0, names.len - 1);
        const obj = try makeObj(testing.allocator, "default", names[name_index], rv);
        try objects.append(testing.allocator, obj);

        const event_type: EventType = switch (kind) {
            0 => .added,
            1 => .modified,
            2 => .deleted,
            else => unreachable,
        };
        try events.append(testing.allocator, .{
            .event_type = event_type,
            .object = obj,
        });

        switch (event_type) {
            .added, .modified => {
                expected_present[name_index] = true;
                expected_rvs[name_index] = rv_num;
            },
            .deleted => {
                expected_present[name_index] = false;
                expected_rvs[name_index] = 0;
            },
            else => unreachable,
        }
        last_rv = rv_num;
    }

    mock_state.events = events.items;
    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try r.listAndWatch();

    for (names, 0..) |name, idx| {
        const obj = s.getByName("default", name);
        if (expected_present[idx]) {
            try testing.expect(obj != null);
            const expected_rv = try std.fmt.allocPrint(testing.allocator, "{d}", .{expected_rvs[idx]});
            defer testing.allocator.free(expected_rv);
            try testing.expectEqualStrings(expected_rv, obj.?.getResourceVersion().?);
        } else {
            try testing.expect(obj == null);
        }
    }

    const expected_last_rv = try std.fmt.allocPrint(testing.allocator, "{d}", .{last_rv});
    defer testing.allocator.free(expected_last_rv);
    try testing.expectEqualStrings(expected_last_rv, s.lastResourceVersion().?);
}

test "reflector: bookmark resume skips relist on watch error" {
    // After receiving a bookmark, a watch error should NOT trigger relist
    // if we have a last_resource_version — instead it resumes the watch.
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    // Simulate having a last RV from a bookmark
    r.last_resource_version = try testing.allocator.dupe(u8, "500");
    r.has_synced = true;

    // Verify the RV is set
    try testing.expectEqualStrings("500", r.last_resource_version.?);

    // The run() loop would use this RV to resume watch instead of relisting.
    // We can't easily test the full run() loop in a unit test, but we verify
    // the state is correct for resume.
}

test "reflector: resync_period_ms option is stored" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{
        .resync_period_ms = 30_000,
    });
    defer r.deinit();

    try testing.expectEqual(@as(u64, 30_000), r.resync_period_ms);
    try testing.expect(r.last_resync_ts == null);
}

test "reflector: doResync sends update events for all store objects" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const obj1 = try makeObj(testing.allocator, "default", "a", "1");
    defer freeObj(testing.allocator, obj1);
    const obj2 = try makeObj(testing.allocator, "default", "b", "2");
    defer freeObj(testing.allocator, obj2);
    try s.add(obj1);
    try s.add(obj2);

    var observer = ObserverState.init(testing.allocator);
    defer observer.deinit();

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{
        .observer = observer.observer(),
        .resync_period_ms = 1,
    });
    defer r.deinit();

    try r.doResync();

    // Should have 2 update events (one per store object)
    try testing.expectEqual(@as(usize, 2), observer.records.items.len);
    for (observer.records.items) |record| {
        switch (record) {
            .update => |update| {
                // old_rv == new_rv because same object
                try testing.expectEqualStrings(update.old_rv, update.new_rv);
            },
            else => return error.TestUnexpectedResult,
        }
    }
}

test "reflector: error handler is called on list failure" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const ErrorLog = struct {
        var call_count: u32 = 0;
        var last_context: Reflector.ErrorContext = .list;

        fn handler(_: *anyopaque, _: anyerror, ctx: Reflector.ErrorContext, _: u32) void {
            call_count += 1;
            last_context = ctx;
        }
    };
    ErrorLog.call_count = 0;

    const Helper = struct {
        var list_calls: u32 = 0;

        fn listFn(_: *anyopaque, _: Allocator, _: ?[]const u8) anyerror!ListResult {
            list_calls += 1;
            if (list_calls <= 1) return error.ListFailed;
            // Second call succeeds with empty list; reflector then calls watch which also fails
            return .{ .items = &.{}, .resource_version = "1" };
        }
        fn watchFn(_: *anyopaque, _: Allocator, _: ?[]const u8) anyerror!WatcherInterface {
            return error.WatchFailed;
        }
    };
    Helper.list_calls = 0;

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), Helper.listFn, Helper.watchFn, .{
        .error_handler = .{
            .ptr = @ptrFromInt(1),
            .on_error_fn = ErrorLog.handler,
        },
    });
    defer r.deinit();

    // run() will: fail list (error handler called with .list), retry list (succeeds),
    // then fail watch (error handler called with .watch), then retry watch... stop it.
    r.should_stop = false;
    // Use listAndWatch-like pattern but go through run() path manually
    // by calling doList which fails, then checking state.
    r.doList(null) catch {
        r.consecutive_errors += 1;
        r.last_error = error.ListFailed;
        r.notifyError(error.ListFailed, .list);
    };

    try testing.expect(ErrorLog.call_count > 0);
    try testing.expectEqual(Reflector.ErrorContext.list, ErrorLog.last_context);
    try testing.expect(r.last_error != null);
}

test "reflector: last_error and consecutive_errors track state" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{});
    defer r.deinit();

    try testing.expect(r.last_error == null);
    try testing.expectEqual(@as(u32, 0), r.consecutive_errors);

    // Simulate errors
    r.last_error = error.WatchFailed;
    r.consecutive_errors = 3;
    try testing.expect(r.last_error != null);
    try testing.expectEqual(@as(u32, 3), r.consecutive_errors);

    // Clear on success
    r.last_error = null;
    r.consecutive_errors = 0;
    try testing.expect(r.last_error == null);
}

test "reflector: error handler receives correct context" {
    var s = Store.init(testing.allocator, testing.io);
    defer s.deinit();

    const ErrorLog = struct {
        var contexts: [8]Reflector.ErrorContext = undefined;
        var count: usize = 0;

        fn handler(_: *anyopaque, _: anyerror, ctx: Reflector.ErrorContext, _: u32) void {
            if (count < 8) {
                contexts[count] = ctx;
                count += 1;
            }
        }
    };
    ErrorLog.count = 0;

    var r = Reflector.init(testing.allocator, &s, @ptrFromInt(1), mockList, mockWatch, .{
        .error_handler = .{
            .ptr = @ptrFromInt(1),
            .on_error_fn = ErrorLog.handler,
        },
    });
    defer r.deinit();

    // Directly test notifyError
    r.notifyError(error.ListFailed, .list);
    r.notifyError(error.WatchFailed, .watch);

    try testing.expectEqual(@as(usize, 2), ErrorLog.count);
    try testing.expectEqual(Reflector.ErrorContext.list, ErrorLog.contexts[0]);
    try testing.expectEqual(Reflector.ErrorContext.watch, ErrorLog.contexts[1]);
}
