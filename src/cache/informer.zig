const std = @import("std");
const mem = std.mem;
const json = std.json;
const Io = std.Io;
const Allocator = mem.Allocator;
const testing = std.testing;

const k8s = @import("k8s_zig");
const client = @import("k8s_client");
const scheme = k8s.scheme;
const Unstructured = k8s.Unstructured;
const DynamicClient = client.DynamicClient;

const reflector_mod = @import("reflector.zig");
const Reflector = reflector_mod.Reflector;
const WatcherInterface = reflector_mod.WatcherInterface;
const WatchEvent = reflector_mod.WatchEvent;
pub const DeleteNotification = reflector_mod.DeleteNotification;
pub const DeletedFinalStateUnknown = reflector_mod.DeletedFinalStateUnknown;
const store_mod = @import("store.zig");
const Store = store_mod.Store;
const IndexFunc = store_mod.IndexFunc;

pub const ResourceEventHandler = struct {
    ptr: *anyopaque,
    on_add_fn: ?*const fn (ptr: *anyopaque, obj: *const Unstructured, is_in_initial_list: bool) anyerror!void = null,
    on_update_fn: ?*const fn (ptr: *anyopaque, old_obj: *const Unstructured, new_obj: *const Unstructured) anyerror!void = null,
    on_delete_fn: ?*const fn (ptr: *anyopaque, obj: *const Unstructured) anyerror!void = null,
    on_delete_detailed_fn: ?*const fn (ptr: *anyopaque, notification: DeleteNotification) anyerror!void = null,
    on_bookmark_fn: ?*const fn (ptr: *anyopaque, rv: []const u8) anyerror!void = null,
};

pub const Source = struct {
    ptr: *anyopaque,
    list_fn: reflector_mod.ListFn,
    watch_fn: reflector_mod.WatchFn,
    deinit_fn: ?*const fn (ptr: *anyopaque, allocator: Allocator) void = null,

    pub fn deinit(self: Source, allocator: Allocator) void {
        if (self.deinit_fn) |func| {
            func(self.ptr, allocator);
        }
    }
};

pub const Informer = struct {
    allocator: Allocator,
    io: Io,
    store: Store,
    reflector: Reflector,
    source: Source,
    handlers: std.ArrayList(ResourceEventHandler),
    handlers_lock: Io.Mutex,
    /// Cached handler snapshot — regenerated only when handlers change.
    /// Avoids per-event mutex + alloc in dispatch path.
    cached_handlers: ?[]ResourceEventHandler,
    handlers_generation: u32,
    dispatch_generation: u32,
    run_future: ?Io.Future(anyerror!void),
    started: bool,

    pub const SyncState = enum {
        /// Informer has not been started.
        not_started,
        /// Initial list+watch is in progress.
        syncing,
        /// At least one full list+watch cycle completed.
        synced,
        /// Synced previously but currently in error backoff.
        error_backoff,
    };

    pub const IndexerEntry = struct {
        name: []const u8,
        func: IndexFunc,
    };

    pub const Options = struct {
        backoff: reflector_mod.BackoffConfig = .{},
        owns_store_objects: bool = true,
        /// Error handler called on list/watch failures.
        error_handler: ?reflector_mod.Reflector.ErrorHandler = null,
        /// Resync period in milliseconds. 0 = disabled.
        resync_period_ms: u64 = 0,
        /// Secondary indexers to register on the store before start.
        indexers: []const IndexerEntry = &.{},
    };

    pub fn create(allocator: Allocator, io: Io, source: Source, opts: Options) !*Informer {
        var informer = try allocator.create(Informer);
        errdefer allocator.destroy(informer);

        informer.* = .{
            .allocator = allocator,
            .io = io,
            .store = Store.init(allocator, io),
            .reflector = undefined,
            .source = source,
            .handlers = .empty,
            .handlers_lock = .init,
            .cached_handlers = null,
            .handlers_generation = 0,
            .dispatch_generation = 0,
            .run_future = null,
            .started = false,
        };

        informer.reflector = Reflector.init(
            allocator,
            &informer.store,
            source.ptr,
            source.list_fn,
            source.watch_fn,
            .{
                .backoff = opts.backoff,
                .observer = makeObserver(informer),
                .owns_store_objects = opts.owns_store_objects,
                .error_handler = opts.error_handler,
                .resync_period_ms = opts.resync_period_ms,
            },
        );

        // Register indexers on the store before any objects are added.
        for (opts.indexers) |entry| {
            try informer.store.addIndexer(entry.name, entry.func);
        }

        return informer;
    }

    pub fn createDynamic(
        allocator: Allocator,
        io: Io,
        dynamic_client: *DynamicClient,
        gvr: scheme.GroupVersionResource,
        opts: DynamicOptions,
    ) !*Informer {
        const source = try DynamicSource.create(allocator, io, dynamic_client, gvr, opts);
        errdefer source.deinit(allocator);
        return Informer.create(allocator, io, source, .{
            .backoff = opts.backoff,
            .owns_store_objects = true,
            .error_handler = opts.error_handler,
            .resync_period_ms = opts.resync_period_ms,
            .indexers = opts.indexers,
        });
    }

    pub fn deinit(self: *Informer) void {
        self.stop();
        self.wait() catch {};
        self.reflector.deinit();
        if (self.cached_handlers) |ch| self.allocator.free(ch);
        self.handlers.deinit(self.allocator);
        self.store.deinit();
        self.source.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn start(self: *Informer) !void {
        if (self.started) return error.AlreadyStarted;
        self.started = true;
        self.run_future = try self.io.concurrent(runTask, .{self});
    }

    pub fn run(self: *Informer) !void {
        try self.reflector.run();
    }

    pub fn listAndWatch(self: *Informer) !void {
        try self.reflector.listAndWatch();
    }

    pub fn stop(self: *Informer) void {
        self.reflector.stop();
    }

    pub fn wait(self: *Informer) !void {
        if (self.run_future) |*future| {
            try future.await(self.io);
            self.run_future = null;
        }
        self.started = false;
    }

    pub fn addEventHandler(self: *Informer, handler: ResourceEventHandler) !void {
        self.handlers_lock.lockUncancelable(self.io);
        errdefer self.handlers_lock.unlock(self.io);
        try self.handlers.append(self.allocator, handler);
        self.handlers_generation +%= 1; // invalidate cached snapshot
        const synced = self.reflector.has_synced;
        self.handlers_lock.unlock(self.io);

        if (synced) {
            try self.replayHandler(handler);
        }
    }

    pub fn hasSynced(self: *const Informer) bool {
        return self.reflector.has_synced;
    }

    /// Returns the current sync state, distinguishing between not started,
    /// syncing, synced, and error backoff.
    pub fn syncState(self: *const Informer) SyncState {
        if (!self.started) return .not_started;
        if (self.reflector.has_synced) {
            if (self.reflector.consecutive_errors > 0) return .error_backoff;
            return .synced;
        }
        return .syncing;
    }

    /// Returns the last error encountered by the reflector, or null if healthy.
    pub fn lastError(self: *const Informer) ?anyerror {
        return self.reflector.last_error;
    }

    /// Returns the number of consecutive errors the reflector has encountered.
    pub fn consecutiveErrors(self: *const Informer) u32 {
        return self.reflector.consecutive_errors;
    }

    pub fn waitForSync(self: *Informer, timeout_ms: ?u64) bool {
        var remaining_ms = timeout_ms;
        while (!self.hasSynced()) {
            if (remaining_ms) |*timeout| {
                if (timeout.* == 0) return false;
                const step = @min(timeout.*, 10);
                timeout.* -= step;
                Io.sleep(self.io, Io.Duration.fromMilliseconds(@intCast(step)), .awake) catch return false;
                continue;
            }
            Io.sleep(self.io, Io.Duration.fromMilliseconds(10), .awake) catch return false;
        }
        return true;
    }

    pub fn getStore(self: *Informer) *Store {
        return &self.store;
    }

    // ── Index accessors ──────────────────────────────────────────────────

    /// Register a secondary index. Must be called before start().
    pub fn addIndexer(self: *Informer, name: []const u8, func_ptr: IndexFunc) !void {
        return self.store.addIndexer(name, func_ptr);
    }

    /// Lookup objects by secondary index value. Caller owns the returned slice.
    pub fn byIndex(self: *Informer, allocator: Allocator, index_name: []const u8, index_value: []const u8) ![]*Unstructured {
        return self.store.byIndex(allocator, index_name, index_value);
    }

    /// Return cache keys matching a secondary index value. Caller owns the returned slice.
    pub fn indexKeys(self: *Informer, allocator: Allocator, index_name: []const u8, index_value: []const u8) ![][]const u8 {
        return self.store.indexKeys(allocator, index_name, index_value);
    }

    fn runTask(self: *Informer) anyerror!void {
        try self.run();
    }

    fn makeObserver(self: *Informer) reflector_mod.Observer {
        return .{
            .ptr = @ptrCast(self),
            .on_add_fn = onReflectorAdd,
            .on_update_fn = onReflectorUpdate,
            .on_delete_detailed_fn = onReflectorDelete,
            .on_bookmark_fn = onReflectorBookmark,
        };
    }

    fn snapshotHandlers(self: *Informer) ![]ResourceEventHandler {
        // Fast path: if handlers haven't changed since last snapshot, reuse cached copy.
        if (self.dispatch_generation == self.handlers_generation) {
            if (self.cached_handlers) |ch| return ch;
        }

        self.handlers_lock.lockUncancelable(self.io);
        defer self.handlers_lock.unlock(self.io);

        // Re-check under lock
        if (self.dispatch_generation == self.handlers_generation) {
            if (self.cached_handlers) |ch| return ch;
        }

        // Regenerate snapshot
        if (self.cached_handlers) |ch| self.allocator.free(ch);
        const snapshot = try self.allocator.dupe(ResourceEventHandler, self.handlers.items);
        self.cached_handlers = snapshot;
        self.dispatch_generation = self.handlers_generation;
        return snapshot;
    }

    fn replayHandler(self: *Informer, handler: ResourceEventHandler) !void {
        const items = try self.store.list(self.allocator);
        defer self.allocator.free(items);

        if (handler.on_add_fn) |func| {
            for (items) |obj| {
                try func(handler.ptr, obj, true);
            }
        }
    }

    fn dispatchAdd(self: *Informer, obj: *const Unstructured, is_in_initial_list: bool) !void {
        const handlers = try self.snapshotHandlers();
        // handlers is a cached snapshot — not freed per-dispatch

        for (handlers) |handler| {
            if (handler.on_add_fn) |func| {
                try func(handler.ptr, obj, is_in_initial_list);
            }
        }
    }

    fn dispatchUpdate(self: *Informer, old_obj: *const Unstructured, new_obj: *const Unstructured) !void {
        const handlers = try self.snapshotHandlers();
        // handlers is a cached snapshot — not freed per-dispatch

        for (handlers) |handler| {
            if (handler.on_update_fn) |func| {
                try func(handler.ptr, old_obj, new_obj);
            }
        }
    }

    fn dispatchDelete(self: *Informer, notification: DeleteNotification) !void {
        const handlers = try self.snapshotHandlers();
        // handlers is a cached snapshot — not freed per-dispatch

        for (handlers) |handler| {
            if (handler.on_delete_detailed_fn) |func| {
                try func(handler.ptr, notification);
            } else if (handler.on_delete_fn) |func| {
                try func(handler.ptr, notification.objectRef());
            }
        }
    }

    fn dispatchBookmark(self: *Informer, rv: []const u8) !void {
        const handlers = try self.snapshotHandlers();
        // handlers is a cached snapshot — not freed per-dispatch

        for (handlers) |handler| {
            if (handler.on_bookmark_fn) |func| {
                try func(handler.ptr, rv);
            }
        }
    }

    fn onReflectorAdd(ptr: *anyopaque, obj: *const Unstructured, is_in_initial_list: bool) anyerror!void {
        const self: *Informer = @ptrCast(@alignCast(ptr));
        try self.dispatchAdd(obj, is_in_initial_list);
    }

    fn onReflectorUpdate(ptr: *anyopaque, old_obj: *const Unstructured, new_obj: *const Unstructured) anyerror!void {
        const self: *Informer = @ptrCast(@alignCast(ptr));
        try self.dispatchUpdate(old_obj, new_obj);
    }

    fn onReflectorDelete(ptr: *anyopaque, notification: DeleteNotification) anyerror!void {
        const self: *Informer = @ptrCast(@alignCast(ptr));
        try self.dispatchDelete(notification);
    }

    fn onReflectorBookmark(ptr: *anyopaque, rv: []const u8) anyerror!void {
        const self: *Informer = @ptrCast(@alignCast(ptr));
        try self.dispatchBookmark(rv);
    }
};

pub const DynamicOptions = struct {
    namespace: ?[]const u8 = null,
    label_selector: ?[]const u8 = null,
    field_selector: ?[]const u8 = null,
    allow_bookmarks: bool = true,
    queue_size: usize = 1,
    backoff: reflector_mod.BackoffConfig = .{},
    error_handler: ?reflector_mod.Reflector.ErrorHandler = null,
    resync_period_ms: u64 = 0,
    /// Secondary indexers to register on the informer's store.
    indexers: []const Informer.IndexerEntry = &.{},
    /// Page size for list pagination. 0 = no pagination (single request).
    /// Go's default is 500.
    list_page_size: u32 = 500,
};

pub const DynamicInformerFactory = struct {
    ptr: *anyopaque,
    create_fn: *const fn (
        ptr: *anyopaque,
        allocator: Allocator,
        io: Io,
        dynamic_client: *DynamicClient,
        gvr: scheme.GroupVersionResource,
        opts: DynamicOptions,
    ) anyerror!*Informer,
};

const DynamicSource = struct {
    allocator: Allocator,
    io: Io,
    dynamic_client: *DynamicClient,
    gvr: scheme.GroupVersionResource,
    namespace: ?[]const u8,
    label_selector: ?[]const u8,
    field_selector: ?[]const u8,
    allow_bookmarks: bool,
    queue_size: usize,
    list_page_size: u32,

    fn create(
        allocator: Allocator,
        io: Io,
        dynamic_client: *DynamicClient,
        gvr: scheme.GroupVersionResource,
        opts: DynamicOptions,
    ) !Source {
        const source = try allocator.create(DynamicSource);
        errdefer allocator.destroy(source);

        source.* = .{
            .allocator = allocator,
            .io = io,
            .dynamic_client = dynamic_client,
            .gvr = gvr,
            .namespace = if (opts.namespace) |ns| try allocator.dupe(u8, ns) else null,
            .label_selector = if (opts.label_selector) |value| try allocator.dupe(u8, value) else null,
            .field_selector = if (opts.field_selector) |value| try allocator.dupe(u8, value) else null,
            .allow_bookmarks = opts.allow_bookmarks,
            .queue_size = opts.queue_size,
            .list_page_size = opts.list_page_size,
        };

        return .{
            .ptr = @ptrCast(source),
            .list_fn = list,
            .watch_fn = watch,
            .deinit_fn = deinitOpaque,
        };
    }

    fn deinitOpaque(ptr: *anyopaque, allocator: Allocator) void {
        const self: *DynamicSource = @ptrCast(@alignCast(ptr));
        if (self.namespace) |ns| allocator.free(ns);
        if (self.label_selector) |value| allocator.free(value);
        if (self.field_selector) |value| allocator.free(value);
        allocator.destroy(self);
    }

    fn list(ptr: *anyopaque, allocator: Allocator, resource_version: ?[]const u8) anyerror!reflector_mod.ListResult {
        const self: *DynamicSource = @ptrCast(@alignCast(ptr));
        const resource = self.dynamic_client.resource(self.gvr, .{ .namespace = self.namespace });

        var all_items: std.ArrayList(*Unstructured) = .empty;
        errdefer {
            for (all_items.items) |obj| {
                obj.deinit();
                allocator.destroy(obj);
            }
            all_items.deinit(allocator);
        }

        var last_rv: ?[]const u8 = null;
        defer if (last_rv) |rv| allocator.free(rv);
        var continue_token: ?[]const u8 = null;
        defer if (continue_token) |ct| allocator.free(ct);
        const limit: ?u32 = if (self.list_page_size > 0) self.list_page_size else null;
        const max_pages: u32 = 10_000; // Safety cap: 10K pages × 500 items = 5M objects max
        var page_count: u32 = 0;

        while (page_count < max_pages) {
            page_count += 1;
            var list_obj = try resource.list(.{
                .label_selector = self.label_selector,
                .field_selector = self.field_selector,
                .resource_version = resource_version,
                .limit = limit,
                .continue_token = continue_token,
            });
            defer list_obj.deinit();

            const page_items = try parseListItems(allocator, &list_obj);
            defer allocator.free(page_items);

            try all_items.ensureUnusedCapacity(allocator, page_items.len);
            for (page_items) |obj| {
                all_items.appendAssumeCapacity(obj);
            }

            // Track the resourceVersion from the last page (dupe since list_obj is deferred)
            if (last_rv) |rv| allocator.free(rv);
            last_rv = try allocator.dupe(u8, list_obj.field("metadata").field("resourceVersion").str() orelse "");

            // Check for continue token — dupe since list_obj is deferred
            if (continue_token) |ct| allocator.free(ct);
            continue_token = null;
            const next_token = list_obj.field("metadata").field("continue").str();
            if (next_token) |tok| {
                if (tok.len == 0) break;
                continue_token = try allocator.dupe(u8, tok);
            } else break;
        }

        const items = try all_items.toOwnedSlice(allocator);
        // Transfer ownership of last_rv to the caller (prevent defer from freeing it)
        const rv = last_rv orelse try allocator.dupe(u8, "");
        last_rv = null;
        return .{
            .items = items,
            .items_owned = true,
            .resource_version = rv,
            .resource_version_owned = true,
        };
    }

    fn watch(ptr: *anyopaque, allocator: Allocator, resource_version: ?[]const u8) anyerror!WatcherInterface {
        const self: *DynamicSource = @ptrCast(@alignCast(ptr));
        const resource = self.dynamic_client.resource(self.gvr, .{ .namespace = self.namespace });

        const adapter = try allocator.create(ClientWatcherAdapter);
        errdefer allocator.destroy(adapter);

        adapter.* = .{
            .allocator = allocator,
            .io = self.io,
            .watcher = try resource.watch(self.io, .{
                .namespace = self.namespace,
                .resource_version = resource_version,
                .label_selector = self.label_selector,
                .field_selector = self.field_selector,
                .allow_bookmarks = self.allow_bookmarks,
                .queue_size = self.queue_size,
            }),
            .stopped = false,
        };

        return .{
            .ptr = @ptrCast(adapter),
            .nextFn = ClientWatcherAdapter.next,
            .stopFn = ClientWatcherAdapter.stop,
            .deinitFn = ClientWatcherAdapter.deinitOpaque,
        };
    }
};

const ClientWatcherAdapter = struct {
    allocator: Allocator,
    io: Io,
    watcher: client.Watcher,
    stopped: bool,

    fn next(ptr: *anyopaque) WatcherInterface.WatcherError!?WatchEvent {
        const self: *ClientWatcherAdapter = @ptrCast(@alignCast(ptr));
        const next_event = self.watcher.events.getOne(self.io) catch return null;

        const obj = self.allocator.create(Unstructured) catch return error.WatchFailed;
        obj.* = next_event.object;

        return .{
            .event_type = switch (next_event.event_type) {
                .added => .added,
                .modified => .modified,
                .deleted => .deleted,
                .bookmark => .bookmark,
                .err => .err,
            },
            .object = obj,
        };
    }

    fn stop(ptr: *anyopaque) void {
        const self: *ClientWatcherAdapter = @ptrCast(@alignCast(ptr));
        if (self.stopped) return;
        self.watcher.stop(self.io);
        self.stopped = true;
    }

    fn deinitOpaque(ptr: *anyopaque) void {
        const self: *ClientWatcherAdapter = @ptrCast(@alignCast(ptr));
        self.allocator.destroy(self);
    }
};

fn parseListItems(allocator: Allocator, list_obj: *const Unstructured) ![]*Unstructured {
    const items_nav = list_obj.field("items");
    const count = items_nav.len() orelse 0;
    const items = try allocator.alloc(*Unstructured, count);
    errdefer allocator.free(items);

    var it = items_nav.iter();
    var i: usize = 0;
    errdefer {
        for (items[0..i]) |obj| {
            obj.deinit();
            allocator.destroy(obj);
        }
    }

    while (it.next()) |item_nav| {
        const raw_value = item_nav.raw() orelse return error.InvalidListObject;
        const bytes = try json.Stringify.valueAlloc(allocator, raw_value, .{});
        defer allocator.free(bytes);

        const obj = try allocator.create(Unstructured);
        errdefer allocator.destroy(obj);
        obj.* = try Unstructured.fromJson(allocator, bytes);
        items[i] = obj;
        i += 1;
    }

    return items[0..i];
}

pub const DynamicInformerManager = struct {
    allocator: Allocator,
    io: Io,
    dynamic_client: *DynamicClient,
    default_options: DynamicOptions,
    factory: DynamicInformerFactory,
    informers: std.StringHashMapUnmanaged(*Informer),
    lock: Io.Mutex,
    started: bool,

    pub fn init(allocator: Allocator, io: Io, dynamic_client: *DynamicClient, opts: DynamicOptions) DynamicInformerManager {
        return .{
            .allocator = allocator,
            .io = io,
            .dynamic_client = dynamic_client,
            .default_options = opts,
            .factory = .{
                .ptr = undefined,
                .create_fn = defaultDynamicInformerFactory,
            },
            .informers = .empty,
            .lock = .init,
            .started = false,
        };
    }

    pub fn initWithFactory(
        allocator: Allocator,
        io: Io,
        dynamic_client: *DynamicClient,
        opts: DynamicOptions,
        factory: DynamicInformerFactory,
    ) DynamicInformerManager {
        return .{
            .allocator = allocator,
            .io = io,
            .dynamic_client = dynamic_client,
            .default_options = opts,
            .factory = factory,
            .informers = .empty,
            .lock = .init,
            .started = false,
        };
    }

    pub fn deinit(self: *DynamicInformerManager) void {
        self.stopAll();
        self.wait() catch {};

        var entries = std.ArrayList(struct { key: []const u8, informer: *Informer }).empty;
        defer entries.deinit(self.allocator);

        self.lock.lockUncancelable(self.io);
        var it = self.informers.iterator();
        while (it.next()) |entry| {
            entries.append(self.allocator, .{
                .key = entry.key_ptr.*,
                .informer = entry.value_ptr.*,
            }) catch {};
        }
        self.informers.clearRetainingCapacity();
        self.informers.deinit(self.allocator);
        self.lock.unlock(self.io);

        for (entries.items) |entry| {
            self.allocator.free(entry.key);
            entry.informer.deinit();
        }
    }

    pub fn forResource(self: *DynamicInformerManager, gvr: scheme.GroupVersionResource, namespace: ?[]const u8) !*Informer {
        const key = try managerKey(self.allocator, gvr, namespace orelse self.default_options.namespace);
        errdefer self.allocator.free(key);

        self.lock.lockUncancelable(self.io);

        if (self.informers.get(key)) |existing| {
            self.lock.unlock(self.io);
            self.allocator.free(key);
            return existing;
        }

        // Release the lock while creating the informer (may do I/O).
        self.lock.unlock(self.io);

        var opts = self.default_options;
        opts.namespace = namespace orelse self.default_options.namespace;

        const informer = try self.factory.create_fn(
            self.factory.ptr,
            self.allocator,
            self.io,
            self.dynamic_client,
            gvr,
            opts,
        );
        errdefer informer.deinit();

        // Re-acquire lock and check for races.
        self.lock.lockUncancelable(self.io);
        if (self.informers.get(key)) |existing| {
            // Another caller raced and created the same informer.
            self.lock.unlock(self.io);
            self.allocator.free(key);
            informer.deinit();
            return existing;
        }

        try self.informers.put(self.allocator, key, informer);
        const should_start = self.started;
        self.lock.unlock(self.io);

        if (should_start) {
            try informer.start();
        }

        return informer;
    }

    pub fn destroyResource(self: *DynamicInformerManager, gvr: scheme.GroupVersionResource, namespace: ?[]const u8) !bool {
        const key = try managerKey(self.allocator, gvr, namespace orelse self.default_options.namespace);
        defer self.allocator.free(key);

        self.lock.lockUncancelable(self.io);
        const removed = self.informers.fetchRemove(key);
        self.lock.unlock(self.io);

        if (removed) |entry| {
            entry.value.stop();
            entry.value.wait() catch {};
            self.allocator.free(entry.key);
            entry.value.deinit();
            return true;
        }
        return false;
    }

    pub fn start(self: *DynamicInformerManager) !void {
        const informers = try self.markStartedAndSnapshot();
        defer self.allocator.free(informers);

        for (informers) |informer| {
            if (!informer.started) {
                try informer.start();
            }
        }
    }

    pub fn stopAll(self: *DynamicInformerManager) void {
        const informers = self.snapshotInformers() catch return;
        defer self.allocator.free(informers);

        for (informers) |informer| {
            informer.stop();
        }
    }

    pub fn wait(self: *DynamicInformerManager) !void {
        const informers = try self.snapshotInformers();
        defer self.allocator.free(informers);

        for (informers) |informer| {
            try informer.wait();
        }
    }

    fn snapshotInformers(self: *DynamicInformerManager) ![]*Informer {
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);

        return self.snapshotInformersLocked();
    }

    fn markStartedAndSnapshot(self: *DynamicInformerManager) ![]*Informer {
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);

        self.started = true;
        return self.snapshotInformersLocked();
    }

    fn snapshotInformersLocked(self: *DynamicInformerManager) ![]*Informer {
        const count = self.informers.count();
        const out = try self.allocator.alloc(*Informer, count);
        var i: usize = 0;
        var it = self.informers.valueIterator();
        while (it.next()) |value| {
            out[i] = value.*;
            i += 1;
        }
        return out[0..i];
    }
};

fn defaultDynamicInformerFactory(
    _: *anyopaque,
    allocator: Allocator,
    io: Io,
    dynamic_client: *DynamicClient,
    gvr: scheme.GroupVersionResource,
    opts: DynamicOptions,
) anyerror!*Informer {
    return Informer.createDynamic(allocator, io, dynamic_client, gvr, opts);
}

fn managerKey(allocator: Allocator, gvr: scheme.GroupVersionResource, namespace: ?[]const u8) ![]const u8 {
    if (namespace) |ns| {
        return try std.fmt.allocPrint(allocator, "{s}/{s}/{s}:{s}", .{ gvr.group, gvr.version, gvr.resource, ns });
    }
    return try std.fmt.allocPrint(allocator, "{s}/{s}/{s}", .{ gvr.group, gvr.version, gvr.resource });
}

const InformerTestWatcher = struct {
    allocator: Allocator,

    fn next(_: *anyopaque) WatcherInterface.WatcherError!?WatchEvent {
        return null;
    }

    fn stop(_: *anyopaque) void {}

    fn deinitOpaque(ptr: *anyopaque) void {
        const self: *InformerTestWatcher = @ptrCast(@alignCast(ptr));
        self.allocator.destroy(self);
    }
};

const InformerTestState = struct {
    list_calls: u32 = 0,
    adds: u32 = 0,

    fn list(ptr: *anyopaque, allocator: Allocator, _: ?[]const u8) anyerror!reflector_mod.ListResult {
        const self: *InformerTestState = @ptrCast(@alignCast(ptr));
        self.list_calls += 1;

        const obj = try allocator.create(Unstructured);
        obj.* = try Unstructured.init(allocator);
        try obj.setNamespace("default");
        try obj.setName("demo");
        try obj.setResourceVersion("10");

        const items = try allocator.alloc(*Unstructured, 1);
        items[0] = obj;
        return .{
            .items = items,
            .items_owned = true,
            .resource_version = try allocator.dupe(u8, "10"),
            .resource_version_owned = true,
        };
    }

    fn watch(_: *anyopaque, allocator: Allocator, _: ?[]const u8) anyerror!WatcherInterface {
        const watcher = try allocator.create(InformerTestWatcher);
        watcher.* = .{ .allocator = allocator };
        return .{
            .ptr = @ptrCast(watcher),
            .nextFn = InformerTestWatcher.next,
            .stopFn = InformerTestWatcher.stop,
            .deinitFn = InformerTestWatcher.deinitOpaque,
        };
    }

    fn onAdd(ptr: *anyopaque, _: *const Unstructured, is_in_initial_list: bool) anyerror!void {
        const self: *InformerTestState = @ptrCast(@alignCast(ptr));
        try testing.expect(is_in_initial_list);
        self.adds += 1;
    }
};

test "informer: initial list replays add handlers" {
    var state = InformerTestState{};
    const source = Source{
        .ptr = @ptrCast(&state),
        .list_fn = InformerTestState.list,
        .watch_fn = InformerTestState.watch,
    };

    var informer = try Informer.create(testing.allocator, testing.io, source, .{});
    defer informer.deinit();

    try informer.addEventHandler(.{
        .ptr = @ptrCast(&state),
        .on_add_fn = InformerTestState.onAdd,
    });

    try informer.listAndWatch();

    try testing.expectEqual(@as(u32, 1), state.list_calls);
    try testing.expectEqual(@as(u32, 1), state.adds);
}

test "informer: addEventHandler after sync replays current store" {
    var state = InformerTestState{};
    const source = Source{
        .ptr = @ptrCast(&state),
        .list_fn = InformerTestState.list,
        .watch_fn = InformerTestState.watch,
    };

    var informer = try Informer.create(testing.allocator, testing.io, source, .{});
    defer informer.deinit();

    try informer.listAndWatch();
    try testing.expect(informer.hasSynced());

    try informer.addEventHandler(.{
        .ptr = @ptrCast(&state),
        .on_add_fn = InformerTestState.onAdd,
    });

    try testing.expectEqual(@as(u32, 1), state.list_calls);
    try testing.expectEqual(@as(u32, 1), state.adds);
}

const HandlerCounters = struct {
    adds: u32 = 0,
    updates: u32 = 0,
    deletes: u32 = 0,
    bookmarks: u32 = 0,
    initial_adds: u32 = 0,

    fn onAdd(ptr: *anyopaque, _: *const Unstructured, is_in_initial_list: bool) anyerror!void {
        const self: *HandlerCounters = @ptrCast(@alignCast(ptr));
        self.adds += 1;
        if (is_in_initial_list) self.initial_adds += 1;
    }

    fn onUpdate(ptr: *anyopaque, _: *const Unstructured, _: *const Unstructured) anyerror!void {
        const self: *HandlerCounters = @ptrCast(@alignCast(ptr));
        self.updates += 1;
    }

    fn onDelete(ptr: *anyopaque, _: *const Unstructured) anyerror!void {
        const self: *HandlerCounters = @ptrCast(@alignCast(ptr));
        self.deletes += 1;
    }

    fn onBookmark(ptr: *anyopaque, _: []const u8) anyerror!void {
        const self: *HandlerCounters = @ptrCast(@alignCast(ptr));
        self.bookmarks += 1;
    }
};

const DeleteDispatchState = struct {
    allocator: Allocator,
    legacy_calls: u32 = 0,
    detailed_calls: u32 = 0,
    tombstone_calls: u32 = 0,
    last_name: ?[]const u8 = null,

    fn deinit(self: *DeleteDispatchState) void {
        if (self.last_name) |name| self.allocator.free(name);
    }

    fn onLegacy(ptr: *anyopaque, obj: *const Unstructured) anyerror!void {
        const self: *DeleteDispatchState = @ptrCast(@alignCast(ptr));
        self.legacy_calls += 1;
        if (self.last_name) |name| self.allocator.free(name);
        self.last_name = try self.allocator.dupe(u8, obj.getName().?);
    }

    fn onDetailed(ptr: *anyopaque, notification: DeleteNotification) anyerror!void {
        const self: *DeleteDispatchState = @ptrCast(@alignCast(ptr));
        self.detailed_calls += 1;
        if (notification.isTombstone()) self.tombstone_calls += 1;
        if (self.last_name) |name| self.allocator.free(name);
        self.last_name = try self.allocator.dupe(u8, notification.objectRef().getName().?);
    }
};

const InformerSequenceState = struct {
    allocator: Allocator,
    watch_events: []const WatchEvent,
    event_index: usize = 0,

    fn list(_: *anyopaque, allocator: Allocator, _: ?[]const u8) anyerror!reflector_mod.ListResult {
        const obj1 = try allocator.create(Unstructured);
        errdefer allocator.destroy(obj1);
        obj1.* = try Unstructured.init(allocator);
        try obj1.setNamespace("default");
        try obj1.setName("a");
        try obj1.setResourceVersion("10");

        const obj2 = try allocator.create(Unstructured);
        errdefer {
            obj2.deinit();
            allocator.destroy(obj2);
        }
        obj2.* = try Unstructured.init(allocator);
        try obj2.setNamespace("default");
        try obj2.setName("b");
        try obj2.setResourceVersion("11");

        const items = try allocator.alloc(*Unstructured, 2);
        items[0] = obj1;
        items[1] = obj2;

        return .{
            .items = items,
            .items_owned = true,
            .resource_version = try allocator.dupe(u8, "11"),
            .resource_version_owned = true,
        };
    }

    fn watch(ptr: *anyopaque, allocator: Allocator, _: ?[]const u8) anyerror!WatcherInterface {
        const self: *InformerSequenceState = @ptrCast(@alignCast(ptr));
        const watcher = try allocator.create(InformerEventWatcher);
        watcher.* = .{
            .allocator = allocator,
            .state = self,
        };
        return .{
            .ptr = @ptrCast(watcher),
            .nextFn = InformerEventWatcher.next,
            .stopFn = InformerEventWatcher.stop,
            .deinitFn = InformerEventWatcher.deinitOpaque,
        };
    }
};

const InformerEventWatcher = struct {
    allocator: Allocator,
    state: *InformerSequenceState,

    fn next(ptr: *anyopaque) WatcherInterface.WatcherError!?WatchEvent {
        const self: *InformerEventWatcher = @ptrCast(@alignCast(ptr));
        if (self.state.event_index >= self.state.watch_events.len) return null;
        const event = self.state.watch_events[self.state.event_index];
        self.state.event_index += 1;
        return event;
    }

    fn stop(_: *anyopaque) void {}

    fn deinitOpaque(ptr: *anyopaque) void {
        const self: *InformerEventWatcher = @ptrCast(@alignCast(ptr));
        self.allocator.destroy(self);
    }
};

test "informer: multiple handlers receive update delete and bookmark events" {
    var updated = try testing.allocator.create(Unstructured);
    updated.* = try Unstructured.init(testing.allocator);
    try updated.setNamespace("default");
    try updated.setName("a");
    try updated.setResourceVersion("12");

    var deleted = try testing.allocator.create(Unstructured);
    deleted.* = try Unstructured.init(testing.allocator);
    try deleted.setNamespace("default");
    try deleted.setName("b");
    try deleted.setResourceVersion("13");

    var bookmark = try testing.allocator.create(Unstructured);
    bookmark.* = try Unstructured.init(testing.allocator);
    try bookmark.setResourceVersion("14");

    var state = InformerSequenceState{
        .allocator = testing.allocator,
        .watch_events = &.{
            .{ .event_type = .modified, .object = updated },
            .{ .event_type = .deleted, .object = deleted },
            .{ .event_type = .bookmark, .object = bookmark },
        },
    };

    const source = Source{
        .ptr = @ptrCast(&state),
        .list_fn = InformerSequenceState.list,
        .watch_fn = InformerSequenceState.watch,
    };

    var informer = try Informer.create(testing.allocator, testing.io, source, .{});
    defer informer.deinit();

    var handler_a = HandlerCounters{};
    var handler_b = HandlerCounters{};
    try informer.addEventHandler(.{
        .ptr = @ptrCast(&handler_a),
        .on_add_fn = HandlerCounters.onAdd,
        .on_update_fn = HandlerCounters.onUpdate,
        .on_delete_fn = HandlerCounters.onDelete,
        .on_bookmark_fn = HandlerCounters.onBookmark,
    });
    try informer.addEventHandler(.{
        .ptr = @ptrCast(&handler_b),
        .on_add_fn = HandlerCounters.onAdd,
        .on_update_fn = HandlerCounters.onUpdate,
        .on_delete_fn = HandlerCounters.onDelete,
        .on_bookmark_fn = HandlerCounters.onBookmark,
    });

    try informer.listAndWatch();

    inline for (&.{ &handler_a, &handler_b }) |handler| {
        try testing.expectEqual(@as(u32, 2), handler.adds);
        try testing.expectEqual(@as(u32, 2), handler.initial_adds);
        try testing.expectEqual(@as(u32, 1), handler.updates);
        try testing.expectEqual(@as(u32, 1), handler.deletes);
        try testing.expectEqual(@as(u32, 1), handler.bookmarks);
    }
}

test "informer: detailed delete handlers preserve tombstones and supersede legacy callbacks" {
    var state = InformerTestState{};
    const source = Source{
        .ptr = @ptrCast(&state),
        .list_fn = InformerTestState.list,
        .watch_fn = InformerTestState.watch,
    };

    var informer = try Informer.create(testing.allocator, testing.io, source, .{});
    defer informer.deinit();

    var detailed = DeleteDispatchState{ .allocator = testing.allocator };
    defer detailed.deinit();
    var legacy = DeleteDispatchState{ .allocator = testing.allocator };
    defer legacy.deinit();

    try informer.addEventHandler(.{
        .ptr = @ptrCast(&detailed),
        .on_delete_fn = DeleteDispatchState.onLegacy,
        .on_delete_detailed_fn = DeleteDispatchState.onDetailed,
    });
    try informer.addEventHandler(.{
        .ptr = @ptrCast(&legacy),
        .on_delete_fn = DeleteDispatchState.onLegacy,
    });

    var stale = try testing.allocator.create(Unstructured);
    stale.* = try Unstructured.init(testing.allocator);
    defer {
        stale.deinit();
        testing.allocator.destroy(stale);
    }
    try stale.setNamespace("default");
    try stale.setName("demo");
    try stale.setResourceVersion("10");

    const ObjectKey = @import("workqueue.zig").ObjectKey;
    try informer.dispatchDelete(.{
        .tombstone = .{
            .key = try ObjectKey.fromObject(stale),
            .obj = stale,
        },
    });

    try testing.expectEqual(@as(u32, 0), detailed.legacy_calls);
    try testing.expectEqual(@as(u32, 1), detailed.detailed_calls);
    try testing.expectEqual(@as(u32, 1), detailed.tombstone_calls);
    try testing.expectEqualStrings("demo", detailed.last_name.?);

    try testing.expectEqual(@as(u32, 1), legacy.legacy_calls);
    try testing.expectEqual(@as(u32, 0), legacy.detailed_calls);
    try testing.expectEqualStrings("demo", legacy.last_name.?);
}

test "informer: waitForSync reports timeout before list" {
    var state = InformerTestState{};
    const source = Source{
        .ptr = @ptrCast(&state),
        .list_fn = InformerTestState.list,
        .watch_fn = InformerTestState.watch,
    };

    var informer = try Informer.create(testing.allocator, testing.io, source, .{});
    defer informer.deinit();

    try testing.expect(!informer.waitForSync(0));
    try informer.listAndWatch();
    try testing.expect(informer.waitForSync(0));
}

const ManagerTestFactory = struct {
    allocator: Allocator,
    created: u32 = 0,
    list_calls: u32 = 0,
    watch_calls: u32 = 0,
    stop_calls: u32 = 0,
    deinits: u32 = 0,

    fn create(
        ptr: *anyopaque,
        allocator: Allocator,
        io: Io,
        _: *DynamicClient,
        gvr: scheme.GroupVersionResource,
        opts: DynamicOptions,
    ) anyerror!*Informer {
        const self: *ManagerTestFactory = @ptrCast(@alignCast(ptr));
        const state = try allocator.create(ManagerInformerState);
        errdefer allocator.destroy(state);

        self.created += 1;
        state.* = .{
            .factory = self,
            .allocator = allocator,
            .gvr = gvr,
            .namespace = if (opts.namespace) |ns| try allocator.dupe(u8, ns) else null,
            .stopped = false,
        };

        const source = Source{
            .ptr = @ptrCast(state),
            .list_fn = ManagerInformerState.list,
            .watch_fn = ManagerInformerState.watch,
            .deinit_fn = ManagerInformerState.deinitOpaque,
        };
        return Informer.create(allocator, io, source, .{});
    }
};

const ManagerInformerState = struct {
    factory: *ManagerTestFactory,
    allocator: Allocator,
    gvr: scheme.GroupVersionResource,
    namespace: ?[]const u8,
    stopped: bool,

    fn list(ptr: *anyopaque, allocator: Allocator, _: ?[]const u8) anyerror!reflector_mod.ListResult {
        const self: *ManagerInformerState = @ptrCast(@alignCast(ptr));
        self.factory.list_calls += 1;
        const items = try allocator.alloc(*Unstructured, 0);
        return .{
            .items = items,
            .items_owned = true,
            .resource_version = try allocator.dupe(u8, "1"),
            .resource_version_owned = true,
        };
    }

    fn watch(ptr: *anyopaque, allocator: Allocator, _: ?[]const u8) anyerror!WatcherInterface {
        const self: *ManagerInformerState = @ptrCast(@alignCast(ptr));
        self.factory.watch_calls += 1;

        const watcher = try allocator.create(ManagerWatcher);
        watcher.* = .{
            .allocator = allocator,
            .state = self,
        };
        return .{
            .ptr = @ptrCast(watcher),
            .nextFn = ManagerWatcher.next,
            .stopFn = ManagerWatcher.stop,
            .deinitFn = ManagerWatcher.deinitOpaque,
        };
    }

    fn deinitOpaque(ptr: *anyopaque, allocator: Allocator) void {
        const self: *ManagerInformerState = @ptrCast(@alignCast(ptr));
        self.factory.deinits += 1;
        if (self.namespace) |ns| allocator.free(ns);
        allocator.destroy(self);
    }
};

const ManagerWatcher = struct {
    allocator: Allocator,
    state: *ManagerInformerState,

    fn next(ptr: *anyopaque) WatcherInterface.WatcherError!?WatchEvent {
        const self: *ManagerWatcher = @ptrCast(@alignCast(ptr));
        while (!self.state.stopped) {
            Io.sleep(testing.io, Io.Duration.fromMilliseconds(1), .awake) catch return null;
        }
        return null;
    }

    fn stop(ptr: *anyopaque) void {
        const self: *ManagerWatcher = @ptrCast(@alignCast(ptr));
        self.state.factory.stop_calls += 1;
        self.state.stopped = true;
    }

    fn deinitOpaque(ptr: *anyopaque) void {
        const self: *ManagerWatcher = @ptrCast(@alignCast(ptr));
        self.allocator.destroy(self);
    }
};

fn waitForCondition(
    comptime predicate: fn (*anyopaque) bool,
    ctx: *anyopaque,
    timeout_ms: u64,
) !void {
    var remaining = timeout_ms;
    while (!predicate(ctx)) {
        if (remaining == 0) return error.Timeout;
        const step = @min(remaining, 5);
        remaining -= step;
        try Io.sleep(testing.io, Io.Duration.fromMilliseconds(@intCast(step)), .awake);
    }
}

fn managerStartedTwo(ptr: *anyopaque) bool {
    const factory: *ManagerTestFactory = @ptrCast(@alignCast(ptr));
    return factory.watch_calls >= 2 and factory.list_calls >= 2;
}

fn managerStartedThree(ptr: *anyopaque) bool {
    const factory: *ManagerTestFactory = @ptrCast(@alignCast(ptr));
    return factory.watch_calls >= 3 and factory.list_calls >= 3;
}

fn managerDestroyedOne(ptr: *anyopaque) bool {
    const factory: *ManagerTestFactory = @ptrCast(@alignCast(ptr));
    return factory.deinits >= 1;
}

test "dynamic informer manager: reuses keys and starts new informers after manager start" {
    var factory = ManagerTestFactory{ .allocator = testing.allocator };
    var manager = DynamicInformerManager.initWithFactory(
        testing.allocator,
        testing.io,
        @ptrFromInt(@alignOf(DynamicClient)),
        .{},
        .{
            .ptr = @ptrCast(&factory),
            .create_fn = ManagerTestFactory.create,
        },
    );
    defer manager.deinit();

    const cm_gvr = scheme.GroupVersionResource{
        .group = "",
        .version = "v1",
        .resource = "configmaps",
    };
    const pod_gvr = scheme.GroupVersionResource{
        .group = "",
        .version = "v1",
        .resource = "pods",
    };

    const inf_a = try manager.forResource(cm_gvr, "default");
    const inf_a_again = try manager.forResource(cm_gvr, "default");
    const inf_b = try manager.forResource(pod_gvr, "default");

    try testing.expectEqual(inf_a, inf_a_again);
    try testing.expect(inf_a != inf_b);
    try testing.expectEqual(@as(u32, 2), factory.created);

    try manager.start();
    try waitForCondition(managerStartedTwo, @ptrCast(&factory), 1_000);

    const inf_c = try manager.forResource(cm_gvr, "kube-system");
    try testing.expect(inf_c != inf_a);
    try waitForCondition(managerStartedThree, @ptrCast(&factory), 1_000);

    try testing.expect(try manager.destroyResource(cm_gvr, "default"));
    try waitForCondition(managerDestroyedOne, @ptrCast(&factory), 1_000);
    try testing.expect(!(try manager.destroyResource(cm_gvr, "default")));
}

test "informer: syncState transitions" {
    const FailSource = struct {
        fn listFn(_: *anyopaque, _: mem.Allocator, _: ?[]const u8) anyerror!reflector_mod.ListResult {
            return error.ListFailed;
        }
        fn watchFn(_: *anyopaque, _: mem.Allocator, _: ?[]const u8) anyerror!reflector_mod.WatcherInterface {
            return error.WatchFailed;
        }
    };

    var informer = try Informer.create(testing.allocator, testing.io, .{
        .ptr = @ptrFromInt(1),
        .list_fn = FailSource.listFn,
        .watch_fn = FailSource.watchFn,
    }, .{});
    defer informer.deinit();

    // Before start
    try testing.expectEqual(Informer.SyncState.not_started, informer.syncState());
    try testing.expect(informer.lastError() == null);
    try testing.expectEqual(@as(u32, 0), informer.consecutiveErrors());
}

test "informer: error_handler option is wired through" {
    const ErrorTracker = struct {
        var called: bool = false;
        fn handler(_: *anyopaque, _: anyerror, _: reflector_mod.Reflector.ErrorContext, _: u32) void {
            called = true;
        }
    };
    ErrorTracker.called = false;

    const FailSource2 = struct {
        fn listFn(_: *anyopaque, _: mem.Allocator, _: ?[]const u8) anyerror!reflector_mod.ListResult {
            return error.ListFailed;
        }
        fn watchFn(_: *anyopaque, _: mem.Allocator, _: ?[]const u8) anyerror!reflector_mod.WatcherInterface {
            return error.WatchFailed;
        }
    };

    var informer = try Informer.create(testing.allocator, testing.io, .{
        .ptr = @ptrFromInt(1),
        .list_fn = FailSource2.listFn,
        .watch_fn = FailSource2.watchFn,
    }, .{
        .error_handler = .{
            .ptr = @ptrFromInt(1),
            .on_error_fn = ErrorTracker.handler,
        },
    });
    defer informer.deinit();

    // Directly trigger error notification through reflector
    informer.reflector.last_error = error.ListFailed;
    informer.reflector.consecutive_errors = 1;
    informer.reflector.notifyError(error.ListFailed, .list);

    try testing.expect(ErrorTracker.called);
    try testing.expect(informer.lastError() != null);
    try testing.expect(informer.consecutiveErrors() > 0);
}

test "informer: addIndexer and byIndex" {
    const SuccessSource = struct {
        var items_buf: [0]*Unstructured = .{};

        fn listFn(_: *anyopaque, _: mem.Allocator, _: ?[]const u8) anyerror!reflector_mod.ListResult {
            return .{ .items = &items_buf, .resource_version = "1" };
        }
        fn watchFn(_: *anyopaque, _: mem.Allocator, _: ?[]const u8) anyerror!reflector_mod.WatcherInterface {
            return error.WatchFailed;
        }
    };

    var informer = try Informer.create(testing.allocator, testing.io, .{
        .ptr = @ptrFromInt(1),
        .list_fn = SuccessSource.listFn,
        .watch_fn = SuccessSource.watchFn,
    }, .{});
    defer informer.deinit();

    // Register indexer before adding objects
    try informer.addIndexer("namespace", &store_mod.indexByNamespace);

    // Manually add objects to store for testing
    const a = try testing.allocator.create(Unstructured);
    a.* = try Unstructured.init(testing.allocator);
    try a.setString(&.{ "metadata", "name" }, "pod-a");
    try a.setString(&.{ "metadata", "namespace" }, "default");
    try informer.getStore().add(a);

    const b = try testing.allocator.create(Unstructured);
    b.* = try Unstructured.init(testing.allocator);
    try b.setString(&.{ "metadata", "name" }, "pod-b");
    try b.setString(&.{ "metadata", "namespace" }, "kube-system");
    try informer.getStore().add(b);

    // Query through informer
    const default_pods = try informer.byIndex(testing.allocator, "namespace", "default");
    defer testing.allocator.free(default_pods);
    try testing.expectEqual(@as(usize, 1), default_pods.len);

    const system_pods = try informer.byIndex(testing.allocator, "namespace", "kube-system");
    defer testing.allocator.free(system_pods);
    try testing.expectEqual(@as(usize, 1), system_pods.len);

    // indexKeys
    const keys = try informer.indexKeys(testing.allocator, "namespace", "default");
    defer testing.allocator.free(keys);
    try testing.expectEqual(@as(usize, 1), keys.len);
}

test "informer: Options.indexers auto-registered on create" {
    const SuccessSource2 = struct {
        var items_buf: [0]*Unstructured = .{};

        fn listFn(_: *anyopaque, _: mem.Allocator, _: ?[]const u8) anyerror!reflector_mod.ListResult {
            return .{ .items = &items_buf, .resource_version = "1" };
        }
        fn watchFn(_: *anyopaque, _: mem.Allocator, _: ?[]const u8) anyerror!reflector_mod.WatcherInterface {
            return error.WatchFailed;
        }
    };

    var informer = try Informer.create(testing.allocator, testing.io, .{
        .ptr = @ptrFromInt(1),
        .list_fn = SuccessSource2.listFn,
        .watch_fn = SuccessSource2.watchFn,
    }, .{
        .indexers = &.{
            .{ .name = "namespace", .func = &store_mod.indexByNamespace },
        },
    });
    defer informer.deinit();

    // Indexer should already be registered
    const a = try testing.allocator.create(Unstructured);
    a.* = try Unstructured.init(testing.allocator);
    try a.setString(&.{ "metadata", "name" }, "nginx");
    try a.setString(&.{ "metadata", "namespace" }, "default");
    try informer.getStore().add(a);

    const results = try informer.byIndex(testing.allocator, "namespace", "default");
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 1), results.len);

    // Duplicate registration should fail
    try testing.expectError(error.IndexerAlreadyExists, informer.addIndexer("namespace", &store_mod.indexByNamespace));
}
