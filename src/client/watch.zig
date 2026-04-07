const std = @import("std");
const mem = std.mem;
const json = std.json;
const http = std.http;
const Io = std.Io;
const Uri = std.Uri;
const Allocator = mem.Allocator;
const testing = std.testing;

const k8s = @import("k8s_zig");
const scheme = k8s.scheme;
const Unstructured = k8s.Unstructured;

const transport_mod = @import("transport.zig");
const Transport = transport_mod.Transport;
const tls_transport_mod = @import("tls_transport.zig");
const TlsTransport = tls_transport_mod.TlsTransport;
const url_mod = @import("url.zig");
const UrlBuilder = url_mod.UrlBuilder;

/// K8s watch event types. Matches Go's watch.EventType.
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

/// A single watch event. Matches Go's watch.Event.
pub const Event = struct {
    event_type: EventType,
    object: Unstructured,

    pub fn deinit(self: *Event) void {
        self.object.deinit();
    }

    pub fn resourceVersion(self: *const Event) ?[]const u8 {
        return self.object.getResourceVersion();
    }
};

/// Reason the watch stream ended.
pub const StopReason = enum {
    /// Normal EOF — server closed the connection.
    eof,
    /// Unexpected EOF — connection dropped mid-stream.
    unexpected_eof,
    /// Network timeout.
    timeout,
    /// Consumer called stop().
    stopped,
    /// HTTP error on watch setup (non-200 status).
    http_error,
    /// Decode error that isn't EOF/timeout.
    decode_error,
};

/// Watcher opens a streaming HTTP watch connection and pushes events to a queue.
///
/// Matches Go's watch.Interface / StreamWatcher pattern:
///   - Background task reads from HTTP stream, decodes events, pushes to queue
///   - Consumer reads from queue, can use io.select for multiplexing
///   - On decode errors (non-EOF), an Error event is pushed to the queue
///   - On stream end, the queue signals completion
///   - Stop() cancels the background task and closes the stream
///
/// Usage:
///   var watcher = try Watcher.start(allocator, io, &transport, &url_builder, gvr, .{});
///   defer watcher.stop(io);
///
///   while (watcher.events.getOne(io)) |event| {
///       defer event.deinit();
///       // handle event
///   } else |err| switch (err) {
///       error.Canceled => {}, // stream ended
///   }
pub const Watcher = struct {
    allocator: Allocator,
    /// Event queue — consumer reads from here.
    events: Io.Queue(Event),
    /// Buffer backing the event queue.
    event_buf: []Event,
    /// Background decoder task future.
    decoder_future: ?Io.Future(DecodeResult),
    /// Why the stream ended (set by background task).
    stop_reason: StopReason,
    stopped: bool,

    const DecodeResult = Io.Cancelable!void;

    pub const Options = struct {
        namespace: ?[]const u8 = null,
        resource_version: ?[]const u8 = null,
        label_selector: ?[]const u8 = null,
        field_selector: ?[]const u8 = null,
        allow_bookmarks: bool = true,
        /// Event queue buffer size. Go uses 0 (unbuffered channel).
        /// Io.Queue requires >= 1. Use 1 for closest parity with Go.
        queue_size: usize = 1,
    };

    /// Start a watch stream. Opens the HTTP connection, verifies 200 status,
    /// then spawns a background decoder task.
    pub fn start(
        allocator: Allocator,
        io: Io,
        transport: *Transport,
        url_builder: *const UrlBuilder,
        gvr: scheme.GroupVersionResource,
        opts: Options,
    ) !Watcher {
        const buf_size = if (opts.queue_size > 0) opts.queue_size else 1;
        const event_buf = try allocator.alloc(Event, buf_size);

        var watcher = Watcher{
            .allocator = allocator,
            .events = Io.Queue(Event).init(event_buf),
            .event_buf = event_buf,
            .decoder_future = null,
            .stop_reason = .eof,
            .stopped = false,
        };

        const watch_url = try buildWatchUrl(allocator, url_builder, gvr, opts);

        // Start background decoder — it owns watch_url and will free it
        watcher.decoder_future = try io.concurrent(decodeLoop, .{
            allocator,
            io,
            transport,
            watch_url,
            &watcher.events,
            &watcher.stop_reason,
        });

        return watcher;
    }

    /// Stop the watcher. Cancels background task and frees resources.
    /// Must always be called, even if the stream ended naturally (matches Go).
    pub fn stop(self: *Watcher, io: Io) void {
        if (self.stopped) return;
        if (self.decoder_future) |*f| {
            _ = f.cancel(io) catch {};
            self.decoder_future = null;
        }
        self.stop_reason = .stopped;
        self.stopped = true;
        self.allocator.free(self.event_buf);
    }

    /// Background decode loop. Reads newline-delimited JSON from the HTTP
    /// stream and pushes events to the queue. On errors, pushes an Error
    /// event (matching Go's StreamWatcher.receive behavior).
    fn decodeLoop(
        allocator: Allocator,
        io: Io,
        transport: *Transport,
        watch_url: []const u8,
        events: *Io.Queue(Event),
        stop_reason: *StopReason,
    ) Io.Cancelable!void {
        defer allocator.free(watch_url);
        defer events.close(io);

        const auth = getAuthHeader(allocator, transport) catch null;
        defer if (auth) |a| allocator.free(a);

        // Establish connection via tls.zig or std.http.Client
        if (transport.tls_transport) |*tt| {
            var stream_resp = tt.streamRequest(.GET, watch_url, auth) catch {
                stop_reason.* = .http_error;
                return;
            };
            defer stream_resp.deinit();

            if (stream_resp.status != 200) {
                stop_reason.* = .http_error;
                return;
            }

            readEventStream(allocator, io, events, stop_reason, StreamReader{ .tls = &stream_resp });
        } else {
            var extra_headers_buf: [2]http.Header = undefined;
            var extra_count: usize = 0;
            extra_headers_buf[extra_count] = .{ .name = "Accept", .value = "application/json" };
            extra_count += 1;
            if (auth) |a| {
                extra_headers_buf[extra_count] = .{ .name = "Authorization", .value = a };
                extra_count += 1;
            }

            var request = transport.openRequest(.GET, watch_url, .{
                .extra_headers = extra_headers_buf[0..extra_count],
                .keep_alive = true,
            }) catch {
                stop_reason.* = .http_error;
                return;
            };
            defer request.deinit();

            request.sendBodiless() catch {
                stop_reason.* = .http_error;
                return;
            };

            var redirect_buf: [8192]u8 = undefined;
            var response = request.receiveHead(&redirect_buf) catch {
                stop_reason.* = .http_error;
                return;
            };

            const status_code = @intFromEnum(response.head.status);
            if (status_code != 200) {
                stop_reason.* = .http_error;
                var read_buf: [4096]u8 = undefined;
                const reader = response.reader(&read_buf);
                var body_buf: std.ArrayList(u8) = .empty;
                defer body_buf.deinit(allocator);
                while (true) {
                    const byte = reader.takeByte() catch break;
                    body_buf.append(allocator, byte) catch break;
                }
                if (body_buf.items.len > 0) {
                    if (makeErrorEvent(allocator, body_buf.items)) |err_event| {
                        events.putOne(io, err_event) catch {};
                    } else |_| {}
                }
                return;
            }

            var read_buf: [65536]u8 = undefined;
            const reader = response.reader(&read_buf);
            readEventStream(allocator, io, events, stop_reason, StreamReader{ .http = reader });
        }
    }

    pub const StreamReader = union(enum) {
        http: *Io.Reader,
        tls: *TlsTransport.StreamResponse,
        /// Test-only: reads from a fixed byte buffer.
        buffer: BufferReader,

        const BufferReader = struct {
            data: []const u8,
            pos: usize = 0,

            fn readByte(self: *BufferReader) !u8 {
                if (self.pos >= self.data.len) return error.EndOfStream;
                const b = self.data[self.pos];
                self.pos += 1;
                return b;
            }
        };

        fn readByte(self: *StreamReader) !u8 {
            switch (self.*) {
                .http => |r| return r.takeByte() catch |err| switch (err) {
                    error.EndOfStream => return error.EndOfStream,
                    else => return error.ReadFailed,
                },
                .tls => |sr| return sr.readByte(),
                .buffer => |*br| return br.readByte(),
            }
        }
    };

    /// Shared event-reading loop for both transport paths.
    pub fn readEventStream(
        allocator: Allocator,
        io: Io,
        events: *Io.Queue(Event),
        stop_reason: *StopReason,
        reader: StreamReader,
    ) void {
        var sr = reader;
        var line_buf: std.ArrayList(u8) = .empty;
        defer line_buf.deinit(allocator);

        while (true) {
            const byte = sr.readByte() catch |read_err| {
                switch (read_err) {
                    error.EndOfStream => {
                        if (line_buf.items.len > 0) {
                            stop_reason.* = .unexpected_eof;
                        } else {
                            stop_reason.* = .eof;
                        }
                    },
                    else => {
                        stop_reason.* = .decode_error;
                        if (makeStatusErrorEvent(allocator, "watch stream read error")) |err_event| {
                            events.putOne(io, err_event) catch {};
                        } else |_| {}
                    },
                }
                return;
            };

            if (byte == '\n') {
                if (line_buf.items.len > 0) {
                    if (parseWatchEvent(allocator, line_buf.items)) |event| {
                        events.putOne(io, event) catch |err| switch (err) {
                            error.Canceled, error.Closed => {
                                var ev = event;
                                ev.deinit();
                                stop_reason.* = .stopped;
                                return;
                            },
                        };
                    } else |_| {
                        if (makeStatusErrorEvent(allocator, "unable to decode watch event")) |err_event| {
                            events.putOne(io, err_event) catch {};
                        } else |_| {}
                    }
                    line_buf.clearRetainingCapacity();
                }
            } else {
                line_buf.append(allocator, byte) catch {
                    stop_reason.* = .decode_error;
                    return;
                };
            }
        }
    }
};

/// Parse a watch event from a JSON line.
/// Format: {"type":"ADDED","object":{...}}
pub fn parseWatchEvent(allocator: Allocator, line: []const u8) !Event {
    const parsed = try json.parseFromSlice(json.Value, allocator, line, .{});
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidWatchEvent;
    const obj = parsed.value.object;

    const type_str = if (obj.get("type")) |v| switch (v) {
        .string => |s| s,
        else => return error.InvalidWatchEvent,
    } else return error.InvalidWatchEvent;

    const event_type = EventType.fromString(type_str) orelse return error.InvalidWatchEvent;

    const object_val = obj.get("object") orelse return error.InvalidWatchEvent;
    if (object_val != .object) return error.InvalidWatchEvent;

    const unstructured = try Unstructured.fromJsonValue(allocator, object_val);

    return .{
        .event_type = event_type,
        .object = unstructured,
    };
}

/// Create an Error event from a raw JSON response body (e.g., Status).
fn makeErrorEvent(allocator: Allocator, body: []const u8) !Event {
    const obj = try Unstructured.fromJson(allocator, body);
    return .{ .event_type = .err, .object = obj };
}

/// Create an Error event with a synthetic Status-like object.
fn makeStatusErrorEvent(allocator: Allocator, message: []const u8) !Event {
    var obj = try Unstructured.init(allocator);
    try obj.setString(&.{"kind"}, "Status");
    try obj.setString(&.{"apiVersion"}, "v1");
    try obj.setString(&.{"status"}, "Failure");
    try obj.setString(&.{"message"}, message);
    return .{ .event_type = .err, .object = obj };
}

/// Build the full watch URL with query parameters.
pub fn buildWatchUrl(
    allocator: Allocator,
    url_builder: *const UrlBuilder,
    gvr: scheme.GroupVersionResource,
    opts: Watcher.Options,
) ![]const u8 {
    const base = try url_builder.resource(allocator, gvr, opts.namespace, null);
    defer allocator.free(base);

    var parts: std.ArrayList(u8) = .empty;
    defer parts.deinit(allocator);

    try parts.appendSlice(allocator, base);
    try parts.append(allocator, '?');
    try parts.appendSlice(allocator, "watch=true");

    if (opts.resource_version) |rv| {
        try parts.appendSlice(allocator, "&resourceVersion=");
        try appendPercentEncoded(allocator, &parts, rv);
    }

    if (opts.allow_bookmarks) {
        try parts.appendSlice(allocator, "&allowWatchBookmarks=true");
    }

    if (opts.label_selector) |ls| {
        try parts.appendSlice(allocator, "&labelSelector=");
        try appendPercentEncoded(allocator, &parts, ls);
    }

    if (opts.field_selector) |fs| {
        try parts.appendSlice(allocator, "&fieldSelector=");
        try appendPercentEncoded(allocator, &parts, fs);
    }

    return try parts.toOwnedSlice(allocator);
}

fn getAuthHeader(allocator: Allocator, transport: *Transport) !?[]const u8 {
    return transport.authHeader(allocator);
}

/// Percent-encode a query parameter value per RFC 3986 §3.4.
fn appendPercentEncoded(allocator: Allocator, buf: *std.ArrayList(u8), value: []const u8) !void {
    for (value) |c| {
        if (std.ascii.isAlphanumeric(c) or c == '-' or c == '.' or c == '_' or c == '~') {
            try buf.append(allocator, c);
        } else {
            const hex = "0123456789ABCDEF";
            try buf.appendSlice(allocator, &[3]u8{ '%', hex[c >> 4], hex[c & 0x0F] });
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

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

test "parseWatchEvent: all event types" {
    const Case = struct { line: []const u8, expected_type: EventType, expected_rv: []const u8 };
    const cases = [_]Case{
        .{ .line = \\{"type":"ADDED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"nginx","resourceVersion":"1234"}}}
        , .expected_type = .added, .expected_rv = "1234" },
        .{ .line = \\{"type":"MODIFIED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"nginx","resourceVersion":"1235"}}}
        , .expected_type = .modified, .expected_rv = "1235" },
        .{ .line = \\{"type":"DELETED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"nginx","resourceVersion":"1236"}}}
        , .expected_type = .deleted, .expected_rv = "1236" },
        .{ .line = \\{"type":"BOOKMARK","object":{"apiVersion":"v1","kind":"Pod","metadata":{"resourceVersion":"9999"}}}
        , .expected_type = .bookmark, .expected_rv = "9999" },
    };
    for (cases) |c| {
        var event = try parseWatchEvent(testing.allocator, c.line);
        defer event.deinit();
        try testing.expectEqual(c.expected_type, event.event_type);
        try testing.expectEqualStrings(c.expected_rv, event.resourceVersion().?);
    }
}

test "parseWatchEvent: ERROR with Status" {
    var event = try parseWatchEvent(testing.allocator,
        \\{"type":"ERROR","object":{"kind":"Status","apiVersion":"v1","status":"Failure","message":"too old","reason":"Gone","code":410}}
    );
    defer event.deinit();
    try testing.expectEqual(EventType.err, event.event_type);
    try testing.expectEqualStrings("Gone", event.object.field("reason").str().?);
    try testing.expectEqual(@as(i64, 410), event.object.field("code").int().?);
}

test "parseWatchEvent: error cases" {
    const Case = struct { line: []const u8, expected_err: anyerror };
    const cases = [_]Case{
        .{ .line = "not json", .expected_err = error.SyntaxError },
        .{ .line = \\{"object":{}}
        , .expected_err = error.InvalidWatchEvent },
        .{ .line = \\{"type":"INVALID","object":{}}
        , .expected_err = error.InvalidWatchEvent },
        .{ .line = \\{"type":"ADDED"}
        , .expected_err = error.InvalidWatchEvent },
    };
    for (cases) |c| {
        const result = parseWatchEvent(testing.allocator, c.line);
        try testing.expectError(c.expected_err, result);
    }
}

test "makeStatusErrorEvent: synthetic error" {
    var event = try makeStatusErrorEvent(testing.allocator, "something broke");
    defer event.deinit();
    try testing.expectEqual(EventType.err, event.event_type);
    try testing.expectEqualStrings("Status", event.object.getKind().?);
    try testing.expectEqualStrings("Failure", event.object.field("status").str().?);
    try testing.expectEqualStrings("something broke", event.object.field("message").str().?);
}

test "makeErrorEvent: from Status JSON" {
    var event = try makeErrorEvent(testing.allocator,
        \\{"kind":"Status","code":410,"reason":"Gone","message":"too old resource version"}
    );
    defer event.deinit();
    try testing.expectEqual(EventType.err, event.event_type);
    try testing.expectEqual(@as(i64, 410), event.object.field("code").int().?);
}

test "buildWatchUrl: full options" {
    const Case = struct {
        gvr: scheme.GroupVersionResource,
        opts: Watcher.Options,
        expected_parts: []const []const u8,
    };
    const cases = [_]Case{
        .{
            .gvr = .{ .group = "", .version = "v1", .resource = "pods" },
            .opts = .{ .namespace = "default", .resource_version = "12345" },
            .expected_parts = &.{ "/api/v1/namespaces/default/pods", "watch=true", "resourceVersion=12345", "allowWatchBookmarks=true" },
        },
        .{
            .gvr = .{ .group = "apps", .version = "v1", .resource = "deployments" },
            .opts = .{ .namespace = "prod", .label_selector = "app=nginx", .field_selector = "metadata.name=web" },
            .expected_parts = &.{ "/apis/apps/v1/namespaces/prod/deployments", "labelSelector=app%3Dnginx", "fieldSelector=metadata.name%3Dweb" },
        },
        .{
            .gvr = .{ .group = "", .version = "v1", .resource = "nodes" },
            .opts = .{ .allow_bookmarks = false },
            .expected_parts = &.{ "/api/v1/nodes", "watch=true" },
        },
    };
    const ub = UrlBuilder.init("https://k8s.example.com");
    for (cases) |c| {
        const url = try buildWatchUrl(testing.allocator, &ub, c.gvr, c.opts);
        defer testing.allocator.free(url);
        for (c.expected_parts) |part| {
            try testing.expect(mem.indexOf(u8, url, part) != null);
        }
    }
}

test "buildWatchUrl: no bookmarks excluded from URL" {
    const ub = UrlBuilder.init("https://k8s.example.com");
    const url = try buildWatchUrl(testing.allocator, &ub, .{ .group = "", .version = "v1", .resource = "nodes" }, .{
        .allow_bookmarks = false,
    });
    defer testing.allocator.free(url);
    try testing.expect(mem.indexOf(u8, url, "allowWatchBookmarks") == null);
}

test "buildWatchUrl: percent-encodes special characters in selectors" {
    const ub = UrlBuilder.init("https://k8s.example.com");
    const cases = .{
        .{
            .opts = Watcher.Options{ .label_selector = "app=nginx,env=prod", .field_selector = "status.phase=Running" },
            .expected_parts = &.{ "labelSelector=app%3Dnginx%2Cenv%3Dprod", "fieldSelector=status.phase%3DRunning" },
        },
        .{
            .opts = Watcher.Options{ .resource_version = "12345" },
            .expected_parts = &.{"resourceVersion=12345"},
        },
        .{
            .opts = Watcher.Options{ .label_selector = "simple" },
            .expected_parts = &.{"labelSelector=simple"},
        },
    };
    inline for (cases) |c| {
        const url = try buildWatchUrl(testing.allocator, &ub, .{ .group = "", .version = "v1", .resource = "pods" }, c.opts);
        defer testing.allocator.free(url);
        inline for (c.expected_parts) |part| {
            try testing.expect(mem.indexOf(u8, url, part) != null);
        }
    }
}

test "readEventStream: parses events from buffer reader" {
    const event_line =
        \\{"type":"ADDED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"nginx","namespace":"default","resourceVersion":"1"}}}
    ;
    const stream_data = event_line ++ "\n";

    var event_buf: [4]Event = undefined;
    var events = Io.Queue(Event).init(&event_buf);
    var stop_reason: StopReason = .eof;

    Watcher.readEventStream(
        testing.allocator,
        testing.io,
        &events,
        &stop_reason,
        Watcher.StreamReader{ .buffer = .{ .data = stream_data } },
    );

    try testing.expectEqual(StopReason.eof, stop_reason);

    // Drain the event
    const event = events.getOne(testing.io) catch null;
    try testing.expect(event != null);
    var ev = event.?;
    defer ev.deinit();
    try testing.expectEqual(EventType.added, ev.event_type);
    try testing.expectEqualStrings("nginx", ev.object.getName().?);
}

test "readEventStream: multiple events" {
    const line1 =
        \\{"type":"ADDED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"a","namespace":"default","resourceVersion":"1"}}}
    ;
    const line2 =
        \\{"type":"MODIFIED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"a","namespace":"default","resourceVersion":"2"}}}
    ;
    const stream_data = line1 ++ "\n" ++ line2 ++ "\n";

    var event_buf: [4]Event = undefined;
    var events = Io.Queue(Event).init(&event_buf);
    var stop_reason: StopReason = .eof;

    Watcher.readEventStream(
        testing.allocator,
        testing.io,
        &events,
        &stop_reason,
        Watcher.StreamReader{ .buffer = .{ .data = stream_data } },
    );

    try testing.expectEqual(StopReason.eof, stop_reason);

    // First event
    var ev1 = (events.getOne(testing.io) catch null).?;
    defer ev1.deinit();
    try testing.expectEqual(EventType.added, ev1.event_type);

    // Second event
    var ev2 = (events.getOne(testing.io) catch null).?;
    defer ev2.deinit();
    try testing.expectEqual(EventType.modified, ev2.event_type);
    try testing.expectEqualStrings("2", ev2.object.getResourceVersion().?);
}

test "EventType: fromString unknown returns null" {
    const unknown_strings = [_][]const u8{
        "INVALID",
        "added",
        "Modified",
        "",
        "UNKNOWN",
        "WATCHING",
        "ADD",
    };
    for (unknown_strings) |s| {
        try testing.expect(EventType.fromString(s) == null);
    }
}

test "readEventStream: malformed JSON line" {
    // A malformed JSON line followed by a valid event — stream should continue
    // and produce an error event for the bad line, then the valid event.
    const bad_line = "{\"invalid json\n";
    const good_line =
        \\{"type":"ADDED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"nginx","resourceVersion":"1"}}}
    ;
    const stream_data = bad_line ++ good_line ++ "\n";

    var event_buf: [8]Event = undefined;
    var events = Io.Queue(Event).init(&event_buf);
    var stop_reason: StopReason = .eof;

    Watcher.readEventStream(
        testing.allocator,
        testing.io,
        &events,
        &stop_reason,
        Watcher.StreamReader{ .buffer = .{ .data = stream_data } },
    );

    try testing.expectEqual(StopReason.eof, stop_reason);

    // First event should be an error (from malformed JSON)
    const ev1_opt = events.getOne(testing.io) catch null;
    try testing.expect(ev1_opt != null);
    var ev1 = ev1_opt.?;
    defer ev1.deinit();
    try testing.expectEqual(EventType.err, ev1.event_type);

    // Second event should be the valid ADDED event
    const ev2_opt = events.getOne(testing.io) catch null;
    try testing.expect(ev2_opt != null);
    var ev2 = ev2_opt.?;
    defer ev2.deinit();
    try testing.expectEqual(EventType.added, ev2.event_type);
    try testing.expectEqualStrings("nginx", ev2.object.getName().?);
}

test "readEventStream: unexpected EOF mid-line" {
    const partial = "{\"type\":\"ADDED\",\"object\":{";

    var event_buf: [4]Event = undefined;
    var events = Io.Queue(Event).init(&event_buf);
    var stop_reason: StopReason = .eof;

    Watcher.readEventStream(
        testing.allocator,
        testing.io,
        &events,
        &stop_reason,
        Watcher.StreamReader{ .buffer = .{ .data = partial } },
    );

    try testing.expectEqual(StopReason.unexpected_eof, stop_reason);
}

test "readEventStream: empty stream" {
    var event_buf: [4]Event = undefined;
    var events = Io.Queue(Event).init(&event_buf);
    var stop_reason: StopReason = .decode_error;

    Watcher.readEventStream(
        testing.allocator,
        testing.io,
        &events,
        &stop_reason,
        Watcher.StreamReader{ .buffer = .{ .data = "" } },
    );

    try testing.expectEqual(StopReason.eof, stop_reason);
}

test "fuzz: parseWatchEvent never crashes on arbitrary JSON" {
    try std.testing.fuzz({}, fuzzParseWatchEvent, .{});
}

fn fuzzParseWatchEvent(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    const len = smith.valueRangeAtMost(u16, 0, 1024);
    var buf: [1024]u8 = undefined;
    for (buf[0..len]) |*b| {
        b.* = smith.valueRangeAtMost(u8, 0, 127);
    }
    if (parseWatchEvent(testing.allocator, buf[0..len])) |ev_val| {
        var ev = ev_val;
        ev.deinit();
    } else |_| {}
}

test "fuzz: readEventStream never crashes on arbitrary bytes" {
    try std.testing.fuzz({}, fuzzReadEventStream, .{});
}

fn fuzzReadEventStream(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    // Keep input short to avoid generating many valid events that fill the queue
    const len = smith.valueRangeAtMost(u8, 0, 64);
    var buf: [64]u8 = undefined;
    for (buf[0..len]) |*b| {
        b.* = smith.valueRangeAtMost(u8, 0, 255);
    }

    // Use large queue to avoid blocking on putOne
    var event_buf: [256]Event = undefined;
    var events = Io.Queue(Event).init(&event_buf);
    var stop_reason: StopReason = .eof;

    Watcher.readEventStream(
        testing.allocator,
        testing.io,
        &events,
        &stop_reason,
        Watcher.StreamReader{ .buffer = .{ .data = buf[0..len] } },
    );

    // Close queue so getOne doesn't block
    events.close(testing.io);

    // Drain and free any events that were parsed
    while (true) {
        var ev = events.getOne(testing.io) catch break;
        ev.deinit();
    }
}
