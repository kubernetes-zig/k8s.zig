const std = @import("std");
const mem = std.mem;
const json = std.json;
const http = std.http;
const Io = std.Io;
const Allocator = mem.Allocator;
const testing = std.testing;

const k8s = @import("k8s_zig");
const scheme = k8s.scheme;
const Unstructured = k8s.Unstructured;
const StatusError = k8s.errors.StatusError;

const transport_mod = @import("transport.zig");
const Transport = transport_mod.Transport;
const url_mod = @import("url.zig");
const UrlBuilder = url_mod.UrlBuilder;
const config_mod = @import("config.zig");
const watch_mod = @import("watch.zig");
const Watcher = watch_mod.Watcher;

/// DynamicClient provides unstructured CRUD operations against the K8s API.
///
/// Usage:
///   var client = DynamicClient.init(allocator, io, &config);
///   defer client.deinit();
///
///   const pods = client.resource(.{ .group = "", .version = "v1", .resource = "pods" }, .{ .namespace = "default" });
///   const pod = try pods.get("nginx");
///   defer pod.deinit();
pub const DynamicClient = struct {
    allocator: Allocator,
    transport: Transport,
    url_builder: UrlBuilder,

    pub fn init(allocator: Allocator, io: Io, cfg: *const config_mod.Config) !DynamicClient {
        const server = try allocator.dupe(u8, cfg.server);
        return .{
            .allocator = allocator,
            .transport = try Transport.init(allocator, io, cfg),
            .url_builder = UrlBuilder.init(server),
        };
    }

    pub fn deinit(self: *DynamicClient) void {
        self.allocator.free(self.url_builder.base);
        self.transport.deinit();
    }

    /// Create a resource handle scoped to a GVR and optional namespace.
    pub fn resource(self: *DynamicClient, gvr: scheme.GroupVersionResource, opts: ResourceOptions) ResourceClient {
        return .{
            .client = self,
            .gvr = gvr,
            .namespace = opts.namespace,
        };
    }

    pub const ResourceOptions = struct {
        namespace: ?[]const u8 = null,
    };
};

/// A handle for performing operations on a specific resource type.
/// Created by `DynamicClient.resource()`. Does not allocate — just holds references.
pub const ResourceClient = struct {
    client: *DynamicClient,
    gvr: scheme.GroupVersionResource,
    namespace: ?[]const u8,

    /// Get a single resource by name. Returns error on 404.
    pub fn get(self: ResourceClient, name: []const u8) !Unstructured {
        const url = try self.client.url_builder.resource(
            self.client.allocator,
            self.gvr,
            self.namespace,
            name,
        );
        defer self.client.allocator.free(url);

        var resp = try self.client.transport.request(.GET, url, null, null);
        defer resp.deinit();

        if (resp.status >= 400) {
            return statusErrorFromResponse(resp);
        }

        return try Unstructured.fromJson(self.client.allocator, resp.body);
    }

    /// Get a single resource by name. Returns null on 404, error on other failures.
    pub fn getOrNull(self: ResourceClient, name: []const u8) !?Unstructured {
        const url = try self.client.url_builder.resource(
            self.client.allocator,
            self.gvr,
            self.namespace,
            name,
        );
        defer self.client.allocator.free(url);

        var resp = try self.client.transport.request(.GET, url, null, null);
        defer resp.deinit();

        if (resp.status == 404) return null;
        if (resp.status >= 400) {
            return statusErrorFromResponse(resp);
        }

        return try Unstructured.fromJson(self.client.allocator, resp.body);
    }

    /// List resources.
    pub fn list(self: ResourceClient, opts: ListOptions) !Unstructured {
        var url = try self.client.url_builder.resource(
            self.client.allocator,
            self.gvr,
            self.namespace,
            null,
        );
        defer self.client.allocator.free(url);

        var query: std.ArrayList(u8) = .empty;
        defer query.deinit(self.client.allocator);

        if (opts.label_selector) |ls| {
            try appendQueryParam(self.client.allocator, &query, "labelSelector", ls);
        }
        if (opts.field_selector) |fs| {
            try appendQueryParam(self.client.allocator, &query, "fieldSelector", fs);
        }
        if (opts.limit) |limit| {
            const value = try std.fmt.allocPrint(self.client.allocator, "{d}", .{limit});
            defer self.client.allocator.free(value);
            try appendQueryParam(self.client.allocator, &query, "limit", value);
        }
        if (opts.resource_version) |rv| {
            try appendQueryParam(self.client.allocator, &query, "resourceVersion", rv);
        }
        if (opts.continue_token) |token| {
            try appendQueryParam(self.client.allocator, &query, "continue", token);
        }

        if (query.items.len > 0) {
            const new_url = try std.fmt.allocPrint(self.client.allocator, "{s}?{s}", .{ url, query.items });
            self.client.allocator.free(url);
            url = new_url;
        }

        var resp = try self.client.transport.request(.GET, url, null, null);
        defer resp.deinit();

        if (resp.status >= 400) {
            return statusErrorFromResponse(resp);
        }

        return try Unstructured.fromJson(self.client.allocator, resp.body);
    }

    /// Start a watch for this resource.
    pub fn watch(self: ResourceClient, io: Io, opts: WatchOptions) !Watcher {
        return Watcher.start(
            self.client.allocator,
            io,
            &self.client.transport,
            &self.client.url_builder,
            self.gvr,
            .{
                .namespace = opts.namespace orelse self.namespace,
                .resource_version = opts.resource_version,
                .label_selector = opts.label_selector,
                .field_selector = opts.field_selector,
                .allow_bookmarks = opts.allow_bookmarks,
                .queue_size = opts.queue_size,
            },
        );
    }

    /// Create a resource.
    pub fn create(self: ResourceClient, obj: *const Unstructured) !Unstructured {
        const url = try self.client.url_builder.resource(
            self.client.allocator,
            self.gvr,
            self.namespace,
            null,
        );
        defer self.client.allocator.free(url);

        const body = try obj.toJson(self.client.allocator);
        defer self.client.allocator.free(body);

        var resp = try self.client.transport.request(.POST, url, body, "application/json");
        defer resp.deinit();

        if (resp.status >= 400) {
            return statusErrorFromResponse(resp);
        }

        return try Unstructured.fromJson(self.client.allocator, resp.body);
    }

    /// Update (replace) a resource.
    pub fn update(self: ResourceClient, obj: *const Unstructured) !Unstructured {
        const name = obj.getName() orelse return error.MissingName;
        const url = try self.client.url_builder.resource(
            self.client.allocator,
            self.gvr,
            self.namespace,
            name,
        );
        defer self.client.allocator.free(url);

        const body = try obj.toJson(self.client.allocator);
        defer self.client.allocator.free(body);

        var resp = try self.client.transport.request(.PUT, url, body, "application/json");
        defer resp.deinit();

        if (resp.status >= 400) {
            return statusErrorFromResponse(resp);
        }

        return try Unstructured.fromJson(self.client.allocator, resp.body);
    }

    /// Update the status subresource.
    pub fn updateStatus(self: ResourceClient, obj: *const Unstructured) !Unstructured {
        const name = obj.getName() orelse return error.MissingName;
        const url = try self.client.url_builder.status(
            self.client.allocator,
            self.gvr,
            self.namespace,
            name,
        );
        defer self.client.allocator.free(url);

        const body = try obj.toJson(self.client.allocator);
        defer self.client.allocator.free(body);

        var resp = try self.client.transport.request(.PUT, url, body, "application/json");
        defer resp.deinit();

        if (resp.status >= 400) {
            return statusErrorFromResponse(resp);
        }

        return try Unstructured.fromJson(self.client.allocator, resp.body);
    }

    /// Server-side apply.
    pub fn apply(
        self: ResourceClient,
        name: []const u8,
        obj: *const Unstructured,
        opts: ApplyOptions,
    ) !Unstructured {
        const url = try self.client.url_builder.resource(
            self.client.allocator,
            self.gvr,
            self.namespace,
            name,
        );
        defer self.client.allocator.free(url);

        // SSA uses PATCH with application/apply-patch+json (body is JSON)
        const body = try obj.toJson(self.client.allocator);
        defer self.client.allocator.free(body);

        // Build query params: fieldManager + force
        var query: std.ArrayList(u8) = .empty;
        defer query.deinit(self.client.allocator);
        try appendQueryParam(self.client.allocator, &query, "fieldManager", opts.field_manager);
        if (opts.force) {
            try appendQueryParam(self.client.allocator, &query, "force", "true");
        }
        const full_url = try std.fmt.allocPrint(
            self.client.allocator,
            "{s}?{s}",
            .{ url, query.items },
        );
        defer self.client.allocator.free(full_url);

        var resp = try self.client.transport.request(
            .PATCH,
            full_url,
            body,
            "application/apply-patch+json",
        );
        defer resp.deinit();

        if (resp.status >= 400) {
            return statusErrorFromResponse(resp);
        }

        return try Unstructured.fromJson(self.client.allocator, resp.body);
    }

    /// Delete a resource by name. Returns error on 404.
    pub fn delete(self: ResourceClient, name: []const u8, opts: DeleteOptions) !void {
        const url = try self.client.url_builder.resource(
            self.client.allocator,
            self.gvr,
            self.namespace,
            name,
        );
        defer self.client.allocator.free(url);

        const body = try opts.toJson(self.client.allocator);
        defer if (body) |b| self.client.allocator.free(b);
        const ct: ?[]const u8 = if (body != null) "application/json" else null;

        var resp = try self.client.transport.request(.DELETE, url, body, ct);
        defer resp.deinit();

        if (resp.status >= 400) {
            return statusErrorFromResponse(resp);
        }
    }

    /// Delete a resource by name. Returns false on 404, error on other failures.
    pub fn deleteIfExists(self: ResourceClient, name: []const u8, opts: DeleteOptions) !bool {
        const url = try self.client.url_builder.resource(
            self.client.allocator,
            self.gvr,
            self.namespace,
            name,
        );
        defer self.client.allocator.free(url);

        const body = try opts.toJson(self.client.allocator);
        defer if (body) |b| self.client.allocator.free(b);
        const ct: ?[]const u8 = if (body != null) "application/json" else null;

        var resp = try self.client.transport.request(.DELETE, url, body, ct);
        defer resp.deinit();

        if (resp.status == 404) return false;
        if (resp.status >= 400) {
            return statusErrorFromResponse(resp);
        }
        return true;
    }

    /// Patch a resource. Supports JSON merge, strategic merge, and JSON patch.
    pub fn patch(
        self: ResourceClient,
        name: []const u8,
        patch_data: []const u8,
        opts: PatchOptions,
    ) !Unstructured {
        const url = try self.client.url_builder.resource(
            self.client.allocator,
            self.gvr,
            self.namespace,
            name,
        );
        defer self.client.allocator.free(url);

        var resp = try self.client.transport.request(
            .PATCH,
            url,
            patch_data,
            opts.patch_type.contentType(),
        );
        defer resp.deinit();

        if (resp.status >= 400) {
            return statusErrorFromResponse(resp);
        }

        return try Unstructured.fromJson(self.client.allocator, resp.body);
    }
};

pub const ListOptions = struct {
    label_selector: ?[]const u8 = null,
    field_selector: ?[]const u8 = null,
    limit: ?u32 = null,
    resource_version: ?[]const u8 = null,
    continue_token: ?[]const u8 = null,
};

pub const WatchOptions = struct {
    namespace: ?[]const u8 = null,
    resource_version: ?[]const u8 = null,
    label_selector: ?[]const u8 = null,
    field_selector: ?[]const u8 = null,
    allow_bookmarks: bool = true,
    queue_size: usize = 1,
};

pub const ApplyOptions = struct {
    field_manager: []const u8 = "k8s-zig",
    force: bool = false,
};

pub const PatchType = enum {
    json_merge,
    strategic_merge,
    json_patch,

    pub fn contentType(self: PatchType) []const u8 {
        return switch (self) {
            .json_merge => "application/merge-patch+json",
            .strategic_merge => "application/strategic-merge-patch+json",
            .json_patch => "application/json-patch+json",
        };
    }
};

pub const PatchOptions = struct {
    patch_type: PatchType = .json_merge,
};

pub const DeleteOptions = struct {
    propagation_policy: ?PropagationPolicy = null,
    grace_period_seconds: ?i64 = null,
    dry_run: bool = false,
    preconditions: ?Preconditions = null,

    pub const PropagationPolicy = enum {
        background,
        foreground,
        orphan,

        pub fn toString(self: PropagationPolicy) []const u8 {
            return switch (self) {
                .background => "Background",
                .foreground => "Foreground",
                .orphan => "Orphan",
            };
        }
    };

    pub const Preconditions = struct {
        uid: ?[]const u8 = null,
        resource_version: ?[]const u8 = null,
    };

    /// Serialize to JSON body. Returns null if all options are defaults (no body needed).
    pub fn toJson(self: DeleteOptions, allocator: Allocator) !?[]const u8 {
        if (self.propagation_policy == null and
            self.grace_period_seconds == null and
            !self.dry_run and
            self.preconditions == null) return null;

        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);

        try buf.appendSlice(allocator, "{");
        var first = true;

        if (self.propagation_policy) |pp| {
            try buf.appendSlice(allocator, "\"propagationPolicy\":\"");
            try buf.appendSlice(allocator, pp.toString());
            try buf.appendSlice(allocator, "\"");
            first = false;
        }
        if (self.grace_period_seconds) |gp| {
            if (!first) try buf.appendSlice(allocator, ",");
            const gp_str = try std.fmt.allocPrint(allocator, "\"gracePeriodSeconds\":{d}", .{gp});
            defer allocator.free(gp_str);
            try buf.appendSlice(allocator, gp_str);
            first = false;
        }
        if (self.dry_run) {
            if (!first) try buf.appendSlice(allocator, ",");
            try buf.appendSlice(allocator, "\"dryRun\":[\"All\"]");
            first = false;
        }
        if (self.preconditions) |pc| {
            if (!first) try buf.appendSlice(allocator, ",");
            try buf.appendSlice(allocator, "\"preconditions\":{");
            var pc_first = true;
            if (pc.uid) |uid| {
                try buf.appendSlice(allocator, "\"uid\":\"");
                try buf.appendSlice(allocator, uid);
                try buf.appendSlice(allocator, "\"");
                pc_first = false;
            }
            if (pc.resource_version) |rv| {
                if (!pc_first) try buf.appendSlice(allocator, ",");
                try buf.appendSlice(allocator, "\"resourceVersion\":\"");
                try buf.appendSlice(allocator, rv);
                try buf.appendSlice(allocator, "\"");
            }
            try buf.appendSlice(allocator, "}");
        }

        try buf.appendSlice(allocator, "}");
        return try buf.toOwnedSlice(allocator);
    }
};

fn appendQueryParam(
    allocator: Allocator,
    query: *std.ArrayList(u8),
    key: []const u8,
    value: []const u8,
) !void {
    if (query.items.len > 0) {
        try query.append(allocator, '&');
    }
    try query.appendSlice(allocator, key);
    try query.append(allocator, '=');
    try appendPercentEncoded(allocator, query, value);
}

/// Percent-encode a query parameter value per RFC 3986 §3.4.
/// Unreserved characters (A-Z, a-z, 0-9, '-', '.', '_', '~') are passed through.
fn appendPercentEncoded(allocator: Allocator, buf: *std.ArrayList(u8), value: []const u8) !void {
    for (value) |c| {
        if (std.ascii.isAlphanumeric(c) or c == '-' or c == '.' or c == '_' or c == '~') {
            try buf.append(allocator, c);
        } else {
            try buf.appendSlice(allocator, &percentEncodeByte(c));
        }
    }
}

fn percentEncodeByte(c: u8) [3]u8 {
    const hex = "0123456789ABCDEF";
    return .{ '%', hex[c >> 4], hex[c & 0x0F] };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "percentEncode: table-driven" {
    const cases = .{
        .{ .input = "simple", .expected = "simple" },
        .{ .input = "app=nginx", .expected = "app%3Dnginx" },
        .{ .input = "a b", .expected = "a%20b" },
        .{ .input = "key&val", .expected = "key%26val" },
        .{ .input = "100%", .expected = "100%25" },
        .{ .input = "hello/world", .expected = "hello%2Fworld" },
        .{ .input = "safe-._~chars", .expected = "safe-._~chars" },
        .{ .input = "", .expected = "" },
        .{ .input = "metadata.name=my pod", .expected = "metadata.name%3Dmy%20pod" },
    };
    inline for (cases) |c| {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(testing.allocator);
        try appendPercentEncoded(testing.allocator, &buf, c.input);
        try testing.expectEqualStrings(c.expected, buf.items);
    }
}

test "appendQueryParam: encodes values" {
    var query: std.ArrayList(u8) = .empty;
    defer query.deinit(testing.allocator);

    try appendQueryParam(testing.allocator, &query, "labelSelector", "app=nginx");
    try testing.expectEqualStrings("labelSelector=app%3Dnginx", query.items);

    try appendQueryParam(testing.allocator, &query, "limit", "100");
    try testing.expectEqualStrings("labelSelector=app%3Dnginx&limit=100", query.items);
}

test "statusErrorFromResponse: maps HTTP status codes" {
    const Case = struct { status: u16, expected: anyerror };
    const cases = [_]Case{
        .{ .status = 401, .expected = error.Unauthorized },
        .{ .status = 403, .expected = error.Forbidden },
        .{ .status = 404, .expected = error.NotFound },
        .{ .status = 409, .expected = error.Conflict },
        .{ .status = 410, .expected = error.Gone },
        .{ .status = 422, .expected = error.Invalid },
        .{ .status = 500, .expected = error.ApiError },
        .{ .status = 503, .expected = error.ApiError },
    };
    for (cases) |c| {
        const resp = transport_mod.Response{ .status = c.status, .body = "", .allocator = testing.allocator };
        const err = statusErrorFromResponse(resp);
        try testing.expectEqual(c.expected, err);
    }
}

test "PatchType: content type mapping" {
    const cases = .{
        .{ .pt = PatchType.json_merge, .expected = "application/merge-patch+json" },
        .{ .pt = PatchType.strategic_merge, .expected = "application/strategic-merge-patch+json" },
        .{ .pt = PatchType.json_patch, .expected = "application/json-patch+json" },
    };
    inline for (cases) |c| {
        try testing.expectEqualStrings(c.expected, c.pt.contentType());
    }
}

test "DeleteOptions: toJson table-driven" {
    const Case = struct {
        opts: DeleteOptions,
        expected: ?[]const u8, // null means no body
    };
    const cases = [_]Case{
        // All defaults → no body
        .{ .opts = .{}, .expected = null },
        // Propagation policy only
        .{
            .opts = .{ .propagation_policy = .foreground },
            .expected = "{\"propagationPolicy\":\"Foreground\"}",
        },
        // Grace period
        .{
            .opts = .{ .grace_period_seconds = 30 },
            .expected = "{\"gracePeriodSeconds\":30}",
        },
        // Dry run
        .{
            .opts = .{ .dry_run = true },
            .expected = "{\"dryRun\":[\"All\"]}",
        },
        // Preconditions with uid
        .{
            .opts = .{ .preconditions = .{ .uid = "abc-123" } },
            .expected = "{\"preconditions\":{\"uid\":\"abc-123\"}}",
        },
        // Combined
        .{
            .opts = .{ .propagation_policy = .background, .grace_period_seconds = 0 },
            .expected = "{\"propagationPolicy\":\"Background\",\"gracePeriodSeconds\":0}",
        },
    };
    for (cases) |c| {
        const result = try c.opts.toJson(testing.allocator);
        defer if (result) |r| testing.allocator.free(r);
        if (c.expected) |exp| {
            try testing.expect(result != null);
            try testing.expectEqualStrings(exp, result.?);
        } else {
            try testing.expect(result == null);
        }
    }
}

fn statusErrorFromResponse(resp: transport_mod.Response) error{ ApiError, NotFound, Conflict, Gone, Unauthorized, Forbidden, Invalid } {
    return switch (resp.status) {
        401 => error.Unauthorized,
        403 => error.Forbidden,
        404 => error.NotFound,
        409 => error.Conflict,
        410 => error.Gone,
        422 => error.Invalid,
        else => error.ApiError,
    };
}
