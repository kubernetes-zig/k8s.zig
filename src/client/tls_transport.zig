const std = @import("std");
const mem = std.mem;
const http = std.http;
const Io = std.Io;
const Uri = std.Uri;
const Allocator = mem.Allocator;
const testing = std.testing;
const tls = @import("tls");
const config_mod = @import("config.zig");
const pem_mod = @import("pem.zig");

/// Read a chunked transfer-encoded body. Format: hex-size\r\n data\r\n ... 0\r\n\r\n
fn readChunkedBody(conn: *tls.Connection, allocator: Allocator, body: *std.ArrayList(u8)) !void {
    while (true) {
        // Read chunk size line: hex digits followed by \r\n
        var size_buf: [32]u8 = undefined;
        var size_len: usize = 0;
        while (size_len < size_buf.len) {
            var byte_buf: [1]u8 = undefined;
            const n = conn.read(&byte_buf) catch return;
            if (n == 0) return;
            if (byte_buf[0] == '\r') {
                // Read the \n
                var lf: [1]u8 = undefined;
                _ = conn.read(&lf) catch return;
                break;
            }
            size_buf[size_len] = byte_buf[0];
            size_len += 1;
        }

        // Parse hex chunk size
        const chunk_size = std.fmt.parseInt(usize, size_buf[0..size_len], 16) catch return;
        if (chunk_size == 0) {
            // Terminal chunk — read trailing \r\n
            var trail: [2]u8 = undefined;
            _ = conn.read(&trail) catch {};
            return;
        }

        // Read chunk data
        try body.ensureUnusedCapacity(allocator, chunk_size);
        var remaining = chunk_size;
        var buf: [4096]u8 = undefined;
        while (remaining > 0) {
            const to_read = @min(remaining, buf.len);
            const n = conn.read(buf[0..to_read]) catch return;
            if (n == 0) return;
            try body.appendSlice(allocator, buf[0..n]);
            remaining -= n;
        }

        // Read trailing \r\n after chunk data
        var trail: [2]u8 = undefined;
        _ = conn.read(&trail) catch return;
    }
}

fn componentToStr(comp: Uri.Component) []const u8 {
    return switch (comp) {
        .raw => |r| r,
        .percent_encoded => |p| p,
    };
}

/// Build the HTTP request-target from a parsed URI: `path?query` if a query
/// is present, else just `path`. The result is written into `buf` and returned
/// as a slice into it.
fn buildRequestTarget(uri: Uri, buf: []u8) ![]const u8 {
    const path = if (uri.path.isEmpty()) "/" else componentToStr(uri.path);
    if (uri.query) |q| {
        const q_str = componentToStr(q);
        if (path.len + 1 + q_str.len > buf.len) return error.RequestTargetTooLong;
        @memcpy(buf[0..path.len], path);
        buf[path.len] = '?';
        @memcpy(buf[path.len + 1 ..][0..q_str.len], q_str);
        return buf[0 .. path.len + 1 + q_str.len];
    }
    if (path.len > buf.len) return error.RequestTargetTooLong;
    @memcpy(buf[0..path.len], path);
    return buf[0..path.len];
}

/// TLS transport backed by tls.zig for mTLS and insecure connections.
/// Provides HTTP/1.1 request/response framing over a tls.zig connection
/// with connection pooling (keep-alive reuse).
pub const TlsTransport = struct {
    allocator: Allocator,
    io: Io,

    // TLS config
    host: []const u8,
    port: u16,
    insecure: bool,
    root_ca: ?tls.config.cert.Bundle,
    auth: ?tls.config.CertKeyPair,
    /// Override hostname used for TLS handshake (SNI + cert verification),
    /// from kubeconfig `tls-server-name`. Falls back to `host` if null.
    tls_server_name: ?[]const u8,

    // Connection timeout
    timeout: Io.Timeout,

    // Connection pool: single idle connection (K8s API is one host)
    idle_conn: ?PooledConnection = null,

    /// Heap-allocated state that tls.zig Connection holds pointers into.
    /// Must outlive the tls.Connection.
    const TlsState = struct {
        stream: Io.net.Stream,
        input_buf: [tls.input_buffer_len]u8,
        output_buf: [tls.output_buffer_len]u8,
        reader: Io.net.Stream.Reader,
        writer: Io.net.Stream.Writer,
    };

    const PooledConnection = struct {
        tls_state: *TlsState,
        tls_conn: tls.Connection,
    };

    pub fn init(allocator: Allocator, io: Io, cfg: *const config_mod.Config) !TlsTransport {
        const uri = try Uri.parse(cfg.server);
        var host_buf: [Io.net.HostName.max_len]u8 = undefined;
        const host_name = try uri.getHost(&host_buf);
        const host_str = host_name.bytes;
        const port = uri.port orelse 443;

        var root_ca: ?tls.config.cert.Bundle = null;
        errdefer if (root_ca) |*ca| ca.deinit(allocator);

        if (cfg.ca_data) |pem| {
            root_ca = try tls.config.cert.fromSlice(allocator, io, pem);
        } else if (cfg.ca_file) |path| {
            root_ca = try tls.config.cert.fromFilePathAbsolute(allocator, io, path);
        } else if (!cfg.insecure) {
            root_ca = try tls.config.cert.fromSystem(allocator, io);
        }

        var auth: ?tls.config.CertKeyPair = null;
        errdefer if (auth) |*a| a.deinit(allocator);

        if (cfg.client_cert_data) |cert_pem| {
            const key_pem_raw = cfg.client_key_data orelse return error.MissingClientKey;
            // Normalize key PEM: tls.zig only accepts PKCS#8 and SEC1 EC, but
            // kubeadm/kind/older kubectl ship PKCS#1 RSA keys. This wraps them
            // in PKCS#8 without touching key material.
            const key_pem = try pem_mod.normalizePrivateKey(allocator, key_pem_raw);
            defer allocator.free(key_pem);
            auth = try tls.config.CertKeyPair.fromSlice(allocator, io, cert_pem, key_pem);
        }

        const tls_server_name: ?[]const u8 = if (cfg.tls_server_name) |n|
            try allocator.dupe(u8, n)
        else
            null;
        errdefer if (tls_server_name) |n| allocator.free(n);

        return .{
            .allocator = allocator,
            .io = io,
            .host = try allocator.dupe(u8, host_str),
            .port = port,
            .insecure = cfg.insecure,
            .root_ca = root_ca,
            .auth = auth,
            .tls_server_name = tls_server_name,
            .timeout = if (cfg.timeout_ms > 0)
                .{ .duration = .{ .raw = Io.Duration.fromMilliseconds(@intCast(cfg.timeout_ms)), .clock = .awake } }
            else
                .none,
        };
    }

    pub fn deinit(self: *TlsTransport) void {
        self.closeIdle();
        if (self.root_ca) |*ca| ca.deinit(self.allocator);
        if (self.auth) |*a| a.deinit(self.allocator);
        if (self.tls_server_name) |n| self.allocator.free(n);
        self.allocator.free(self.host);
    }

    /// Hot-swap client certificate+key for exec credential plugin auth.
    /// Normalizes the key PEM (PKCS#1 → PKCS#8 if needed) and rebuilds
    /// the CertKeyPair. Closes any idle connections so the next request
    /// performs a fresh TLS handshake with the new identity.
    pub fn updateClientAuth(self: *TlsTransport, cert_pem: []const u8, key_pem: []const u8) !void {
        if (self.auth) |*old| old.deinit(self.allocator);
        self.auth = null;
        self.closeIdle(); // force fresh handshake with new creds

        const pem_normalize = @import("pem.zig");
        const normalized_key = try pem_normalize.normalizePrivateKey(self.allocator, key_pem);
        defer self.allocator.free(normalized_key);
        self.auth = try tls.config.CertKeyPair.fromSlice(self.allocator, self.io, cert_pem, normalized_key);
    }

    fn closeIdle(self: *TlsTransport) void {
        if (self.idle_conn) |*conn| {
            conn.tls_conn.close() catch {};
            conn.tls_state.stream.close(self.io);
            self.allocator.destroy(conn.tls_state);
            self.idle_conn = null;
        }
    }

    /// Acquire a TLS connection — reuse idle or create new.
    fn acquire(self: *TlsTransport) !ActiveConnection {
        // Try reuse idle connection
        if (self.idle_conn) |conn| {
            self.idle_conn = null;
            return .{
                .tls_state = conn.tls_state,
                .tls_conn = conn.tls_conn,
                .transport = self,
            };
        }

        // New TCP connection
        const host_name = try Io.net.HostName.init(self.host);
        const stream = try host_name.connect(self.io, self.port, .{ .mode = .stream, .timeout = self.timeout });

        // Allocate TLS state on heap so pointers survive past this function
        const state = try self.allocator.create(TlsState);
        errdefer self.allocator.destroy(state);
        state.stream = stream;
        state.input_buf = undefined;
        state.output_buf = undefined;
        // Use state.stream (heap) not local stream (stack) so reader/writer
        // pointers remain valid after this function returns.
        state.reader = state.stream.reader(self.io, &state.input_buf);
        state.writer = state.stream.writer(self.io, &state.output_buf);

        // TLS handshake. Use tls_server_name override (kubeconfig
        // `tls-server-name`) for SNI + cert verification when set, so users
        // connecting to an IP endpoint with a DNS-named cert can verify
        // against the DNS name without disabling TLS.
        const rng_impl: std.Random.IoSource = .{ .io = self.io };
        const verify_host = self.tls_server_name orelse self.host;
        const tls_conn = try tls.client(
            &state.reader.interface,
            &state.writer.interface,
            .{
                .rng = rng_impl.interface(),
                .now = Io.Clock.real.now(self.io),
                .host = verify_host,
                .root_ca = self.root_ca orelse .empty,
                .insecure_skip_verify = self.insecure,
                .auth = if (self.auth) |*a| a else null,
            },
        );

        return .{
            .tls_state = state,
            .tls_conn = tls_conn,
            .transport = self,
        };
    }

    /// Perform an HTTP request and return the response.
    pub fn request(
        self: *TlsTransport,
        method: http.Method,
        url: []const u8,
        body: ?[]const u8,
        content_type: ?[]const u8,
        auth_header: ?[]const u8,
    ) !Response {
        var conn = try self.acquire();
        errdefer conn.destroy();

        const uri = try Uri.parse(url);
        var target_buf: [4096]u8 = undefined;
        const target = try buildRequestTarget(uri, &target_buf);

        // Write request
        try conn.writeRequest(method, target, if (uri.host) |h| componentToStr(h) else self.host, auth_header, content_type, body);

        // Read response head
        const head = try conn.readResponseHead(self.allocator);

        // Read body
        var response_body: std.ArrayList(u8) = .empty;
        if (head.content_length) |cl| {
            if (cl > 0) {
                try response_body.ensureTotalCapacity(self.allocator, cl);
                var buf: [4096]u8 = undefined;
                var remaining = cl;
                while (remaining > 0) {
                    const to_read = @min(remaining, buf.len);
                    const n = conn.tls_conn.read(buf[0..to_read]) catch break;
                    if (n == 0) break;
                    try response_body.appendSlice(self.allocator, buf[0..n]);
                    remaining -= n;
                }
            }
        } else if (head.chunked) {
            // Chunked transfer encoding: read chunk-size\r\n, chunk-data\r\n, ..., 0\r\n\r\n
            try readChunkedBody(&conn.tls_conn, self.allocator, &response_body);
        } else {
            // Read until EOF (connection close)
            var buf: [4096]u8 = undefined;
            while (true) {
                const n = conn.tls_conn.read(&buf) catch break;
                if (n == 0) break;
                try response_body.appendSlice(self.allocator, buf[0..n]);
            }
        }

        // Return connection to pool if keep-alive
        conn.release();

        return .{
            .status = head.status,
            .body = try response_body.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Open a streaming request (for watch). Caller reads from the returned StreamResponse.
    pub fn streamRequest(
        self: *TlsTransport,
        method: http.Method,
        url: []const u8,
        auth_header: ?[]const u8,
    ) !StreamResponse {
        var conn = try self.acquire();
        errdefer conn.destroy();

        const uri = try Uri.parse(url);
        var target_buf: [4096]u8 = undefined;
        const target = try buildRequestTarget(uri, &target_buf);

        try conn.writeRequest(method, target, if (uri.host) |h| componentToStr(h) else self.host, auth_header, "application/json", null);

        const head = try conn.readResponseHead(self.allocator);

        return .{
            .conn = conn,
            .status = head.status,
        };
    }

    pub const Response = struct {
        status: u16,
        body: []const u8,
        allocator: Allocator,

        pub fn deinit(self: *Response) void {
            self.allocator.free(self.body);
        }
    };

    pub const StreamResponse = struct {
        conn: ActiveConnection,
        status: u16,

        /// Read a single byte from the stream. Used by watch decoder.
        pub fn readByte(self: *StreamResponse) !u8 {
            var buf: [1]u8 = undefined;
            const n = self.conn.tls_conn.read(&buf) catch return error.ReadFailed;
            if (n == 0) return error.EndOfStream;
            return buf[0];
        }

        pub fn deinit(self: *StreamResponse) void {
            self.conn.destroy();
        }
    };

    const ActiveConnection = struct {
        tls_state: *TlsState,
        tls_conn: tls.Connection,
        transport: *TlsTransport,

        fn writeRequest(
            self: *ActiveConnection,
            method: http.Method,
            path: []const u8,
            host: []const u8,
            auth_header: ?[]const u8,
            content_type: ?[]const u8,
            body: ?[]const u8,
        ) !void {
            const serialized = try serializeRequest(method, path, host, auth_header, content_type, body);
            try self.tls_conn.writeAll(serialized.buf[0..serialized.len]);
            if (body) |b| {
                try self.tls_conn.writeAll(b);
            }
        }

        fn readResponseHead(self: *ActiveConnection, allocator: Allocator) !ParsedResponseHead {
            var head_buf: std.ArrayList(u8) = .empty;
            defer head_buf.deinit(allocator);

            while (true) {
                var byte_buf: [1]u8 = undefined;
                const n = self.tls_conn.read(&byte_buf) catch return error.ReadFailed;
                if (n == 0) return error.UnexpectedEof;
                try head_buf.append(allocator, byte_buf[0]);

                if (head_buf.items.len >= 4) {
                    const tail = head_buf.items[head_buf.items.len - 4 ..];
                    if (mem.eql(u8, tail, "\r\n\r\n")) break;
                }
            }

            return parseResponseHead(head_buf.items);
        }

        /// Return connection to pool for reuse.
        fn release(self: *ActiveConnection) void {
            self.transport.closeIdle();
            self.transport.idle_conn = .{
                .tls_state = self.tls_state,
                .tls_conn = self.tls_conn,
            };
        }

        /// Close and free connection resources.
        fn destroy(self: *ActiveConnection) void {
            self.tls_conn.close() catch {};
            self.tls_state.stream.close(self.transport.io);
            self.transport.allocator.destroy(self.tls_state);
        }
    };
};

// ─────────────────────────────────────────────────────────────────────────────
// Standalone helpers (testable without a live connection)
// ─────────────────────────────────────────────────────────────────────────────

const SerializedRequest = struct { buf: [8192]u8, len: usize };

/// Serialize an HTTP/1.1 request into a buffer. Used by ActiveConnection.writeRequest
/// and exposed for testing.
fn serializeRequest(
    method: http.Method,
    path: []const u8,
    host: []const u8,
    auth_header: ?[]const u8,
    content_type: ?[]const u8,
    body: ?[]const u8,
) !SerializedRequest {
    var result: SerializedRequest = .{ .buf = undefined, .len = 0 };
    var pos: usize = 0;

    const method_str = @tagName(method);
    pos += (std.fmt.bufPrint(result.buf[pos..], "{s} {s} HTTP/1.1\r\nHost: {s}\r\nAccept: application/json\r\nConnection: keep-alive\r\n", .{ method_str, path, host }) catch return error.RequestTooLarge).len;
    if (auth_header) |auth| {
        pos += (std.fmt.bufPrint(result.buf[pos..], "Authorization: {s}\r\n", .{auth}) catch return error.RequestTooLarge).len;
    }
    if (content_type) |ct| {
        pos += (std.fmt.bufPrint(result.buf[pos..], "Content-Type: {s}\r\n", .{ct}) catch return error.RequestTooLarge).len;
    }
    if (body) |b| {
        pos += (std.fmt.bufPrint(result.buf[pos..], "Content-Length: {d}\r\n", .{b.len}) catch return error.RequestTooLarge).len;
    }
    pos += (std.fmt.bufPrint(result.buf[pos..], "\r\n", .{}) catch return error.RequestTooLarge).len;
    result.len = pos;
    return result;
}

const ParsedResponseHead = struct {
    status: u16,
    content_length: ?usize,
    chunked: bool,
};

/// Parse an HTTP/1.1 response head from raw bytes. Exposed for testing.
fn parseResponseHead(raw: []const u8) !ParsedResponseHead {
    const status_line_end = mem.indexOf(u8, raw, "\r\n") orelse return error.MalformedResponse;
    const status_line = raw[0..status_line_end];

    const space1 = mem.indexOf(u8, status_line, " ") orelse return error.MalformedResponse;
    const after_space = status_line[space1 + 1 ..];
    const space2 = mem.indexOf(u8, after_space, " ") orelse after_space.len;
    const status_str = after_space[0..space2];
    const status = std.fmt.parseInt(u16, status_str, 10) catch return error.MalformedResponse;

    var content_length: ?usize = null;
    var chunked = false;
    const headers_start = status_line_end + 2;
    if (headers_start < raw.len) {
        var it = mem.splitSequence(u8, raw[headers_start..], "\r\n");
        while (it.next()) |line| {
            if (line.len == 0) break;
            if (std.ascii.startsWithIgnoreCase(line, "content-length:")) {
                const val = mem.trim(u8, line["content-length:".len..], " ");
                content_length = std.fmt.parseInt(usize, val, 10) catch null;
            }
            if (std.ascii.startsWithIgnoreCase(line, "transfer-encoding:")) {
                const val = mem.trim(u8, line["transfer-encoding:".len..], " ");
                if (std.ascii.indexOfIgnoreCase(val, "chunked") != null) {
                    chunked = true;
                }
            }
        }
    }

    return .{ .status = status, .content_length = content_length, .chunked = chunked };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "buildRequestTarget: preserves query string" {
    const Case = struct {
        url: []const u8,
        expected: []const u8,
    };
    const cases = [_]Case{
        .{ .url = "https://api.k8s/api/v1/pods", .expected = "/api/v1/pods" },
        .{ .url = "https://api.k8s/api/v1/pods?watch=true", .expected = "/api/v1/pods?watch=true" },
        .{
            .url = "https://api.k8s/api/v1/namespaces/default/pods?watch=true&resourceVersion=12345",
            .expected = "/api/v1/namespaces/default/pods?watch=true&resourceVersion=12345",
        },
        .{
            .url = "https://api.k8s/apis/apps/v1/deployments?labelSelector=app%3Dnginx",
            .expected = "/apis/apps/v1/deployments?labelSelector=app%3Dnginx",
        },
        .{ .url = "https://api.k8s/", .expected = "/" },
        .{ .url = "https://api.k8s", .expected = "/" },
        .{ .url = "https://api.k8s?continue=abc", .expected = "/?continue=abc" },
    };
    for (cases) |c| {
        const uri = try Uri.parse(c.url);
        var buf: [256]u8 = undefined;
        const target = try buildRequestTarget(uri, &buf);
        try testing.expectEqualStrings(c.expected, target);
    }
}

test "buildRequestTarget: rejects oversized targets" {
    const uri = try Uri.parse("https://h/" ++ "x" ** 100 ++ "?q=" ++ "y" ** 100);
    var buf: [50]u8 = undefined;
    try testing.expectError(error.RequestTargetTooLong, buildRequestTarget(uri, &buf));
}

test "TlsTransport: init table-driven" {
    const Case = struct {
        server: []const u8,
        insecure: bool,
        expected_host: []const u8,
        expected_port: u16,
        expected_insecure: bool,
    };
    const cases = [_]Case{
        .{ .server = "https://127.0.0.1:6443", .insecure = true, .expected_host = "127.0.0.1", .expected_port = 6443, .expected_insecure = true },
        .{ .server = "https://api.example.com", .insecure = true, .expected_host = "api.example.com", .expected_port = 443, .expected_insecure = true },
        .{ .server = "https://10.0.0.1:8443", .insecure = true, .expected_host = "10.0.0.1", .expected_port = 8443, .expected_insecure = true },
    };
    for (cases) |c| {
        const cfg = config_mod.Config{
            .allocator = testing.allocator,
            .server = try testing.allocator.dupe(u8, c.server),
            .namespace = try testing.allocator.dupe(u8, "default"),
            .insecure = c.insecure,
        };
        defer {
            var owned = cfg;
            owned.deinit();
        }
        var t = try TlsTransport.init(testing.allocator, testing.io, &cfg);
        defer t.deinit();
        try testing.expectEqualStrings(c.expected_host, t.host);
        try testing.expectEqual(c.expected_port, t.port);
        try testing.expectEqual(c.expected_insecure, t.insecure);
        try testing.expect(t.auth == null);
    }
}

test "TlsTransport: init with insecure config" {
    // Can't actually connect in unit tests, but verify init doesn't error
    // for insecure config (no CA needed).
    const cfg = config_mod.Config{
        .allocator = testing.allocator,
        .server = try testing.allocator.dupe(u8, "https://127.0.0.1:6443"),
        .namespace = try testing.allocator.dupe(u8, "default"),
        .insecure = true,
    };
    defer {
        var owned = cfg;
        owned.deinit();
    }

    var t = try TlsTransport.init(testing.allocator, testing.io, &cfg);
    defer t.deinit();

    try testing.expectEqualStrings("127.0.0.1", t.host);
    try testing.expectEqual(@as(u16, 6443), t.port);
    try testing.expect(t.insecure);
    try testing.expect(t.root_ca == null);
    try testing.expect(t.auth == null);
    try testing.expect(t.tls_server_name == null);
}

test "TlsTransport: tls_server_name is stored from config" {
    const cfg = config_mod.Config{
        .allocator = testing.allocator,
        .server = try testing.allocator.dupe(u8, "https://10.0.0.1:6443"),
        .namespace = try testing.allocator.dupe(u8, "default"),
        .insecure = true,
        .tls_server_name = try testing.allocator.dupe(u8, "kubernetes.internal"),
    };
    defer {
        var owned = cfg;
        owned.deinit();
    }

    var t = try TlsTransport.init(testing.allocator, testing.io, &cfg);
    defer t.deinit();

    try testing.expectEqualStrings("10.0.0.1", t.host);
    try testing.expect(t.tls_server_name != null);
    try testing.expectEqualStrings("kubernetes.internal", t.tls_server_name.?);
}

test "TlsTransport: init rejects client cert without key" {
    const cfg = config_mod.Config{
        .allocator = testing.allocator,
        .server = try testing.allocator.dupe(u8, "https://127.0.0.1:6443"),
        .namespace = try testing.allocator.dupe(u8, "default"),
        .insecure = true,
        .client_cert_data = try testing.allocator.dupe(u8, "cert-pem-data"),
        // no client_key_data
    };
    defer {
        var owned = cfg;
        owned.deinit();
    }

    try testing.expectError(error.MissingClientKey, TlsTransport.init(testing.allocator, testing.io, &cfg));
}

test "HTTP/1.1 response head parsing: table-driven" {
    const Case = struct {
        raw: []const u8,
        expected_status: u16,
        expected_cl: ?usize,
    };
    const cases = [_]Case{
        .{ .raw = "HTTP/1.1 200 OK\r\nContent-Length: 42\r\n\r\n", .expected_status = 200, .expected_cl = 42 },
        .{ .raw = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n", .expected_status = 404, .expected_cl = 0 },
        .{ .raw = "HTTP/1.1 201 Created\r\ncontent-length: 100\r\n\r\n", .expected_status = 201, .expected_cl = 100 },
        .{ .raw = "HTTP/1.1 204 No Content\r\n\r\n", .expected_status = 204, .expected_cl = null },
        .{ .raw = "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\n", .expected_status = 500, .expected_cl = 5 },
        .{ .raw = "HTTP/1.0 301 Moved\r\n\r\n", .expected_status = 301, .expected_cl = null },
    };
    for (cases) |c| {
        const result = try parseResponseHead(c.raw);
        try testing.expectEqual(c.expected_status, result.status);
        try testing.expectEqual(c.expected_cl, result.content_length);
    }
}

test "HTTP/1.1 request serialization" {
    const Case = struct {
        method: http.Method,
        path: []const u8,
        host: []const u8,
        auth: ?[]const u8,
        content_type: ?[]const u8,
        body: ?[]const u8,
        expected_parts: []const []const u8,
    };
    const cases = [_]Case{
        .{
            .method = .GET,
            .path = "/api/v1/pods",
            .host = "k8s.example.com",
            .auth = "Bearer tok-123",
            .content_type = null,
            .body = null,
            .expected_parts = &.{ "GET /api/v1/pods HTTP/1.1\r\n", "Host: k8s.example.com\r\n", "Authorization: Bearer tok-123\r\n", "Accept: application/json\r\n", "Connection: keep-alive\r\n" },
        },
        .{
            .method = .POST,
            .path = "/api/v1/namespaces/default/pods",
            .host = "k8s.example.com",
            .auth = null,
            .content_type = "application/json",
            .body = "{\"kind\":\"Pod\"}",
            .expected_parts = &.{ "POST /api/v1/namespaces/default/pods HTTP/1.1\r\n", "Content-Type: application/json\r\n", "Content-Length: 14\r\n" },
        },
        .{
            .method = .DELETE,
            .path = "/api/v1/pods/nginx",
            .host = "10.0.0.1:6443",
            .auth = null,
            .content_type = null,
            .body = null,
            .expected_parts = &.{ "DELETE /api/v1/pods/nginx HTTP/1.1\r\n", "Host: 10.0.0.1:6443\r\n" },
        },
    };
    for (cases) |c| {
        const serialized = try serializeRequest(c.method, c.path, c.host, c.auth, c.content_type, c.body);
        for (c.expected_parts) |part| {
            try testing.expect(mem.indexOf(u8, &serialized.buf, part) != null);
        }
        // Verify ends with \r\n\r\n (headers terminated)
        const written = serialized.buf[0..serialized.len];
        try testing.expect(mem.endsWith(u8, written, "\r\n\r\n"));
    }
}

test "HTTP/1.1 response head parsing: edge cases" {
    const Case = struct {
        raw: []const u8,
        expected_status: u16,
        expected_cl: ?usize,
    };
    const cases = [_]Case{
        // Status with no reason phrase
        .{ .raw = "HTTP/1.1 200\r\n\r\n", .expected_status = 200, .expected_cl = null },
        // Multiple Content-Length headers (last wins)
        .{ .raw = "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nContent-Length: 42\r\n\r\n", .expected_status = 200, .expected_cl = 42 },
        // Header with extra whitespace around value
        .{ .raw = "HTTP/1.1 200 OK\r\nContent-Length:   99  \r\n\r\n", .expected_status = 200, .expected_cl = 99 },
        // Very long status line with extended reason phrase
        .{ .raw = "HTTP/1.1 503 Service Temporarily Unavailable Due To Maintenance\r\nContent-Length: 0\r\n\r\n", .expected_status = 503, .expected_cl = 0 },
        // Status 100 Continue with no body headers
        .{ .raw = "HTTP/1.1 100 Continue\r\n\r\n", .expected_status = 100, .expected_cl = null },
        // Mixed-case content-length
        .{ .raw = "HTTP/1.1 200 OK\r\nCONTENT-LENGTH: 7\r\n\r\n", .expected_status = 200, .expected_cl = 7 },
    };
    for (cases) |c| {
        const result = try parseResponseHead(c.raw);
        try testing.expectEqual(c.expected_status, result.status);
        try testing.expectEqual(c.expected_cl, result.content_length);
    }
}

test "HTTP/1.1 request serialization: no auth no body" {
    // Minimal GET request — should have no Authorization or Content-Length headers
    const serialized = try serializeRequest(.GET, "/api/v1/nodes", "k8s.local", null, null, null);
    const written = serialized.buf[0..serialized.len];

    // Must contain request line, Host, Accept, Connection
    try testing.expect(mem.indexOf(u8, written, "GET /api/v1/nodes HTTP/1.1\r\n") != null);
    try testing.expect(mem.indexOf(u8, written, "Host: k8s.local\r\n") != null);
    try testing.expect(mem.indexOf(u8, written, "Accept: application/json\r\n") != null);
    try testing.expect(mem.indexOf(u8, written, "Connection: keep-alive\r\n") != null);

    // Must NOT contain Authorization or Content-Length
    try testing.expect(mem.indexOf(u8, written, "Authorization:") == null);
    try testing.expect(mem.indexOf(u8, written, "Content-Length:") == null);
    try testing.expect(mem.indexOf(u8, written, "Content-Type:") == null);

    // Must end with header terminator
    try testing.expect(mem.endsWith(u8, written, "\r\n\r\n"));
}

test "transport: normal TLS config has no tls_transport" {
    const cfg = config_mod.Config{
        .allocator = testing.allocator,
        .server = try testing.allocator.dupe(u8, "http://127.0.0.1:8080"),
        .namespace = try testing.allocator.dupe(u8, "default"),
    };
    defer {
        var owned = cfg;
        owned.deinit();
    }
    var t = try @import("transport.zig").Transport.init(testing.allocator, testing.io, &cfg);
    defer t.deinit();
    try testing.expect(t.tls_transport == null);
}

test "fuzz: parseResponseHead never crashes on arbitrary input" {
    try std.testing.fuzz({}, fuzzParseResponseHead, .{});
}

fn fuzzParseResponseHead(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    const len = smith.valueRangeAtMost(u16, 0, 512);
    var buf: [512]u8 = undefined;
    for (buf[0..len]) |*b| {
        b.* = smith.valueRangeAtMost(u8, 0, 127);
    }
    // Must not crash
    if (parseResponseHead(buf[0..len])) |head| {
        _ = head.status;
        _ = head.content_length;
        _ = head.chunked;
    } else |_| {}
}

test "fuzz: serializeRequest never crashes" {
    try std.testing.fuzz({}, fuzzSerializeRequest, .{});
}

fn fuzzSerializeRequest(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    const path_len = smith.valueRangeAtMost(u8, 1, 64);
    var path_buf: [64]u8 = undefined;
    for (path_buf[0..path_len]) |*b| {
        b.* = smith.valueRangeAtMost(u8, 32, 126);
    }
    const host_len = smith.valueRangeAtMost(u8, 1, 32);
    var host_buf: [32]u8 = undefined;
    for (host_buf[0..host_len]) |*b| {
        b.* = smith.valueRangeAtMost(u8, 32, 126);
    }

    const method: http.Method = switch (smith.valueRangeAtMost(u8, 0, 4)) {
        0 => .GET,
        1 => .POST,
        2 => .PUT,
        3 => .DELETE,
        4 => .PATCH,
        else => unreachable,
    };

    if (serializeRequest(method, path_buf[0..path_len], host_buf[0..host_len], null, null, null)) |_| {} else |_| {}
}
