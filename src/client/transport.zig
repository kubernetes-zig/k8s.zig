const std = @import("std");
const mem = std.mem;
const http = std.http;
const Io = std.Io;
const Uri = std.Uri;
const HostName = Io.net.HostName;
const Certificate = std.crypto.Certificate;
const Allocator = mem.Allocator;
const testing = std.testing;
const json = std.json;
const config_mod = @import("config.zig");
const kubeconfig_mod = @import("kubeconfig.zig");
const tls_transport_mod = @import("tls_transport.zig");
const TlsTransport = tls_transport_mod.TlsTransport;

/// HTTP transport for talking to a K8s API server.
/// Wraps std.http.Client with auth headers and TLS configuration.
pub const Transport = struct {
    allocator: Allocator,
    http_client: http.Client,
    /// Bearer token for Authorization header.
    token: ?[]const u8,
    /// Path to token file (re-read on each request for rotation).
    token_file: ?[]const u8,
    /// Basic auth username.
    username: ?[]const u8,
    /// Basic auth password.
    password: ?[]const u8,
    /// Override hostname used for TLS verification / SNI.
    tls_server_name: ?[]const u8,
    /// tls.zig transport for mTLS/insecure connections (null when using std.http.Client).
    tls_transport: ?TlsTransport,
    /// Exec-based credential plugin config.
    exec: ?kubeconfig_mod.ExecConfig,
    /// Io handle for process spawning (exec auth) and file I/O.
    io: Io,
    /// Request timeout.
    timeout: Io.Timeout,

    /// Initialize transport from a resolved Config.
    /// Dupes config strings so Transport is independent of Config lifetime.
    pub fn init(allocator: Allocator, io: Io, cfg: *const config_mod.Config) !Transport {
        var transport = Transport{
            .allocator = allocator,
            .http_client = .{
                .allocator = allocator,
                .io = io,
            },
            .token = if (cfg.token) |t| try allocator.dupe(u8, t) else null,
            .token_file = if (cfg.token_file) |t| try allocator.dupe(u8, t) else null,
            .username = if (cfg.username) |name| try allocator.dupe(u8, name) else null,
            .password = if (cfg.password) |pass| try allocator.dupe(u8, pass) else null,
            .tls_server_name = if (cfg.tls_server_name) |name| try allocator.dupe(u8, name) else null,
            .tls_transport = null,
            .exec = if (cfg.exec) |e| try cloneExecConfig(allocator, e) else null,
            .io = io,
            .timeout = if (cfg.timeout_ms > 0)
                .{ .duration = .{ .raw = Io.Duration.fromMilliseconds(@intCast(cfg.timeout_ms)), .clock = .awake } }
            else
                .none,
        };
        errdefer transport.deinit();

        try transport.validateAuthConfig();
        try transport.configureTls(io, cfg);
        return transport;
    }

    pub fn deinit(self: *Transport) void {
        if (self.token) |t| self.allocator.free(t);
        if (self.token_file) |t| self.allocator.free(t);
        if (self.username) |v| self.allocator.free(v);
        if (self.password) |v| self.allocator.free(v);
        if (self.tls_server_name) |v| self.allocator.free(v);
        if (self.tls_transport) |*t| t.deinit();
        if (self.exec) |*e| e.deinit(self.allocator);
        self.http_client.deinit();
    }

    /// Perform an HTTP request and return the response body.
    /// Caller owns the returned body slice.
    pub fn request(
        self: *Transport,
        method: http.Method,
        url: []const u8,
        body: ?[]const u8,
        content_type: ?[]const u8,
    ) !Response {
        const auth_header = try self.authHeader(self.allocator);
        defer if (auth_header) |a| self.allocator.free(a);

        // Route through tls.zig transport for mTLS/insecure
        if (self.tls_transport) |*tt| {
            const resp = try tt.request(method, url, body, content_type, auth_header);
            return .{
                .status = resp.status,
                .body = resp.body,
                .allocator = resp.allocator,
            };
        }

        var extra_headers_buf: [3]http.Header = undefined;
        var extra_count: usize = 0;

        // Accept JSON
        extra_headers_buf[extra_count] = .{ .name = "Accept", .value = "application/json" };
        extra_count += 1;

        // Auth
        if (auth_header) |auth| {
            extra_headers_buf[extra_count] = .{ .name = "Authorization", .value = auth };
            extra_count += 1;
        }

        // Content-Type
        if (content_type) |ct| {
            extra_headers_buf[extra_count] = .{ .name = "Content-Type", .value = ct };
            extra_count += 1;
        }

        // Use the low-level request API for body support
        var req = try self.openRequest(method, url, .{
            .extra_headers = extra_headers_buf[0..extra_count],
        });
        defer req.deinit();

        if (body) |b| {
            req.transfer_encoding = .{ .content_length = b.len };
            var send_body = try req.sendBodyUnflushed(&.{});
            try send_body.writer.writeAll(b);
            try send_body.end();
            try req.connection.?.flush();
        } else {
            try req.sendBodiless();
        }

        var redirect_buf: [8192]u8 = undefined;
        var response = try req.receiveHead(&redirect_buf);

        // Read response body byte-by-byte via the HTTP body reader
        // (handles chunked transfer encoding and Content-Length internally)
        var response_body: std.ArrayList(u8) = .empty;
        var reader_buf: [8192]u8 = undefined;
        const reader = response.reader(&reader_buf);
        while (true) {
            const byte = reader.takeByte() catch break;
            try response_body.append(self.allocator, byte);
        }
        const body_bytes = try response_body.toOwnedSlice(self.allocator);

        return .{
            .status = @intFromEnum(response.head.status),
            .body = body_bytes,
            .allocator = self.allocator,
            .retry_after_seconds = parseRetryAfterHeader(response.head),
        };
    }

    /// Extract Retry-After header value (seconds) from HTTP response head.
    fn parseRetryAfterHeader(head: http.Client.Response.Head) ?u32 {
        var it = head.iterateHeaders();
        while (it.next()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "retry-after")) {
                return std.fmt.parseInt(u32, mem.trim(u8, header.value, " "), 10) catch null;
            }
        }
        return null;
    }

    pub fn authHeader(self: *Transport, allocator: Allocator) !?[]const u8 {
        // Static token takes precedence
        if (self.token) |t| {
            return try std.fmt.allocPrint(allocator, "Bearer {s}", .{t});
        }

        if (self.token_file) |path| {
            const raw = try readFileAlloc(allocator, self.http_client.io, path, 1024 * 1024);
            defer allocator.free(raw);

            const trimmed = mem.trim(u8, raw, " \t\r\n");
            if (trimmed.len == 0) return null;
            return try std.fmt.allocPrint(allocator, "Bearer {s}", .{trimmed});
        }

        if (self.username) |user| {
            const pass = self.password orelse "";
            const encoded = try std.fmt.allocPrint(allocator, "{s}:{s}", .{ user, pass });
            defer allocator.free(encoded);
            return try std.fmt.allocPrint(allocator, "Basic {b64}", .{encoded});
        }

        if (self.exec) |exec_cfg| {
            return try execCredentialToken(self.allocator, self.io, exec_cfg);
        }

        return null;
    }

    /// Run an exec credential plugin and extract the bearer token from its output.
    /// The plugin must output an ExecCredential JSON to stdout with `status.token`.
    fn execCredentialToken(allocator: Allocator, io: Io, exec_cfg: kubeconfig_mod.ExecConfig) !?[]const u8 {
        // Build argv: command + args
        const args = exec_cfg.args orelse &[_][]const u8{};
        var argv_buf: std.ArrayList([]const u8) = .empty;
        defer argv_buf.deinit(allocator);
        try argv_buf.append(allocator, exec_cfg.command);
        try argv_buf.appendSlice(allocator, args);

        var child = try std.process.spawn(io, .{
            .argv = argv_buf.items,
            .stdout = .pipe,
            .stderr = .ignore,
        });

        // Read stdout
        var stdout_buf: [65536]u8 = undefined;
        var stdout_len: usize = 0;
        if (child.stdout) |stdout_file| {
            var read_buf: [4096]u8 = undefined;
            var reader = stdout_file.reader(io, &read_buf);
            while (true) {
                const byte = reader.interface.takeByte() catch break;
                if (stdout_len >= stdout_buf.len) break;
                stdout_buf[stdout_len] = byte;
                stdout_len += 1;
            }
        }

        const term = try child.wait(io);
        switch (term) {
            .exited => |code| if (code != 0) return error.ExecPluginFailed,
            else => return error.ExecPluginFailed,
        }

        if (stdout_len == 0) return error.ExecPluginNoOutput;

        // Parse ExecCredential JSON: {"status":{"token":"..."}}
        const parsed = json.parseFromSlice(json.Value, allocator, stdout_buf[0..stdout_len], .{}) catch
            return error.ExecPluginInvalidOutput;
        defer parsed.deinit();

        if (parsed.value != .object) return error.ExecPluginInvalidOutput;
        const status = parsed.value.object.get("status") orelse return error.ExecPluginInvalidOutput;
        if (status != .object) return error.ExecPluginInvalidOutput;
        const token_val = status.object.get("token") orelse return error.ExecPluginInvalidOutput;
        if (token_val != .string) return error.ExecPluginInvalidOutput;

        return try std.fmt.allocPrint(allocator, "Bearer {s}", .{token_val.string});
    }

    pub fn openRequest(
        self: *Transport,
        method: http.Method,
        url: []const u8,
        options: http.Client.RequestOptions,
    ) !http.Client.Request {
        const uri = try Uri.parse(url);
        const protocol = http.Client.Protocol.fromUri(uri) orelse return error.UnsupportedUriScheme;
        if (protocol == .tls) {
            try self.ensureTlsReady();
        }

        var req_options = options;
        if (protocol == .tls and self.tls_server_name != null and req_options.connection == null) {
            req_options.connection = try self.connectTlsWithServerName(uri);
        }
        return try self.http_client.request(method, uri, req_options);
    }

    fn configureTls(self: *Transport, io: Io, cfg: *const config_mod.Config) !void {
        if (!cfg.isTLS()) return;

        // Use tls.zig for mTLS or insecure connections (std TLS doesn't support these)
        const needs_tls_zig = cfg.insecure or cfg.client_cert_data != null;
        if (needs_tls_zig) {
            self.tls_transport = try TlsTransport.init(self.allocator, io, cfg);
            return;
        }

        // Normal TLS path via std.http.Client
        if (http.Client.disable_tls) return error.TlsUnsupported;

        if (cfg.ca_data == null and cfg.ca_file == null) return;

        const now = Io.Clock.real.now(self.http_client.io);
        if (cfg.ca_data) |pem| {
            try addCertsFromPem(&self.http_client.ca_bundle, self.allocator, pem, now.toSeconds());
        } else if (cfg.ca_file) |path| {
            try addCertsFromFilePath(&self.http_client.ca_bundle, self.allocator, self.http_client.io, now, path);
        }
        self.http_client.now = now;
    }

    fn validateAuthConfig(self: *Transport) !void {
        const has_token = self.token != null or self.token_file != null;
        const has_basic = self.username != null or self.password != null;
        if (has_token and has_basic) return error.AuthConflict;
        if (self.username == null and self.password != null) return error.BasicAuthPasswordWithoutUsername;
    }

    fn ensureTlsReady(self: *Transport) !void {
        {
            self.http_client.ca_bundle_lock.lockSharedUncancelable(self.http_client.io);
            defer self.http_client.ca_bundle_lock.unlockShared(self.http_client.io);
            if (self.http_client.now != null) return;
        }

        var bundle: Certificate.Bundle = .empty;
        defer bundle.deinit(self.allocator);
        const now = Io.Clock.real.now(self.http_client.io);
        bundle.rescan(self.allocator, self.http_client.io, now) catch |err| switch (err) {
            error.Canceled => |e| return e,
            else => return error.CertificateBundleLoadFailure,
        };

        self.http_client.ca_bundle_lock.lockUncancelable(self.http_client.io);
        defer self.http_client.ca_bundle_lock.unlock(self.http_client.io);
        if (self.http_client.now != null) return;
        self.http_client.now = now;
        mem.swap(Certificate.Bundle, &self.http_client.ca_bundle, &bundle);
    }

    fn connectTlsWithServerName(self: *Transport, uri: Uri) !*http.Client.Connection {
        var host_buf: [HostName.max_len]u8 = undefined;
        const actual_host = try uri.getHost(&host_buf);

        const override_name = self.tls_server_name orelse unreachable;
        const override_host = try HostName.init(override_name);

        return try self.http_client.connectTcpOptions(.{
            .host = actual_host,
            .port = uri.port orelse 443,
            .protocol = .tls,
            .proxied_host = override_host,
        });
    }
};

pub const Response = struct {
    status: u16,
    body: []const u8,
    allocator: Allocator,
    /// Retry-After header value in seconds (from 429/503 responses).
    retry_after_seconds: ?u32 = null,

    pub fn deinit(self: *Response) void {
        self.allocator.free(self.body);
    }
};

fn cloneExecConfig(allocator: Allocator, src: kubeconfig_mod.ExecConfig) !kubeconfig_mod.ExecConfig {
    var args_copy: ?[]const []const u8 = null;
    if (src.args) |args| {
        const out = try allocator.alloc([]const u8, args.len);
        for (args, 0..) |arg, i| {
            out[i] = try allocator.dupe(u8, arg);
        }
        args_copy = out;
    }
    return .{
        .api_version = if (src.api_version) |v| try allocator.dupe(u8, v) else null,
        .command = try allocator.dupe(u8, src.command),
        .args = args_copy,
    };
}

fn readFileAlloc(allocator: Allocator, io: Io, path: []const u8, max_size: usize) ![]u8 {
    var file = if (Io.Dir.path.isAbsolute(path))
        try Io.Dir.openFileAbsolute(io, path, .{})
    else
        try Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    var reader_buf: [4096]u8 = undefined;
    var reader = file.reader(io, &reader_buf);
    return reader.interface.allocRemaining(allocator, .limited(max_size)) catch |err| switch (err) {
        error.ReadFailed => return reader.err.?,
        else => return err,
    };
}

fn addCertsFromFilePath(
    bundle: *Certificate.Bundle,
    allocator: Allocator,
    io: Io,
    now: Io.Timestamp,
    path: []const u8,
) !void {
    if (Io.Dir.path.isAbsolute(path)) {
        try bundle.addCertsFromFilePathAbsolute(allocator, io, now, path);
    } else {
        try bundle.addCertsFromFilePath(allocator, io, now, Io.Dir.cwd(), path);
    }
}

fn addCertsFromPem(
    bundle: *Certificate.Bundle,
    allocator: Allocator,
    pem: []const u8,
    now_sec: i64,
) !void {
    const begin_marker = "-----BEGIN CERTIFICATE-----";
    const end_marker = "-----END CERTIFICATE-----";
    const base64 = std.base64.standard.decoderWithIgnore(" \t\r\n");

    var cursor: usize = 0;
    var added_any = false;
    while (mem.findPos(u8, pem, cursor, begin_marker)) |begin_marker_start| {
        const cert_start = begin_marker_start + begin_marker.len;
        const cert_end = mem.findPos(u8, pem, cert_start, end_marker) orelse
            return error.MissingEndCertificateMarker;
        cursor = cert_end + end_marker.len;

        const encoded_cert = mem.trim(u8, pem[cert_start..cert_end], " \t\r\n");
        if (encoded_cert.len == 0) continue;

        const decoded_start: u32 = @intCast(bundle.bytes.items.len);
        const decoded_len_upper = encoded_cert.len / 4 * 3;
        try bundle.bytes.ensureUnusedCapacity(allocator, decoded_len_upper);
        const dest = bundle.bytes.allocatedSlice()[decoded_start..];
        bundle.bytes.items.len += try base64.decode(dest, encoded_cert);
        try bundle.parseCert(allocator, decoded_start, now_sec);
        added_any = true;
    }

    if (!added_any) return error.NoCertificatesFound;
}

test "transport: token file is re-read and trimmed" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    // Write token file
    var file = try tmp.dir.createFile(testing.io, "token", .{});
    try file.writeStreamingAll(testing.io, "  abc123 \n");
    file.close(testing.io);

    // Build path: .zig-cache/tmp/<subpath>/token
    const token_path = try std.fmt.allocPrint(testing.allocator, ".zig-cache/tmp/{s}/token", .{tmp.sub_path});
    defer testing.allocator.free(token_path);

    const cfg = config_mod.Config{
        .allocator = testing.allocator,
        .server = try testing.allocator.dupe(u8, "http://127.0.0.1"),
        .namespace = try testing.allocator.dupe(u8, "default"),
        .token_file = try testing.allocator.dupe(u8, token_path),
    };
    defer {
        var owned = cfg;
        owned.deinit();
    }

    var transport = try Transport.init(testing.allocator, testing.io, &cfg);
    defer transport.deinit();

    const auth = try transport.authHeader(testing.allocator);
    defer testing.allocator.free(auth.?);
    try testing.expectEqualStrings("Bearer abc123", auth.?);
}

test "transport: basic auth header" {
    const cfg = config_mod.Config{
        .allocator = testing.allocator,
        .server = try testing.allocator.dupe(u8, "http://127.0.0.1"),
        .namespace = try testing.allocator.dupe(u8, "default"),
        .username = try testing.allocator.dupe(u8, "alice"),
        .password = try testing.allocator.dupe(u8, "secret"),
    };
    defer {
        var owned = cfg;
        owned.deinit();
    }

    var transport = try Transport.init(testing.allocator, testing.io, &cfg);
    defer transport.deinit();

    const auth = try transport.authHeader(testing.allocator);
    defer testing.allocator.free(auth.?);
    try testing.expectEqualStrings("Basic YWxpY2U6c2VjcmV0", auth.?);
}

test "transport: rejects conflicting auth" {
    var conflict_cfg = config_mod.Config{
        .allocator = testing.allocator,
        .server = try testing.allocator.dupe(u8, "http://127.0.0.1"),
        .namespace = try testing.allocator.dupe(u8, "default"),
        .token = try testing.allocator.dupe(u8, "abc"),
        .username = try testing.allocator.dupe(u8, "alice"),
    };
    defer conflict_cfg.deinit();
    try testing.expectError(error.AuthConflict, Transport.init(testing.allocator, testing.io, &conflict_cfg));
}

test "transport: insecure TLS uses tls.zig transport" {
    var insecure_cfg = config_mod.Config{
        .allocator = testing.allocator,
        .server = try testing.allocator.dupe(u8, "https://127.0.0.1:6443"),
        .namespace = try testing.allocator.dupe(u8, "default"),
        .insecure = true,
    };
    defer insecure_cfg.deinit();

    var t = try Transport.init(testing.allocator, testing.io, &insecure_cfg);
    defer t.deinit();
    try testing.expect(t.tls_transport != null);
    try testing.expect(t.tls_transport.?.insecure);
}

fn makeExecConfig(allocator: Allocator, shell_cmd: []const u8) !config_mod.Config {
    const args = try allocator.alloc([]const u8, 2);
    args[0] = try allocator.dupe(u8, "-c");
    args[1] = try allocator.dupe(u8, shell_cmd);
    return .{
        .allocator = allocator,
        .server = try allocator.dupe(u8, "http://127.0.0.1"),
        .namespace = try allocator.dupe(u8, "default"),
        .exec = .{
            .command = try allocator.dupe(u8, "/bin/sh"),
            .args = args,
        },
    };
}

test "transport: exec auth runs command and extracts token" {
    var cfg = try makeExecConfig(testing.allocator, "echo '{\"apiVersion\":\"client.authentication.k8s.io/v1beta1\",\"status\":{\"token\":\"test-tok-123\"}}'");
    defer cfg.deinit();

    var t = try Transport.init(testing.allocator, testing.io, &cfg);
    defer t.deinit();

    const auth = try t.authHeader(testing.allocator);
    defer if (auth) |a| testing.allocator.free(a);
    try testing.expect(auth != null);
    try testing.expectEqualStrings("Bearer test-tok-123", auth.?);
}

test "transport: exec auth fails on non-zero exit" {
    var cfg = try makeExecConfig(testing.allocator, "exit 1");
    defer cfg.deinit();

    var t = try Transport.init(testing.allocator, testing.io, &cfg);
    defer t.deinit();

    try testing.expectError(error.ExecPluginFailed, t.authHeader(testing.allocator));
}

test "transport: exec auth fails on invalid JSON output" {
    var cfg = try makeExecConfig(testing.allocator, "echo 'not json'");
    defer cfg.deinit();

    var t = try Transport.init(testing.allocator, testing.io, &cfg);
    defer t.deinit();

    try testing.expectError(error.ExecPluginInvalidOutput, t.authHeader(testing.allocator));
}

test "transport: exec auth fails on missing status.token" {
    var cfg = try makeExecConfig(testing.allocator, "echo '{\"status\":{}}'");
    defer cfg.deinit();

    var t = try Transport.init(testing.allocator, testing.io, &cfg);
    defer t.deinit();

    try testing.expectError(error.ExecPluginInvalidOutput, t.authHeader(testing.allocator));
}

test "transport: client cert without key is rejected" {
    var mtls_cfg = config_mod.Config{
        .allocator = testing.allocator,
        .server = try testing.allocator.dupe(u8, "https://127.0.0.1"),
        .namespace = try testing.allocator.dupe(u8, "default"),
        .client_cert_data = try testing.allocator.dupe(u8, "cert"),
    };
    defer mtls_cfg.deinit();
    try testing.expectError(error.MissingClientKey, Transport.init(testing.allocator, testing.io, &mtls_cfg));
}

test "transport: timeout is stored from config" {
    const Case = struct { timeout_ms: u64, expect_none: bool };
    const cases = [_]Case{
        .{ .timeout_ms = 0, .expect_none = true },
        .{ .timeout_ms = 5000, .expect_none = false },
        .{ .timeout_ms = 30000, .expect_none = false },
    };
    for (cases) |c| {
        var cfg = config_mod.Config{
            .allocator = testing.allocator,
            .server = try testing.allocator.dupe(u8, "http://127.0.0.1"),
            .namespace = try testing.allocator.dupe(u8, "default"),
            .timeout_ms = c.timeout_ms,
        };
        defer cfg.deinit();
        var t = try Transport.init(testing.allocator, testing.io, &cfg);
        defer t.deinit();
        if (c.expect_none) {
            try testing.expect(t.timeout == .none);
        } else {
            try testing.expect(t.timeout != .none);
        }
    }
}

test "Response: retry_after_seconds defaults to null" {
    var resp = Response{
        .status = 200,
        .body = try testing.allocator.dupe(u8, ""),
        .allocator = testing.allocator,
    };
    defer resp.deinit();
    try testing.expect(resp.retry_after_seconds == null);
}
