const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const Allocator = mem.Allocator;
const kubeconfig = @import("kubeconfig.zig");
const base64 = std.base64.standard.decoderWithIgnore(" \t\r\n");

/// Resolved configuration for connecting to a Kubernetes cluster.
/// Flattened from kubeconfig's cluster + user + context into a single struct.
pub const Config = struct {
    allocator: Allocator,
    /// API server URL (e.g., "https://kubernetes.default.svc:6443")
    server: []const u8,
    /// Default namespace for requests
    namespace: []const u8,
    /// Bearer token for authentication
    token: ?[]const u8 = null,
    /// Path to a file containing a bearer token (refreshed on each read)
    token_file: ?[]const u8 = null,
    /// PEM-encoded CA certificate data (base64-decoded)
    ca_data: ?[]const u8 = null,
    /// Path to a CA certificate file
    ca_file: ?[]const u8 = null,
    /// Override hostname used for TLS verification / SNI
    tls_server_name: ?[]const u8 = null,
    /// PEM-encoded client certificate data
    client_cert_data: ?[]const u8 = null,
    /// PEM-encoded client key data
    client_key_data: ?[]const u8 = null,
    /// Basic auth username
    username: ?[]const u8 = null,
    /// Basic auth password
    password: ?[]const u8 = null,
    /// Skip TLS verification
    insecure: bool = false,
    /// Exec-based credential plugin
    exec: ?kubeconfig.ExecConfig = null,
    /// Request timeout in milliseconds (0 = no timeout).
    timeout_ms: u64 = 0,

    pub fn deinit(self: *Config) void {
        self.allocator.free(self.server);
        self.allocator.free(self.namespace);
        if (self.token) |v| self.allocator.free(v);
        if (self.token_file) |v| self.allocator.free(v);
        if (self.ca_data) |v| self.allocator.free(v);
        if (self.ca_file) |v| self.allocator.free(v);
        if (self.tls_server_name) |v| self.allocator.free(v);
        if (self.client_cert_data) |v| self.allocator.free(v);
        if (self.client_key_data) |v| self.allocator.free(v);
        if (self.username) |v| self.allocator.free(v);
        if (self.password) |v| self.allocator.free(v);
        if (self.exec) |*e| e.deinit(self.allocator);
    }

    /// Build config from a parsed kubeconfig using the current context.
    pub fn fromKubeconfig(allocator: Allocator, kc: *const kubeconfig.Kubeconfig) !Config {
        return fromKubeconfigContext(allocator, kc, null);
    }

    /// Build config from a parsed kubeconfig using a specific context name.
    pub fn fromKubeconfigContext(allocator: Allocator, kc: *const kubeconfig.Kubeconfig, context_name: ?[]const u8) !Config {
        const ctx = if (context_name) |name|
            kc.getContext(name) orelse return error.ContextNotFound
        else
            kc.getCurrentContext() orelse return error.NoCurrentContext;

        const cluster = kc.getCluster(ctx.cluster) orelse return error.ClusterNotFound;
        const user = kc.getUser(ctx.user) orelse return error.UserNotFound;

        return .{
            .allocator = allocator,
            .server = try allocator.dupe(u8, cluster.server),
            .namespace = try allocator.dupe(u8, ctx.namespace orelse "default"),
            .token = if (user.token) |t| try allocator.dupe(u8, t) else null,
            .token_file = if (user.token_file) |t| try allocator.dupe(u8, t) else null,
            .ca_data = try decodeBase64Opt(allocator, cluster.certificate_authority_data),
            .ca_file = if (cluster.certificate_authority) |f| try allocator.dupe(u8, f) else null,
            .tls_server_name = if (cluster.tls_server_name) |name| try allocator.dupe(u8, name) else null,
            .client_cert_data = try decodeBase64Opt(allocator, user.client_certificate_data),
            .client_key_data = try decodeBase64Opt(allocator, user.client_key_data),
            .username = if (user.username) |name| try allocator.dupe(u8, name) else null,
            .password = if (user.password) |pass| try allocator.dupe(u8, pass) else null,
            .insecure = cluster.insecure_skip_tls_verify,
            .exec = if (user.exec) |exec_cfg| try cloneExecConfig(allocator, exec_cfg) else null,
        };
    }

    /// Build in-cluster config (for pods running inside K8s).
    /// Reads service account token and CA from standard mount paths.
    pub fn inCluster(allocator: Allocator) !Config {
        return .{
            .allocator = allocator,
            .server = try allocator.dupe(u8, "https://kubernetes.default.svc"),
            .namespace = try readFileAlloc(allocator, "/var/run/secrets/kubernetes.io/serviceaccount/namespace", 256) orelse try allocator.dupe(u8, "default"),
            .token_file = try allocator.dupe(u8, "/var/run/secrets/kubernetes.io/serviceaccount/token"),
            .ca_file = try allocator.dupe(u8, "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"),
        };
    }

    /// Returns true if this config uses TLS (server starts with https://).
    pub fn isTLS(self: *const Config) bool {
        return mem.startsWith(u8, self.server, "https://");
    }
};

fn readFileAlloc(allocator: Allocator, path: []const u8, max_size: usize) !?[]const u8 {
    const bytes = std.fs.cwd().readFileAlloc(allocator, path, max_size) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    errdefer allocator.free(bytes);

    const trimmed = mem.trim(u8, bytes, " \t\r\n");
    if (trimmed.len == bytes.len) return bytes;

    const out = try allocator.dupe(u8, trimmed);
    allocator.free(bytes);
    return out;
}

fn decodeBase64Opt(allocator: Allocator, value: ?[]const u8) !?[]const u8 {
    const input = value orelse return null;
    if (input.len == 0) return try allocator.alloc(u8, 0);

    const max_len = input.len / 4 * 3 + 3;
    const buffer = try allocator.alloc(u8, max_len);
    errdefer allocator.free(buffer);

    const decoded_len = try base64.decode(buffer, input);
    return try allocator.realloc(buffer, decoded_len);
}

fn cloneExecConfig(allocator: Allocator, src: kubeconfig.ExecConfig) !kubeconfig.ExecConfig {
    var args_copy: ?[]const []const u8 = null;
    if (src.args) |args| {
        const out = try allocator.alloc([]const u8, args.len);
        errdefer {
            for (out[0..]) |arg| allocator.free(arg);
            allocator.free(out);
        }
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

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "fromKubeconfig: resolves current context" {
    var kc = try kubeconfig.fromJson(testing.allocator,
        \\{
        \\  "current-context": "dev",
        \\  "clusters": [{"name": "c1", "cluster": {"server": "https://10.0.0.1:6443", "certificate-authority-data": "Y2EtZGF0YQ==", "tls-server-name": "api.internal"}}],
        \\  "users": [{"name": "u1", "user": {"token": "tok-123"}}],
        \\  "contexts": [{"name": "dev", "context": {"cluster": "c1", "user": "u1", "namespace": "dev-ns"}}]
        \\}
    );
    defer kc.deinit();

    var cfg = try Config.fromKubeconfig(testing.allocator, &kc);
    defer cfg.deinit();

    try testing.expectEqualStrings("https://10.0.0.1:6443", cfg.server);
    try testing.expectEqualStrings("dev-ns", cfg.namespace);
    try testing.expectEqualStrings("tok-123", cfg.token.?);
    try testing.expectEqualStrings("ca-data", cfg.ca_data.?);
    try testing.expectEqualStrings("api.internal", cfg.tls_server_name.?);
    try testing.expect(cfg.isTLS());
}

test "fromKubeconfig: default namespace when absent" {
    var kc = try kubeconfig.fromJson(testing.allocator,
        \\{
        \\  "current-context": "ctx",
        \\  "clusters": [{"name": "c", "cluster": {"server": "https://host"}}],
        \\  "users": [{"name": "u", "user": {}}],
        \\  "contexts": [{"name": "ctx", "context": {"cluster": "c", "user": "u"}}]
        \\}
    );
    defer kc.deinit();

    var cfg = try Config.fromKubeconfig(testing.allocator, &kc);
    defer cfg.deinit();

    try testing.expectEqualStrings("default", cfg.namespace);
}

test "fromKubeconfig: specific context" {
    var kc = try kubeconfig.fromJson(testing.allocator,
        \\{
        \\  "current-context": "a",
        \\  "clusters": [{"name": "c1", "cluster": {"server": "https://a"}}, {"name": "c2", "cluster": {"server": "https://b"}}],
        \\  "users": [{"name": "u1", "user": {}}, {"name": "u2", "user": {"token": "b-tok"}}],
        \\  "contexts": [{"name": "a", "context": {"cluster": "c1", "user": "u1"}}, {"name": "b", "context": {"cluster": "c2", "user": "u2"}}]
        \\}
    );
    defer kc.deinit();

    var cfg = try Config.fromKubeconfigContext(testing.allocator, &kc, "b");
    defer cfg.deinit();

    try testing.expectEqualStrings("https://b", cfg.server);
    try testing.expectEqualStrings("b-tok", cfg.token.?);
}

test "fromKubeconfig: decodes auth data and basic auth" {
    var kc = try kubeconfig.fromJson(testing.allocator,
        \\{
        \\  "current-context": "ctx",
        \\  "clusters": [{"name": "c1", "cluster": {"server": "https://host", "certificate-authority-data": "Y2EtZGF0YQ=="}}],
        \\  "users": [{"name": "u1", "user": {"username": "alice", "password": "secret", "client-certificate-data": "Y2VydA==", "client-key-data": "a2V5"}}],
        \\  "contexts": [{"name": "ctx", "context": {"cluster": "c1", "user": "u1"}}]
        \\}
    );
    defer kc.deinit();

    var cfg = try Config.fromKubeconfig(testing.allocator, &kc);
    defer cfg.deinit();

    try testing.expectEqualStrings("ca-data", cfg.ca_data.?);
    try testing.expectEqualStrings("cert", cfg.client_cert_data.?);
    try testing.expectEqualStrings("key", cfg.client_key_data.?);
    try testing.expectEqualStrings("alice", cfg.username.?);
    try testing.expectEqualStrings("secret", cfg.password.?);
}

test "fromKubeconfig: error on missing context" {
    var kc = try kubeconfig.fromJson(testing.allocator,
        \\{"clusters": [], "users": [], "contexts": []}
    );
    defer kc.deinit();

    try testing.expectError(error.NoCurrentContext, Config.fromKubeconfig(testing.allocator, &kc));
}
