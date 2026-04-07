const std = @import("std");
const mem = std.mem;
const json = std.json;
const testing = std.testing;
const Allocator = mem.Allocator;
const Yaml = @import("yaml").Yaml;

/// Kubeconfig represents a complete Kubernetes configuration file.
/// Supports both JSON and YAML formats.
pub const Kubeconfig = struct {
    clusters: []Cluster,
    users: []User,
    contexts: []Context,
    current_context: ?[]const u8,
    allocator: Allocator,

    pub fn deinit(self: *Kubeconfig) void {
        for (self.clusters) |*c| c.deinit(self.allocator);
        self.allocator.free(self.clusters);
        for (self.users) |*u| u.deinit(self.allocator);
        self.allocator.free(self.users);
        for (self.contexts) |*c| c.deinit(self.allocator);
        self.allocator.free(self.contexts);
        if (self.current_context) |cc| self.allocator.free(cc);
    }

    pub fn getCurrentContext(self: *const Kubeconfig) ?*const Context {
        const name = self.current_context orelse return null;
        for (self.contexts) |*ctx| {
            if (mem.eql(u8, ctx.name, name)) return ctx;
        }
        return null;
    }

    pub fn getCluster(self: *const Kubeconfig, name: []const u8) ?*const Cluster {
        for (self.clusters) |*c| {
            if (mem.eql(u8, c.name, name)) return c;
        }
        return null;
    }

    pub fn getUser(self: *const Kubeconfig, name: []const u8) ?*const User {
        for (self.users) |*u| {
            if (mem.eql(u8, u.name, name)) return u;
        }
        return null;
    }

    pub fn getContext(self: *const Kubeconfig, name: []const u8) ?*const Context {
        for (self.contexts) |*ctx| {
            if (mem.eql(u8, ctx.name, name)) return ctx;
        }
        return null;
    }
};

pub const Cluster = struct {
    name: []const u8,
    server: []const u8,
    certificate_authority: ?[]const u8 = null,
    certificate_authority_data: ?[]const u8 = null,
    tls_server_name: ?[]const u8 = null,
    insecure_skip_tls_verify: bool = false,

    fn deinit(self: *Cluster, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.server);
        if (self.certificate_authority) |v| allocator.free(v);
        if (self.certificate_authority_data) |v| allocator.free(v);
        if (self.tls_server_name) |v| allocator.free(v);
    }
};

pub const User = struct {
    name: []const u8,
    token: ?[]const u8 = null,
    token_file: ?[]const u8 = null,
    client_certificate: ?[]const u8 = null,
    client_certificate_data: ?[]const u8 = null,
    client_key: ?[]const u8 = null,
    client_key_data: ?[]const u8 = null,
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,
    exec: ?ExecConfig = null,

    fn deinit(self: *User, allocator: Allocator) void {
        allocator.free(self.name);
        if (self.token) |v| allocator.free(v);
        if (self.token_file) |v| allocator.free(v);
        if (self.client_certificate) |v| allocator.free(v);
        if (self.client_certificate_data) |v| allocator.free(v);
        if (self.client_key) |v| allocator.free(v);
        if (self.client_key_data) |v| allocator.free(v);
        if (self.username) |v| allocator.free(v);
        if (self.password) |v| allocator.free(v);
        if (self.exec) |*e| e.deinit(allocator);
    }
};

pub const ExecConfig = struct {
    api_version: ?[]const u8 = null,
    command: []const u8,
    args: ?[]const []const u8 = null,

    pub fn deinit(self: *ExecConfig, allocator: Allocator) void {
        if (self.api_version) |v| allocator.free(v);
        allocator.free(self.command);
        if (self.args) |args| {
            for (args) |a| allocator.free(a);
            allocator.free(args);
        }
    }
};

pub const Context = struct {
    name: []const u8,
    cluster: []const u8,
    user: []const u8,
    namespace: ?[]const u8 = null,

    fn deinit(self: *Context, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.cluster);
        allocator.free(self.user);
        if (self.namespace) |v| allocator.free(v);
    }
};

// ── Parsing ───────────────────────────────────────────────────────────────

/// Parse kubeconfig from JSON bytes.
pub fn fromJson(allocator: Allocator, bytes: []const u8) !Kubeconfig {
    const parsed = try json.parseFromSlice(json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    return try fromValue(allocator, parsed.value);
}

/// Parse kubeconfig from YAML bytes.
pub fn fromYaml(allocator: Allocator, bytes: []const u8) !Kubeconfig {
    var yaml: Yaml = .{ .source = bytes };
    defer yaml.deinit(allocator);
    try yaml.load(allocator);

    if (yaml.docs.items.len == 0) return error.InvalidKubeconfig;

    return try fromYamlValue(allocator, yaml.docs.items[0]);
}

fn fromValue(allocator: Allocator, root: json.Value) !Kubeconfig {
    if (root != .object) return error.InvalidKubeconfig;
    const obj = root.object;

    var clusters: std.ArrayList(Cluster) = .empty;
    errdefer {
        for (clusters.items) |*c| c.deinit(allocator);
        clusters.deinit(allocator);
    }
    if (obj.get("clusters")) |v| {
        if (v == .array) for (v.array.items) |item| {
            if (item != .object) continue;
            const o = item.object;
            const cluster_data = (o.get("cluster") orelse continue);
            if (cluster_data != .object) continue;
            const cd = cluster_data.object;
            try clusters.append(allocator, .{
                .name = try dupeStr(allocator, o.get("name")),
                .server = try dupeStr(allocator, cd.get("server")),
                .certificate_authority = try dupeStrOpt(allocator, cd.get("certificate-authority")),
                .certificate_authority_data = try dupeStrOpt(allocator, cd.get("certificate-authority-data")),
                .tls_server_name = try dupeStrOpt(allocator, cd.get("tls-server-name")),
                .insecure_skip_tls_verify = if (cd.get("insecure-skip-tls-verify")) |v2| v2 == .bool and v2.bool else false,
            });
        };
    }

    var users: std.ArrayList(User) = .empty;
    errdefer {
        for (users.items) |*u| u.deinit(allocator);
        users.deinit(allocator);
    }
    if (obj.get("users")) |v| {
        if (v == .array) for (v.array.items) |item| {
            if (item != .object) continue;
            const o = item.object;
            const user_data = (o.get("user") orelse continue);
            if (user_data != .object) continue;
            const ud = user_data.object;
            try users.append(allocator, .{
                .name = try dupeStr(allocator, o.get("name")),
                .token = try dupeStrOpt(allocator, ud.get("token")),
                .token_file = try dupeStrOpt(allocator, ud.get("tokenFile")),
                .client_certificate = try dupeStrOpt(allocator, ud.get("client-certificate")),
                .client_certificate_data = try dupeStrOpt(allocator, ud.get("client-certificate-data")),
                .client_key = try dupeStrOpt(allocator, ud.get("client-key")),
                .client_key_data = try dupeStrOpt(allocator, ud.get("client-key-data")),
                .username = try dupeStrOpt(allocator, ud.get("username")),
                .password = try dupeStrOpt(allocator, ud.get("password")),
                .exec = try parseExecConfig(allocator, ud.get("exec")),
            });
        };
    }

    var contexts: std.ArrayList(Context) = .empty;
    errdefer {
        for (contexts.items) |*c| c.deinit(allocator);
        contexts.deinit(allocator);
    }
    if (obj.get("contexts")) |v| {
        if (v == .array) for (v.array.items) |item| {
            if (item != .object) continue;
            const o = item.object;
            const ctx_data = (o.get("context") orelse continue);
            if (ctx_data != .object) continue;
            const cd = ctx_data.object;
            try contexts.append(allocator, .{
                .name = try dupeStr(allocator, o.get("name")),
                .cluster = try dupeStr(allocator, cd.get("cluster")),
                .user = try dupeStr(allocator, cd.get("user")),
                .namespace = try dupeStrOpt(allocator, cd.get("namespace")),
            });
        };
    }

    return .{
        .allocator = allocator,
        .clusters = try clusters.toOwnedSlice(allocator),
        .users = try users.toOwnedSlice(allocator),
        .contexts = try contexts.toOwnedSlice(allocator),
        .current_context = try dupeStrOpt(allocator, obj.get("current-context")),
    };
}

fn fromYamlValue(allocator: Allocator, doc: Yaml.Value) !Kubeconfig {
    const root = doc.asMap() orelse return error.InvalidKubeconfig;

    var clusters: std.ArrayList(Cluster) = .empty;
    errdefer {
        for (clusters.items) |*c| c.deinit(allocator);
        clusters.deinit(allocator);
    }
    if (root.get("clusters")) |v| {
        if (v.asList()) |list| for (list) |item| {
            const o = item.asMap() orelse continue;
            const cd = if (o.get("cluster")) |c| c.asMap() orelse continue else continue;
            try clusters.append(allocator, .{
                .name = try dupeYamlStr(allocator, o.get("name")),
                .server = try dupeYamlStr(allocator, cd.get("server")),
                .certificate_authority = try dupeYamlStrOpt(allocator, cd.get("certificate-authority")),
                .certificate_authority_data = try dupeYamlStrOpt(allocator, cd.get("certificate-authority-data")),
                .tls_server_name = try dupeYamlStrOpt(allocator, cd.get("tls-server-name")),
                .insecure_skip_tls_verify = if (cd.get("insecure-skip-tls-verify")) |v2|
                    mem.eql(u8, v2.asScalar() orelse "false", "true")
                else
                    false,
            });
        };
    }

    var users: std.ArrayList(User) = .empty;
    errdefer {
        for (users.items) |*u| u.deinit(allocator);
        users.deinit(allocator);
    }
    if (root.get("users")) |v| {
        if (v.asList()) |list| for (list) |item| {
            const o = item.asMap() orelse continue;
            const ud = if (o.get("user")) |u| u.asMap() orelse continue else continue;
            try users.append(allocator, .{
                .name = try dupeYamlStr(allocator, o.get("name")),
                .token = try dupeYamlStrOpt(allocator, ud.get("token")),
                .token_file = try dupeYamlStrOpt(allocator, ud.get("tokenFile")),
                .client_certificate = try dupeYamlStrOpt(allocator, ud.get("client-certificate")),
                .client_certificate_data = try dupeYamlStrOpt(allocator, ud.get("client-certificate-data")),
                .client_key = try dupeYamlStrOpt(allocator, ud.get("client-key")),
                .client_key_data = try dupeYamlStrOpt(allocator, ud.get("client-key-data")),
                .username = try dupeYamlStrOpt(allocator, ud.get("username")),
                .password = try dupeYamlStrOpt(allocator, ud.get("password")),
                .exec = try parseExecConfigFromYaml(allocator, ud.get("exec"))
            });
        };
    }

    var contexts: std.ArrayList(Context) = .empty;
    errdefer {
        for (contexts.items) |*c| c.deinit(allocator);
        contexts.deinit(allocator);
    }
    if (root.get("contexts")) |v| {
        if (v.asList()) |list| for (list) |item| {
            const o = item.asMap() orelse continue;
            const cd = if (o.get("context")) |c| c.asMap() orelse continue else continue;
            try contexts.append(allocator, .{
                .name = try dupeYamlStr(allocator, o.get("name")),
                .cluster = try dupeYamlStr(allocator, cd.get("cluster")),
                .user = try dupeYamlStr(allocator, cd.get("user")),
                .namespace = try dupeYamlStrOpt(allocator, cd.get("namespace")),
            });
        };
    }

    return .{
        .allocator = allocator,
        .clusters = try clusters.toOwnedSlice(allocator),
        .users = try users.toOwnedSlice(allocator),
        .contexts = try contexts.toOwnedSlice(allocator),
        .current_context = try dupeYamlStrOpt(allocator, root.get("current-context")),
    };
}

// ── JSON helpers ──────────────────────────────────────────────────────────

fn dupeStr(allocator: Allocator, val: ?json.Value) ![]const u8 {
    if (val) |v| {
        if (v == .string) return try allocator.dupe(u8, v.string);
    }
    return error.MissingField;
}

fn dupeStrOpt(allocator: Allocator, val: ?json.Value) !?[]const u8 {
    if (val) |v| {
        if (v == .string) return try allocator.dupe(u8, v.string);
    }
    return null;
}

fn parseExecConfig(allocator: Allocator, val: ?json.Value) !?ExecConfig {
    const v = val orelse return null;
    if (v != .object) return null;
    const obj = v.object;

    const command_val = obj.get("command") orelse return null;
    if (command_val != .string) return null;

    var args_list: ?[]const []const u8 = null;
    if (obj.get("args")) |args_val| {
        if (args_val == .array) {
            var args: std.ArrayList([]const u8) = .empty;
            for (args_val.array.items) |arg| {
                if (arg == .string) {
                    try args.append(allocator, try allocator.dupe(u8, arg.string));
                }
            }
            args_list = try args.toOwnedSlice(allocator);
        }
    }

    return .{
        .api_version = try dupeStrOpt(allocator, obj.get("apiVersion")),
        .command = try allocator.dupe(u8, command_val.string),
        .args = args_list,
    };
}

// ── YAML helpers ──────────────────────────────────────────────────────────

fn dupeYamlStr(allocator: Allocator, val: ?Yaml.Value) ![]const u8 {
    if (val) |v| {
        if (v.asScalar()) |s| return try allocator.dupe(u8, s);
    }
    return error.MissingField;
}

fn dupeYamlStrOpt(allocator: Allocator, val: ?Yaml.Value) !?[]const u8 {
    if (val) |v| {
        if (v.asScalar()) |s| return try allocator.dupe(u8, s);
    }
    return null;
}

fn parseExecConfigFromYaml(allocator: Allocator, val: ?Yaml.Value) !?ExecConfig {
    const v = val orelse return null;
    const m = v.asMap() orelse return null;

    const command = if (m.get("command")) |c| c.asScalar() orelse return null else return null;

    var args_list: ?[]const []const u8 = null;
    if (m.get("args")) |args_val| {
        if (args_val.asList()) |list| {
            var args: std.ArrayList([]const u8) = .empty;
            errdefer {
                for (args.items) |a| allocator.free(a);
                args.deinit(allocator);
            }
            for (list) |arg| {
                if (arg.asScalar()) |s| {
                    try args.append(allocator, try allocator.dupe(u8, s));
                }
            }
            args_list = try args.toOwnedSlice(allocator);
        }
    }
    errdefer if (args_list) |args| {
        for (args) |a| allocator.free(a);
        allocator.free(args);
    };

    return .{
        .api_version = try dupeYamlStrOpt(allocator, m.get("apiVersion")),
        .command = try allocator.dupe(u8, command),
        .args = args_list,
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const test_kubeconfig_json =
    \\{
    \\  "apiVersion": "v1",
    \\  "kind": "Config",
    \\  "current-context": "dev",
    \\  "clusters": [
    \\    {"name": "dev-cluster", "cluster": {"server": "https://dev.example.com:6443", "certificate-authority-data": "LS0tLS1C"}},
    \\    {"name": "prod-cluster", "cluster": {"server": "https://prod.example.com:6443", "insecure-skip-tls-verify": true}}
    \\  ],
    \\  "users": [
    \\    {"name": "dev-user", "user": {"token": "dev-token-123"}},
    \\    {"name": "prod-user", "user": {"client-certificate-data": "Y2VydA==", "client-key-data": "a2V5"}}
    \\  ],
    \\  "contexts": [
    \\    {"name": "dev", "context": {"cluster": "dev-cluster", "user": "dev-user", "namespace": "development"}},
    \\    {"name": "prod", "context": {"cluster": "prod-cluster", "user": "prod-user"}}
    \\  ]
    \\}
;

test "fromJson: parse kubeconfig" {
    var kc = try fromJson(testing.allocator, test_kubeconfig_json);
    defer kc.deinit();

    try testing.expectEqualStrings("dev", kc.current_context.?);
    try testing.expectEqual(@as(usize, 2), kc.clusters.len);
    try testing.expectEqual(@as(usize, 2), kc.users.len);
    try testing.expectEqual(@as(usize, 2), kc.contexts.len);
}

test "fromJson: cluster fields" {
    var kc = try fromJson(testing.allocator, test_kubeconfig_json);
    defer kc.deinit();

    const dev = kc.getCluster("dev-cluster").?;
    try testing.expectEqualStrings("https://dev.example.com:6443", dev.server);
    try testing.expectEqualStrings("LS0tLS1C", dev.certificate_authority_data.?);
    try testing.expect(!dev.insecure_skip_tls_verify);

    const prod = kc.getCluster("prod-cluster").?;
    try testing.expect(prod.insecure_skip_tls_verify);
    try testing.expect(prod.certificate_authority_data == null);
}

test "fromJson: user fields" {
    var kc = try fromJson(testing.allocator, test_kubeconfig_json);
    defer kc.deinit();

    const dev = kc.getUser("dev-user").?;
    try testing.expectEqualStrings("dev-token-123", dev.token.?);
    try testing.expect(dev.client_certificate_data == null);

    const prod = kc.getUser("prod-user").?;
    try testing.expect(prod.token == null);
    try testing.expectEqualStrings("Y2VydA==", prod.client_certificate_data.?);
    try testing.expectEqualStrings("a2V5", prod.client_key_data.?);
}

test "fromJson: context resolution" {
    var kc = try fromJson(testing.allocator, test_kubeconfig_json);
    defer kc.deinit();

    const current = kc.getCurrentContext().?;
    try testing.expectEqualStrings("dev", current.name);
    try testing.expectEqualStrings("dev-cluster", current.cluster);
    try testing.expectEqualStrings("dev-user", current.user);
    try testing.expectEqualStrings("development", current.namespace.?);

    const prod = kc.getContext("prod").?;
    try testing.expect(prod.namespace == null);
}

test "fromJson: missing context returns null" {
    var kc = try fromJson(testing.allocator, test_kubeconfig_json);
    defer kc.deinit();

    try testing.expect(kc.getContext("nonexistent") == null);
    try testing.expect(kc.getCluster("nonexistent") == null);
    try testing.expect(kc.getUser("nonexistent") == null);
}

const test_kubeconfig_yaml =
    \\apiVersion: v1
    \\kind: Config
    \\current-context: dev
    \\clusters:
    \\- name: dev-cluster
    \\  cluster:
    \\    server: https://dev.example.com:6443
    \\    certificate-authority-data: LS0tLS1C
    \\users:
    \\- name: dev-user
    \\  user:
    \\    token: dev-token-123
    \\contexts:
    \\- name: dev
    \\  context:
    \\    cluster: dev-cluster
    \\    user: dev-user
    \\    namespace: development
;

test "fromYaml: parse kubeconfig" {
    var kc = try fromYaml(testing.allocator, test_kubeconfig_yaml);
    defer kc.deinit();

    try testing.expectEqualStrings("dev", kc.current_context.?);
    try testing.expectEqual(@as(usize, 1), kc.clusters.len);
    try testing.expectEqual(@as(usize, 1), kc.users.len);
    try testing.expectEqual(@as(usize, 1), kc.contexts.len);

    const cluster = kc.getCluster("dev-cluster").?;
    try testing.expectEqualStrings("https://dev.example.com:6443", cluster.server);

    const user = kc.getUser("dev-user").?;
    try testing.expectEqualStrings("dev-token-123", user.token.?);

    const ctx = kc.getCurrentContext().?;
    try testing.expectEqualStrings("development", ctx.namespace.?);
}

test "fromJson: exec config parsing" {
    const json_with_exec =
        \\{
        \\  "apiVersion": "v1",
        \\  "kind": "Config",
        \\  "current-context": "eks",
        \\  "clusters": [
        \\    {"name": "eks-cluster", "cluster": {"server": "https://eks.example.com"}}
        \\  ],
        \\  "users": [
        \\    {"name": "eks-user", "user": {"exec": {"apiVersion": "client.authentication.k8s.io/v1beta1", "command": "aws", "args": ["eks", "get-token", "--cluster-name", "my-cluster"]}}}
        \\  ],
        \\  "contexts": [
        \\    {"name": "eks", "context": {"cluster": "eks-cluster", "user": "eks-user"}}
        \\  ]
        \\}
    ;
    var kc = try fromJson(testing.allocator, json_with_exec);
    defer kc.deinit();

    const user = kc.getUser("eks-user").?;
    const exec = user.exec.?;
    try testing.expectEqualStrings("aws", exec.command);
    try testing.expectEqualStrings("client.authentication.k8s.io/v1beta1", exec.api_version.?);
    const args = exec.args.?;
    try testing.expectEqual(@as(usize, 4), args.len);
    try testing.expectEqualStrings("eks", args[0]);
    try testing.expectEqualStrings("get-token", args[1]);
    try testing.expectEqualStrings("--cluster-name", args[2]);
    try testing.expectEqualStrings("my-cluster", args[3]);
}

test "fromYaml: exec config parsing" {
    const yaml_with_exec =
        \\apiVersion: v1
        \\kind: Config
        \\current-context: eks
        \\clusters:
        \\- name: eks-cluster
        \\  cluster:
        \\    server: https://eks.example.com
        \\users:
        \\- name: eks-user
        \\  user:
        \\    exec:
        \\      apiVersion: client.authentication.k8s.io/v1beta1
        \\      command: aws
        \\      args:
        \\      - eks
        \\      - get-token
        \\contexts:
        \\- name: eks
        \\  context:
        \\    cluster: eks-cluster
        \\    user: eks-user
    ;
    var kc = try fromYaml(testing.allocator, yaml_with_exec);
    defer kc.deinit();

    const user = kc.getUser("eks-user").?;
    const exec = user.exec.?;
    try testing.expectEqualStrings("aws", exec.command);
    try testing.expectEqualStrings("client.authentication.k8s.io/v1beta1", exec.api_version.?);
    const args = exec.args.?;
    try testing.expectEqual(@as(usize, 2), args.len);
    try testing.expectEqualStrings("eks", args[0]);
    try testing.expectEqualStrings("get-token", args[1]);
}

test "fromJson: exec config with no args" {
    const json_exec_no_args =
        \\{
        \\  "apiVersion": "v1",
        \\  "kind": "Config",
        \\  "current-context": "test",
        \\  "clusters": [
        \\    {"name": "test-cluster", "cluster": {"server": "https://test.example.com"}}
        \\  ],
        \\  "users": [
        \\    {"name": "test-user", "user": {"exec": {"apiVersion": "client.authentication.k8s.io/v1beta1", "command": "/usr/local/bin/my-auth-plugin"}}}
        \\  ],
        \\  "contexts": [
        \\    {"name": "test", "context": {"cluster": "test-cluster", "user": "test-user"}}
        \\  ]
        \\}
    ;
    var kc = try fromJson(testing.allocator, json_exec_no_args);
    defer kc.deinit();

    const user = kc.getUser("test-user").?;
    const exec = user.exec.?;
    try testing.expectEqualStrings("/usr/local/bin/my-auth-plugin", exec.command);
    try testing.expectEqualStrings("client.authentication.k8s.io/v1beta1", exec.api_version.?);
    try testing.expect(exec.args == null);
}

test "fromJson: user with no auth" {
    const json_no_auth =
        \\{
        \\  "apiVersion": "v1",
        \\  "kind": "Config",
        \\  "current-context": "empty",
        \\  "clusters": [
        \\    {"name": "empty-cluster", "cluster": {"server": "https://empty.example.com"}}
        \\  ],
        \\  "users": [
        \\    {"name": "empty-user", "user": {}}
        \\  ],
        \\  "contexts": [
        \\    {"name": "empty", "context": {"cluster": "empty-cluster", "user": "empty-user"}}
        \\  ]
        \\}
    ;
    var kc = try fromJson(testing.allocator, json_no_auth);
    defer kc.deinit();

    const user = kc.getUser("empty-user").?;
    try testing.expectEqualStrings("empty-user", user.name);
    try testing.expect(user.token == null);
    try testing.expect(user.token_file == null);
    try testing.expect(user.client_certificate == null);
    try testing.expect(user.client_certificate_data == null);
    try testing.expect(user.client_key == null);
    try testing.expect(user.client_key_data == null);
    try testing.expect(user.username == null);
    try testing.expect(user.password == null);
    try testing.expect(user.exec == null);
}

test "fromJson and fromYaml: user without exec has null exec" {
    var kc_json = try fromJson(testing.allocator, test_kubeconfig_json);
    defer kc_json.deinit();
    try testing.expect(kc_json.getUser("dev-user").?.exec == null);

    var kc_yaml = try fromYaml(testing.allocator, test_kubeconfig_yaml);
    defer kc_yaml.deinit();
    try testing.expect(kc_yaml.getUser("dev-user").?.exec == null);
}
