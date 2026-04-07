const std = @import("std");
const mem = std.mem;
const testing = std.testing;

/// GroupVersion identifies an API group and version pair.
/// Represents strings like "apps/v1" or "v1" (core group).
pub const GroupVersion = struct {
    group: []const u8,
    version: []const u8,

    /// Parses an apiVersion string into a GroupVersion.
    /// Handles both "group/version" and "version" (core group) formats.
    /// Returns null if the input is empty.
    pub fn parse(s: []const u8) ?GroupVersion {
        if (s.len == 0) return null;
        if (mem.indexOfScalar(u8, s, '/')) |i| {
            return .{
                .group = s[0..i],
                .version = s[i + 1 ..],
            };
        }
        return .{ .group = "", .version = s };
    }

    pub fn eql(a: GroupVersion, b: GroupVersion) bool {
        return mem.eql(u8, a.group, b.group) and mem.eql(u8, a.version, b.version);
    }

    /// Returns the "group/version" string representation.
    /// For core group (empty group), returns just the version.
    pub fn format(self: GroupVersion, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        if (self.group.len > 0) {
            try writer.print("{s}/{s}", .{ self.group, self.version });
        } else {
            try writer.print("{s}", .{self.version});
        }
    }

    pub fn withKind(self: GroupVersion, kind: []const u8) GroupVersionKind {
        return .{ .group = self.group, .version = self.version, .kind = kind };
    }

    pub fn withResource(self: GroupVersion, resource: []const u8) GroupVersionResource {
        return .{ .group = self.group, .version = self.version, .resource = resource };
    }

    /// Writes the "group/version" string into the provided buffer.
    /// For core group (empty group), writes just the version.
    pub fn string(self: GroupVersion, buf: []u8) ![]const u8 {
        if (self.group.len > 0) {
            return std.fmt.bufPrint(buf, "{s}/{s}", .{ self.group, self.version });
        } else {
            return std.fmt.bufPrint(buf, "{s}", .{self.version});
        }
    }
};

/// GroupVersionKind identifies a kind in a particular group and version.
pub const GroupVersionKind = struct {
    group: []const u8,
    version: []const u8,
    kind: []const u8,

    pub fn eql(a: GroupVersionKind, b: GroupVersionKind) bool {
        return mem.eql(u8, a.group, b.group) and
            mem.eql(u8, a.version, b.version) and
            mem.eql(u8, a.kind, b.kind);
    }

    pub fn groupVersion(self: GroupVersionKind) GroupVersion {
        return .{ .group = self.group, .version = self.version };
    }

    /// Formats as "apps/v1, Kind=Deployment" or "v1, Kind=Pod" (core group).
    pub fn format(self: GroupVersionKind, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        if (self.group.len > 0) {
            try writer.print("{s}/{s}, Kind={s}", .{ self.group, self.version, self.kind });
        } else {
            try writer.print("{s}, Kind={s}", .{ self.version, self.kind });
        }
    }

    /// Writes the formatted string into the provided buffer.
    pub fn string(self: GroupVersionKind, buf: []u8) ![]const u8 {
        if (self.group.len > 0) {
            return std.fmt.bufPrint(buf, "{s}/{s}, Kind={s}", .{ self.group, self.version, self.kind });
        } else {
            return std.fmt.bufPrint(buf, "{s}, Kind={s}", .{ self.version, self.kind });
        }
    }
};

/// GroupVersionResource identifies a resource in a particular group and version.
pub const GroupVersionResource = struct {
    group: []const u8,
    version: []const u8,
    resource: []const u8,

    pub fn eql(a: GroupVersionResource, b: GroupVersionResource) bool {
        return mem.eql(u8, a.group, b.group) and
            mem.eql(u8, a.version, b.version) and
            mem.eql(u8, a.resource, b.resource);
    }

    pub fn groupVersion(self: GroupVersionResource) GroupVersion {
        return .{ .group = self.group, .version = self.version };
    }

    /// Formats as "apps/v1/deployments" or "v1/pods" (core group).
    pub fn format(self: GroupVersionResource, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        if (self.group.len > 0) {
            try writer.print("{s}/{s}/{s}", .{ self.group, self.version, self.resource });
        } else {
            try writer.print("{s}/{s}", .{ self.version, self.resource });
        }
    }

    /// Writes the formatted string into the provided buffer.
    pub fn string(self: GroupVersionResource, buf: []u8) ![]const u8 {
        if (self.group.len > 0) {
            return std.fmt.bufPrint(buf, "{s}/{s}/{s}", .{ self.group, self.version, self.resource });
        } else {
            return std.fmt.bufPrint(buf, "{s}/{s}", .{ self.version, self.resource });
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "GroupVersion.parse" {
    const cases = .{
        .{ "v1", "", "v1" },
        .{ "apps/v1", "apps", "v1" },
        .{ "rbac.authorization.k8s.io/v1", "rbac.authorization.k8s.io", "v1" },
        .{ "batch/v1beta1", "batch", "v1beta1" },
    };
    inline for (cases) |c| {
        const gv = GroupVersion.parse(c[0]).?;
        try testing.expectEqualStrings(c[1], gv.group);
        try testing.expectEqualStrings(c[2], gv.version);
    }
    try testing.expect(GroupVersion.parse("") == null);
}

test "GroupVersion.eql" {
    const cases = .{
        .{ GroupVersion{ .group = "apps", .version = "v1" }, GroupVersion{ .group = "apps", .version = "v1" }, true },
        .{ GroupVersion{ .group = "apps", .version = "v1" }, GroupVersion{ .group = "apps", .version = "v1beta1" }, false },
        .{ GroupVersion{ .group = "", .version = "v1" }, GroupVersion{ .group = "", .version = "v1" }, true },
        .{ GroupVersion{ .group = "apps", .version = "v1" }, GroupVersion{ .group = "batch", .version = "v1" }, false },
    };
    inline for (cases) |c| {
        try testing.expectEqual(c[2], c[0].eql(c[1]));
    }
}

test "GroupVersion.withKind and withResource" {
    const gv = GroupVersion{ .group = "apps", .version = "v1" };

    const gvk = gv.withKind("Deployment");
    try testing.expectEqualStrings("apps", gvk.group);
    try testing.expectEqualStrings("v1", gvk.version);
    try testing.expectEqualStrings("Deployment", gvk.kind);

    const gvr = gv.withResource("deployments");
    try testing.expectEqualStrings("apps", gvr.group);
    try testing.expectEqualStrings("v1", gvr.version);
    try testing.expectEqualStrings("deployments", gvr.resource);
}

test "GroupVersion.string" {
    const cases = .{
        .{ GroupVersion{ .group = "apps", .version = "v1" }, "apps/v1" },
        .{ GroupVersion{ .group = "", .version = "v1" }, "v1" },
        .{ GroupVersion{ .group = "rbac.authorization.k8s.io", .version = "v1" }, "rbac.authorization.k8s.io/v1" },
    };
    inline for (cases) |c| {
        var buf: [128]u8 = undefined;
        const s = try c[0].string(&buf);
        try testing.expectEqualStrings(c[1], s);
    }
}

test "GroupVersionKind.eql" {
    const cases = .{
        .{ GroupVersionKind{ .group = "apps", .version = "v1", .kind = "Deployment" }, GroupVersionKind{ .group = "apps", .version = "v1", .kind = "Deployment" }, true },
        .{ GroupVersionKind{ .group = "apps", .version = "v1", .kind = "Deployment" }, GroupVersionKind{ .group = "apps", .version = "v1", .kind = "StatefulSet" }, false },
    };
    inline for (cases) |c| {
        try testing.expectEqual(c[2], c[0].eql(c[1]));
    }
}

test "GroupVersionKind.groupVersion" {
    const gvk = GroupVersionKind{ .group = "apps", .version = "v1", .kind = "Deployment" };
    const gv = gvk.groupVersion();
    try testing.expectEqualStrings("apps", gv.group);
    try testing.expectEqualStrings("v1", gv.version);
}

test "GroupVersionKind.string" {
    const cases = .{
        .{ GroupVersionKind{ .group = "apps", .version = "v1", .kind = "Deployment" }, "apps/v1, Kind=Deployment" },
        .{ GroupVersionKind{ .group = "", .version = "v1", .kind = "Pod" }, "v1, Kind=Pod" },
    };
    inline for (cases) |c| {
        var buf: [128]u8 = undefined;
        const s = try c[0].string(&buf);
        try testing.expectEqualStrings(c[1], s);
    }
}

test "GroupVersionResource.eql" {
    const cases = .{
        .{ GroupVersionResource{ .group = "apps", .version = "v1", .resource = "deployments" }, GroupVersionResource{ .group = "apps", .version = "v1", .resource = "deployments" }, true },
        .{ GroupVersionResource{ .group = "apps", .version = "v1", .resource = "deployments" }, GroupVersionResource{ .group = "apps", .version = "v1", .resource = "statefulsets" }, false },
    };
    inline for (cases) |c| {
        try testing.expectEqual(c[2], c[0].eql(c[1]));
    }
}

test "GroupVersionResource.groupVersion" {
    const gvr = GroupVersionResource{ .group = "apps", .version = "v1", .resource = "deployments" };
    const gv = gvr.groupVersion();
    try testing.expectEqualStrings("apps", gv.group);
    try testing.expectEqualStrings("v1", gv.version);
}

test "GroupVersionResource.string" {
    const cases = .{
        .{ GroupVersionResource{ .group = "apps", .version = "v1", .resource = "deployments" }, "apps/v1/deployments" },
        .{ GroupVersionResource{ .group = "", .version = "v1", .resource = "pods" }, "v1/pods" },
    };
    inline for (cases) |c| {
        var buf: [128]u8 = undefined;
        const s = try c[0].string(&buf);
        try testing.expectEqualStrings(c[1], s);
    }
}
