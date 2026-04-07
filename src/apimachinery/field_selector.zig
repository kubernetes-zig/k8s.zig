const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const Allocator = mem.Allocator;

/// Operator for a field selector requirement.
pub const Operator = enum {
    equals,
    not_equals,
};

/// A single field selector requirement.
/// e.g., "metadata.name=nginx" or "status.phase!=Running"
pub const Requirement = struct {
    field: []const u8,
    op: Operator,
    value: []const u8,

    /// Check if this requirement matches by looking up the field path.
    /// `lookup` must implement `fn get(key: []const u8) ?[]const u8`.
    pub fn matches(self: Requirement, lookup: anytype) bool {
        const val = lookup.get(self.field) orelse return self.op == .not_equals;
        return switch (self.op) {
            .equals => mem.eql(u8, val, self.value),
            .not_equals => !mem.eql(u8, val, self.value),
        };
    }
};

/// A field selector is a conjunction (AND) of requirements.
pub const Selector = struct {
    requirements: []const Requirement,

    pub fn matchesAll() Selector {
        return .{ .requirements = &.{} };
    }

    pub fn matches(self: Selector, lookup: anytype) bool {
        for (self.requirements) |req| {
            if (!req.matches(lookup)) return false;
        }
        return true;
    }

    pub fn empty(self: Selector) bool {
        return self.requirements.len == 0;
    }

    pub fn deinit(self: Selector, allocator: Allocator) void {
        for (self.requirements) |req| {
            allocator.free(req.field);
            allocator.free(req.value);
        }
        if (self.requirements.len > 0) allocator.free(self.requirements);
    }
};

/// Parse a K8s field selector string.
/// Supports: field=value, field!=value, comma-separated AND.
/// Field selectors are simpler than label selectors — no in/notin/exists/gt/lt.
pub fn parse(allocator: Allocator, input: []const u8) !Selector {
    const trimmed = mem.trim(u8, input, " ");
    if (trimmed.len == 0) return Selector.matchesAll();

    var requirements: std.ArrayList(Requirement) = .empty;
    errdefer {
        for (requirements.items) |req| {
            allocator.free(req.field);
            allocator.free(req.value);
        }
        requirements.deinit(allocator);
    }

    var it = mem.splitScalar(u8, trimmed, ',');
    while (it.next()) |part| {
        const p = mem.trim(u8, part, " ");
        if (p.len == 0) continue;

        if (mem.indexOf(u8, p, "!=")) |i| {
            const field = try allocator.dupe(u8, mem.trim(u8, p[0..i], " "));
            errdefer allocator.free(field);
            const value = try allocator.dupe(u8, mem.trim(u8, p[i + 2 ..], " "));
            errdefer allocator.free(value);
            try requirements.append(allocator, .{ .field = field, .op = .not_equals, .value = value });
        } else if (mem.indexOfScalar(u8, p, '=')) |i| {
            const field = try allocator.dupe(u8, mem.trim(u8, p[0..i], " "));
            errdefer allocator.free(field);
            const value = try allocator.dupe(u8, mem.trim(u8, p[i + 1 ..], " "));
            errdefer allocator.free(value);
            try requirements.append(allocator, .{ .field = field, .op = .equals, .value = value });
        } else {
            return error.InvalidSelector;
        }
    }

    return .{ .requirements = try requirements.toOwnedSlice(allocator) };
}

/// Simple field map — flat key-value pairs where key is the dotted path.
pub const FieldMap = struct {
    entries: []const Entry,

    pub const Entry = struct { key: []const u8, value: []const u8 };

    pub fn get(self: FieldMap, key: []const u8) ?[]const u8 {
        for (self.entries) |e| {
            if (mem.eql(u8, e.key, key)) return e.value;
        }
        return null;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const test_fields = FieldMap{ .entries = &.{
    .{ .key = "metadata.name", .value = "nginx" },
    .{ .key = "metadata.namespace", .value = "default" },
    .{ .key = "status.phase", .value = "Running" },
} };

test "parse and match: equality" {
    const cases = .{
        .{ "metadata.name=nginx", true },
        .{ "metadata.name=other", false },
        .{ "metadata.namespace=default", true },
        .{ "status.phase=Running", true },
        .{ "status.phase=Pending", false },
        .{ "missing.field=value", false },
    };
    inline for (cases) |c| {
        const sel = try parse(testing.allocator, c[0]);
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(c[1], sel.matches(test_fields));
    }
}

test "parse and match: not equals" {
    const cases = .{
        .{ "metadata.name!=other", true },
        .{ "metadata.name!=nginx", false },
        .{ "status.phase!=Pending", true },
        .{ "status.phase!=Running", false },
        .{ "missing.field!=value", true },
    };
    inline for (cases) |c| {
        const sel = try parse(testing.allocator, c[0]);
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(c[1], sel.matches(test_fields));
    }
}

test "parse and match: compound" {
    const cases = .{
        .{ "metadata.name=nginx,metadata.namespace=default", true },
        .{ "metadata.name=nginx,status.phase=Pending", false },
        .{ "metadata.name!=other,status.phase!=Pending", true },
    };
    inline for (cases) |c| {
        const sel = try parse(testing.allocator, c[0]);
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(c[1], sel.matches(test_fields));
    }
}

test "parse: empty matches all" {
    const sel = try parse(testing.allocator, "");
    try testing.expect(sel.empty());
    try testing.expect(sel.matches(test_fields));
}

test "parse: whitespace handling" {
    const sel = try parse(testing.allocator, " metadata.name = nginx , status.phase = Running ");
    defer sel.deinit(testing.allocator);
    try testing.expect(sel.matches(test_fields));
}

test "parse: invalid selector" {
    try testing.expectError(error.InvalidSelector, parse(testing.allocator, "justAKey"));
    try testing.expectError(error.InvalidSelector, parse(testing.allocator, "key in (a,b)"));
}
