const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const json = std.json;
const Allocator = mem.Allocator;

/// Operator for a label selector requirement.
pub const Operator = enum {
    equals,
    not_equals,
    in,
    not_in,
    exists,
    does_not_exist,
    greater_than,
    less_than,
};

/// A single requirement in a label selector.
/// e.g., "app=nginx" is Requirement{ .key = "app", .op = .equals, .values = &.{"nginx"} }
pub const Requirement = struct {
    key: []const u8,
    op: Operator,
    values: []const []const u8,

    /// Check if this requirement matches the given labels.
    /// Labels are represented as a lookup function (key → ?value).
    pub fn matches(self: Requirement, lookup: anytype) bool {
        return matchesImpl(self, lookup);
    }

    fn matchesImpl(self: Requirement, lookup: anytype) bool {
        switch (self.op) {
            .exists => return lookup.get(self.key) != null,
            .does_not_exist => return lookup.get(self.key) == null,
            .equals => {
                const val = lookup.get(self.key) orelse return false;
                return self.values.len == 1 and mem.eql(u8, valStr(val), self.values[0]);
            },
            .not_equals => {
                const val = lookup.get(self.key) orelse return true;
                return self.values.len == 1 and !mem.eql(u8, valStr(val), self.values[0]);
            },
            .in => {
                const val = lookup.get(self.key) orelse return false;
                const v = valStr(val);
                for (self.values) |allowed| {
                    if (mem.eql(u8, v, allowed)) return true;
                }
                return false;
            },
            .not_in => {
                const val = lookup.get(self.key) orelse return true;
                const v = valStr(val);
                for (self.values) |disallowed| {
                    if (mem.eql(u8, v, disallowed)) return false;
                }
                return true;
            },
            .greater_than => {
                const val = lookup.get(self.key) orelse return false;
                const lhs = std.fmt.parseInt(i64, valStr(val), 10) catch return false;
                if (self.values.len != 1) return false;
                const rhs = std.fmt.parseInt(i64, self.values[0], 10) catch return false;
                return lhs > rhs;
            },
            .less_than => {
                const val = lookup.get(self.key) orelse return false;
                const lhs = std.fmt.parseInt(i64, valStr(val), 10) catch return false;
                if (self.values.len != 1) return false;
                const rhs = std.fmt.parseInt(i64, self.values[0], 10) catch return false;
                return lhs < rhs;
            },
        }
    }

    fn valStr(val: anytype) []const u8 {
        // Support both []const u8 and json.Value
        const T = @TypeOf(val);
        if (T == []const u8) return val;
        if (T == json.Value) return switch (val) {
            .string => |s| s,
            else => "",
        };
        if (T == *const json.Value) return switch (val.*) {
            .string => |s| s,
            else => "",
        };
        return "";
    }
};

/// A label selector is a conjunction (AND) of requirements.
pub const Selector = struct {
    requirements: []const Requirement,

    /// An empty selector matches everything.
    pub fn matchesAll() Selector {
        return .{ .requirements = &.{} };
    }

    /// Check if this selector matches the given labels.
    pub fn matches(self: Selector, lookup: anytype) bool {
        for (self.requirements) |req| {
            if (!req.matches(lookup)) return false;
        }
        return true;
    }

    /// Returns true if this selector matches all labels (empty selector).
    pub fn empty(self: Selector) bool {
        return self.requirements.len == 0;
    }

    /// Free all memory allocated by parse().
    pub fn deinit(self: Selector, allocator: Allocator) void {
        for (self.requirements) |req| {
            for (req.values) |v| {
                allocator.free(v);
            }
            if (req.values.len > 0) allocator.free(req.values);
        }
        if (self.requirements.len > 0) allocator.free(self.requirements);
    }
};

/// Parse a K8s label selector string into a Selector.
/// Supports: key=value, key!=value, key in (v1,v2), key notin (v1,v2), key, !key, key>n, key<n
pub fn parse(allocator: Allocator, input: []const u8) !Selector {
    const trimmed = mem.trim(u8, input, " ");
    if (trimmed.len == 0) return Selector.matchesAll();

    var requirements: std.ArrayList(Requirement) = .empty;
    errdefer requirements.deinit(allocator);

    var rest: []const u8 = trimmed;
    while (rest.len > 0) {
        rest = trimLeft(rest, " ");
        if (rest.len == 0) break;

        const req = try parseRequirement(allocator, &rest);
        try requirements.append(allocator, req);

        rest = trimLeft(rest, " ");
        if (rest.len > 0 and rest[0] == ',') {
            rest = rest[1..];
        }
    }

    return .{ .requirements = try requirements.toOwnedSlice(allocator) };
}

fn parseRequirement(allocator: Allocator, rest: *[]const u8) !Requirement {
    const s = rest.*;

    // DoesNotExist: !key
    if (s[0] == '!') {
        const key_end = findKeyEnd(s[1..]);
        const key = s[1 .. key_end + 1];
        if (key.len == 0) return error.InvalidSelector;
        rest.* = s[key_end + 1 ..];
        return .{ .key = key, .op = .does_not_exist, .values = &.{} };
    }

    // Find the key
    const key_end = findKeyEnd(s);
    const key = s[0..key_end];
    if (key.len == 0) return error.InvalidSelector;

    var after_key = trimLeft(s[key_end..], " ");

    // Exists: bare key at end or before comma
    if (after_key.len == 0 or after_key[0] == ',') {
        rest.* = after_key;
        return .{ .key = key, .op = .exists, .values = &.{} };
    }

    // Operators
    if (mem.startsWith(u8, after_key, "!=")) {
        after_key = trimLeft(after_key[2..], " ");
        const val_end = findValueEnd(after_key);
        const val = try allocator.dupe(u8, after_key[0..val_end]);
        errdefer allocator.free(val);
        const vals = try allocator.alloc([]const u8, 1);
        vals[0] = val;
        rest.* = after_key[val_end..];
        return .{ .key = key, .op = .not_equals, .values = vals };
    }

    if (mem.startsWith(u8, after_key, "==") or mem.startsWith(u8, after_key, "=")) {
        const skip: usize = if (after_key.len >= 2 and after_key[1] == '=') 2 else 1;
        after_key = trimLeft(after_key[skip..], " ");
        const val_end = findValueEnd(after_key);
        const val = try allocator.dupe(u8, after_key[0..val_end]);
        errdefer allocator.free(val);
        const vals = try allocator.alloc([]const u8, 1);
        vals[0] = val;
        rest.* = after_key[val_end..];
        return .{ .key = key, .op = .equals, .values = vals };
    }

    if (mem.startsWith(u8, after_key, ">")) {
        after_key = trimLeft(after_key[1..], " ");
        const val_end = findValueEnd(after_key);
        const val = try allocator.dupe(u8, after_key[0..val_end]);
        errdefer allocator.free(val);
        const vals = try allocator.alloc([]const u8, 1);
        vals[0] = val;
        rest.* = after_key[val_end..];
        return .{ .key = key, .op = .greater_than, .values = vals };
    }

    if (mem.startsWith(u8, after_key, "<")) {
        after_key = trimLeft(after_key[1..], " ");
        const val_end = findValueEnd(after_key);
        const val = try allocator.dupe(u8, after_key[0..val_end]);
        errdefer allocator.free(val);
        const vals = try allocator.alloc([]const u8, 1);
        vals[0] = val;
        rest.* = after_key[val_end..];
        return .{ .key = key, .op = .less_than, .values = vals };
    }

    // "in" and "notin" operators
    if (mem.startsWith(u8, after_key, "notin")) {
        after_key = trimLeft(after_key[5..], " ");
        const vals = try parseValueSet(allocator, &after_key);
        if (vals.len == 0) return error.InvalidSelector;
        rest.* = after_key;
        return .{ .key = key, .op = .not_in, .values = vals };
    }

    if (mem.startsWith(u8, after_key, "in")) {
        after_key = trimLeft(after_key[2..], " ");
        const vals = try parseValueSet(allocator, &after_key);
        if (vals.len == 0) return error.InvalidSelector;
        rest.* = after_key;
        return .{ .key = key, .op = .in, .values = vals };
    }

    return error.InvalidSelector;
}

fn parseValueSet(allocator: Allocator, rest: *[]const u8) ![]const []const u8 {
    const s = rest.*;
    if (s.len == 0 or s[0] != '(') return error.InvalidSelector;

    const close = mem.indexOfScalar(u8, s, ')') orelse return error.InvalidSelector;
    const inner = s[1..close];
    rest.* = s[close + 1 ..];

    var values: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (values.items) |v| allocator.free(v);
        values.deinit(allocator);
    }
    var it = mem.splitScalar(u8, inner, ',');
    while (it.next()) |part| {
        const trimmed = mem.trim(u8, part, " ");
        if (trimmed.len > 0) {
            try values.append(allocator, try allocator.dupe(u8, trimmed));
        }
    }
    return try values.toOwnedSlice(allocator);
}

/// Trim leading characters from a slice (replaces std.mem.trimLeft removed in 0.16-dev).
fn trimLeft(s: []const u8, strip: []const u8) []const u8 {
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        var matched = false;
        for (strip) |c| {
            if (s[i] == c) {
                matched = true;
                break;
            }
        }
        if (!matched) break;
    }
    return s[i..];
}

fn findKeyEnd(s: []const u8) usize {
    for (s, 0..) |c, i| {
        switch (c) {
            '=', '!', '>', '<', ' ', ',', '(' => return i,
            else => {},
        }
    }
    return s.len;
}

fn findValueEnd(s: []const u8) usize {
    for (s, 0..) |c, i| {
        switch (c) {
            ',', ' ', ')' => return i,
            else => {},
        }
    }
    return s.len;
}

// ── Label map helper for testing ──────────────────────────────────────────

/// Simple label map for matching. Wraps a slice of key-value pairs.
pub const LabelMap = struct {
    entries: []const Entry,

    pub const Entry = struct { key: []const u8, value: []const u8 };

    pub fn get(self: LabelMap, key: []const u8) ?[]const u8 {
        for (self.entries) |e| {
            if (mem.eql(u8, e.key, key)) return e.value;
        }
        return null;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const test_labels = LabelMap{ .entries = &.{
    .{ .key = "app", .value = "nginx" },
    .{ .key = "tier", .value = "frontend" },
    .{ .key = "version", .value = "v2" },
    .{ .key = "count", .value = "5" },
} };

test "parse and match: equality operators" {
    const cases = .{
        .{ "app=nginx", true },
        .{ "app==nginx", true },
        .{ "app=other", false },
        .{ "app!=other", true },
        .{ "app!=nginx", false },
        .{ "missing=value", false },
        .{ "missing!=value", true },
    };
    inline for (cases) |c| {
        const sel = try parse(testing.allocator, c[0]);
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(c[1], sel.matches(test_labels));
    }
}

test "parse and match: set operators" {
    const cases = .{
        .{ "tier in (frontend,backend)", true },
        .{ "tier in (backend,api)", false },
        .{ "tier notin (backend,api)", true },
        .{ "tier notin (frontend,backend)", false },
        .{ "missing in (a,b)", false },
        .{ "missing notin (a,b)", true },
    };
    inline for (cases) |c| {
        const sel = try parse(testing.allocator, c[0]);
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(c[1], sel.matches(test_labels));
    }
}

test "parse and match: existence operators" {
    const cases = .{
        .{ "app", true },
        .{ "missing", false },
        .{ "!missing", true },
        .{ "!app", false },
    };
    inline for (cases) |c| {
        const sel = try parse(testing.allocator, c[0]);
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(c[1], sel.matches(test_labels));
    }
}

test "parse and match: comparison operators" {
    const cases = .{
        .{ "count > 3", true },
        .{ "count > 5", false },
        .{ "count > 10", false },
        .{ "count < 10", true },
        .{ "count < 5", false },
        .{ "count < 3", false },
    };
    inline for (cases) |c| {
        const sel = try parse(testing.allocator, c[0]);
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(c[1], sel.matches(test_labels));
    }
}

test "parse and match: compound selectors" {
    const cases = .{
        .{ "app=nginx,tier=frontend", true },
        .{ "app=nginx,tier=backend", false },
        .{ "app=nginx,!missing", true },
        .{ "app,tier,version", true },
        .{ "app=nginx,tier in (frontend,backend),!missing", true },
    };
    inline for (cases) |c| {
        const sel = try parse(testing.allocator, c[0]);
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(c[1], sel.matches(test_labels));
    }
}

test "parse: empty selector matches everything" {
    const sel = try parse(testing.allocator, "");
    try testing.expect(sel.empty());
    try testing.expect(sel.matches(test_labels));
}

test "parse: whitespace handling" {
    const cases = .{
        "  app = nginx  ",
        "app =nginx",
        "app= nginx",
        " app = nginx , tier = frontend ",
    };
    inline for (cases) |c| {
        const sel = try parse(testing.allocator, c);
        defer sel.deinit(testing.allocator);
        try testing.expect(sel.matches(test_labels));
    }
}

test "parse: invalid selectors" {
    const cases = .{
        "=value",
        "!",
        "key in ()",
        "key in ",
    };
    inline for (cases) |c| {
        const result = parse(testing.allocator, c);
        try testing.expect(result == error.InvalidSelector);
    }
}

test "parse: edge cases" {
    // "key=" → equals with empty value
    {
        const sel = try parse(testing.allocator, "key=");
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(@as(usize, 1), sel.requirements.len);
        try testing.expectEqual(Operator.equals, sel.requirements[0].op);
        try testing.expectEqualStrings("key", sel.requirements[0].key);
        try testing.expectEqualStrings("", sel.requirements[0].values[0]);
    }

    // "key==" → double equals with empty value
    {
        const sel = try parse(testing.allocator, "key==");
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(@as(usize, 1), sel.requirements.len);
        try testing.expectEqual(Operator.equals, sel.requirements[0].op);
        try testing.expectEqualStrings("key", sel.requirements[0].key);
        try testing.expectEqualStrings("", sel.requirements[0].values[0]);
    }

    // single key → exists operator
    {
        const sel = try parse(testing.allocator, "mykey");
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(@as(usize, 1), sel.requirements.len);
        try testing.expectEqual(Operator.exists, sel.requirements[0].op);
        try testing.expectEqualStrings("mykey", sel.requirements[0].key);
        try testing.expectEqual(@as(usize, 0), sel.requirements[0].values.len);
    }

    // "!key" → does_not_exist operator
    {
        const sel = try parse(testing.allocator, "!mykey");
        defer sel.deinit(testing.allocator);
        try testing.expectEqual(@as(usize, 1), sel.requirements.len);
        try testing.expectEqual(Operator.does_not_exist, sel.requirements[0].op);
        try testing.expectEqualStrings("mykey", sel.requirements[0].key);
        try testing.expectEqual(@as(usize, 0), sel.requirements[0].values.len);
    }
}

test "fuzz: selector parse never crashes on arbitrary input" {
    try std.testing.fuzz({}, fuzzSelectorParse, .{});
}

fn fuzzSelectorParse(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    const len = smith.valueRangeAtMost(u8, 0, 128);
    var buf: [128]u8 = undefined;
    for (buf[0..len]) |*b| {
        b.* = smith.valueRangeAtMost(u8, 32, 126); // printable ASCII
    }
    if (parse(testing.allocator, buf[0..len])) |sel| {
        defer sel.deinit(testing.allocator);
    } else |_| {}
}
