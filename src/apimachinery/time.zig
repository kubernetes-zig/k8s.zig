const std = @import("std");
const mem = std.mem;
const testing = std.testing;

/// Parsed K8s timestamp from RFC3339 format.
/// Stores as epoch seconds for comparison and arithmetic.
pub const Time = struct {
    /// Seconds since Unix epoch (1970-01-01T00:00:00Z).
    epoch_seconds: i64,

    /// The original string representation, if available.
    raw: ?[]const u8 = null,

    /// Parse an RFC3339 timestamp string like "2024-01-15T10:30:00Z".
    /// K8s only uses UTC (Z suffix), no timezone offsets.
    pub fn parse(s: []const u8) !Time {
        if (s.len < 20) return error.InvalidTimestamp;

        // Expected: "YYYY-MM-DDThh:mm:ssZ" (20 chars minimum)
        if (s[4] != '-' or s[7] != '-' or s[10] != 'T' or s[13] != ':' or s[16] != ':')
            return error.InvalidTimestamp;

        const year = std.fmt.parseInt(i32, s[0..4], 10) catch return error.InvalidTimestamp;
        const month = std.fmt.parseInt(u8, s[5..7], 10) catch return error.InvalidTimestamp;
        const day = std.fmt.parseInt(u8, s[8..10], 10) catch return error.InvalidTimestamp;
        const hour = std.fmt.parseInt(u8, s[11..13], 10) catch return error.InvalidTimestamp;
        const minute = std.fmt.parseInt(u8, s[14..16], 10) catch return error.InvalidTimestamp;
        const second = std.fmt.parseInt(u8, s[17..19], 10) catch return error.InvalidTimestamp;

        if (month < 1 or month > 12) return error.InvalidTimestamp;
        if (day < 1 or day > 31) return error.InvalidTimestamp;
        if (hour > 23) return error.InvalidTimestamp;
        if (minute > 59) return error.InvalidTimestamp;
        if (second > 59) return error.InvalidTimestamp;

        // Must end with Z (K8s always uses UTC)
        if (s[s.len - 1] != 'Z') return error.InvalidTimestamp;

        const epoch = toEpoch(year, month, day, hour, minute, second);
        return .{ .epoch_seconds = epoch, .raw = s };
    }

    /// Returns true if this time is zero (epoch 0 or null).
    pub fn isZero(self: Time) bool {
        return self.epoch_seconds == 0;
    }

    /// Returns true if `self` is before `other`.
    pub fn before(self: Time, other: Time) bool {
        return self.epoch_seconds < other.epoch_seconds;
    }

    /// Returns true if `self` is after `other`.
    pub fn after(self: Time, other: Time) bool {
        return self.epoch_seconds > other.epoch_seconds;
    }

    /// Returns true if both represent the same point in time.
    pub fn eql(self: Time, other: Time) bool {
        return self.epoch_seconds == other.epoch_seconds;
    }

    /// Duration in seconds between two times (self - other).
    pub fn diffSeconds(self: Time, other: Time) i64 {
        return self.epoch_seconds - other.epoch_seconds;
    }

    /// Format as RFC3339 UTC string.
    pub fn format(self: Time, buf: []u8) ![]const u8 {
        const s = fromEpoch(self.epoch_seconds);
        const year: u16 = @intCast(s.year);
        return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
            year, s.month, s.day, s.hour, s.minute, s.second,
        });
    }
};

const DateParts = struct {
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
};

fn isLeapYear(year: i32) bool {
    if (@mod(year, 400) == 0) return true;
    if (@mod(year, 100) == 0) return false;
    return @mod(year, 4) == 0;
}

fn daysInMonth(month: u8, year: i32) u8 {
    const days = [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (month < 1 or month > 12) return 30; // defensive: invalid month
    if (month == 2 and isLeapYear(year)) return 29;
    return days[month - 1];
}

fn toEpoch(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: u8) i64 {
    // Days from epoch (1970-01-01) to the given date
    var days: i64 = 0;

    // Years
    if (year > 1970) {
        var y: i32 = 1970;
        while (y < year) : (y += 1) {
            days += if (isLeapYear(y)) 366 else 365;
        }
    }

    // Months
    var m: u8 = 1;
    while (m < month) : (m += 1) {
        days += daysInMonth(m, year);
    }

    // Days (1-indexed)
    days += @as(i64, day) - 1;

    return days * 86400 + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
}

fn fromEpoch(epoch: i64) DateParts {
    var remaining = epoch;
    const day_seconds: i64 = 86400;

    var total_days = @divTrunc(remaining, day_seconds);
    remaining = @mod(remaining, day_seconds);

    const hour: u8 = @intCast(@divTrunc(remaining, 3600));
    remaining = @mod(remaining, 3600);
    const minute: u8 = @intCast(@divTrunc(remaining, 60));
    const second: u8 = @intCast(@mod(remaining, 60));

    // Calculate year
    var year: i32 = 1970;
    while (true) {
        const days_in_year: i64 = if (isLeapYear(year)) 366 else 365;
        if (total_days < days_in_year) break;
        total_days -= days_in_year;
        year += 1;
    }

    // Calculate month
    var month: u8 = 1;
    while (month < 12) {
        const dim: i64 = daysInMonth(month, year);
        if (total_days < dim) break;
        total_days -= dim;
        month += 1;
    }

    const day: u8 = @intCast(total_days + 1);

    return .{ .year = year, .month = month, .day = day, .hour = hour, .minute = minute, .second = second };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "parse: valid timestamps" {
    const cases = .{
        .{ "2024-01-01T00:00:00Z", @as(i64, 1704067200) },
        .{ "1970-01-01T00:00:00Z", @as(i64, 0) },
        .{ "2024-06-15T10:30:00Z", @as(i64, 1718447400) },
    };
    inline for (cases) |c| {
        const t = try Time.parse(c[0]);
        try testing.expectEqual(c[1], t.epoch_seconds);
    }
}

test "parse: invalid timestamps" {
    const cases = .{ "", "not-a-date", "2024-01-01", "2024-01-01T00:00:00" };
    inline for (cases) |c| {
        try testing.expectError(error.InvalidTimestamp, Time.parse(c));
    }
}

test "comparison" {
    const t1 = try Time.parse("2024-01-01T00:00:00Z");
    const t2 = try Time.parse("2024-06-15T10:30:00Z");
    const t3 = try Time.parse("2024-01-01T00:00:00Z");

    try testing.expect(t1.before(t2));
    try testing.expect(t2.after(t1));
    try testing.expect(t1.eql(t3));
    try testing.expect(!t1.eql(t2));
}

test "isZero" {
    const zero = try Time.parse("1970-01-01T00:00:00Z");
    const nonzero = try Time.parse("2024-01-01T00:00:00Z");
    try testing.expect(zero.isZero());
    try testing.expect(!nonzero.isZero());
}

test "diffSeconds" {
    const t1 = try Time.parse("2024-01-01T00:00:00Z");
    const t2 = try Time.parse("2024-01-01T01:00:00Z");
    try testing.expectEqual(@as(i64, 3600), t2.diffSeconds(t1));
    try testing.expectEqual(@as(i64, -3600), t1.diffSeconds(t2));
}

test "format: roundtrip" {
    const cases = .{
        "2024-01-01T00:00:00Z",
        "1970-01-01T00:00:00Z",
        "2024-06-15T10:30:00Z",
    };
    inline for (cases) |c| {
        const t = try Time.parse(c);
        var buf: [32]u8 = undefined;
        const formatted = try t.format(&buf);
        try testing.expectEqualStrings(c, formatted);
    }
}

test "parse: invalid timestamps table-driven" {
    const cases = .{
        "2024-00-15T10:30:00Z", // month=0
        "2024-13-15T10:30:00Z", // month=13
        "2024-01-00T10:30:00Z", // day=0
        "2024-01-32T10:30:00Z", // day=32
        "2024-01-15T24:30:00Z", // hour=24
        "2024-01-15T10:60:00Z", // minute=60
        "2024-01-15T10:30:60Z", // second=60
        "2024-01-15T10:30:0", // too short (19 chars, no Z)
        "2024-01-15T10:30:00X", // missing Z (wrong suffix)
        "2024/01/15T10:30:00Z", // wrong date separators
        "2024-01-15 10:30:00Z", // space instead of T
    };
    inline for (cases) |c| {
        try testing.expectError(error.InvalidTimestamp, Time.parse(c));
    }
}

test "format and parse roundtrip: table-driven" {
    const cases = .{
        .{ "2024-01-01T00:00:00Z", @as(i64, 1704067200) },
        .{ "1970-01-01T00:00:00Z", @as(i64, 0) },
        .{ "2024-06-15T10:30:00Z", @as(i64, 1718447400) },
        .{ "2000-02-29T12:00:00Z", @as(i64, 951825600) },
        .{ "2024-12-31T23:59:59Z", @as(i64, 1735689599) },
    };
    inline for (cases) |c| {
        const t = try Time.parse(c[0]);
        try testing.expectEqual(c[1], t.epoch_seconds);

        // Format back and re-parse — epoch must be unchanged
        var buf: [32]u8 = undefined;
        const formatted = try t.format(&buf);
        const t2 = try Time.parse(formatted);
        try testing.expectEqual(t.epoch_seconds, t2.epoch_seconds);
    }
}

test "fuzz: Time.parse never crashes on arbitrary input" {
    try std.testing.fuzz({}, fuzzTimeParse, .{});
}

fn fuzzTimeParse(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    const len = smith.valueRangeAtMost(u8, 0, 32);
    var buf: [32]u8 = undefined;
    for (buf[0..len]) |*b| {
        b.* = smith.valueRangeAtMost(u8, 0, 127);
    }
    if (Time.parse(buf[0..len])) |t| {
        var fmt_buf: [32]u8 = undefined;
        _ = t.format(&fmt_buf) catch {};
    } else |_| {}
}
