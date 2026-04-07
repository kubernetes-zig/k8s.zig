const std = @import("std");
const mem = std.mem;
const testing = std.testing;

/// Parsed quantity value as a scaled integer.
/// Internally represented as value * 10^scale to avoid floating point.
pub const Quantity = struct {
    /// The mantissa. For "1500m", this is 1500. For "1Gi", this is 1.
    mantissa: i64,
    /// The scale as power of 10 (for decimal) or special values for binary.
    /// For "1500m": mantissa=1500, suffix=.milli
    /// For "1Gi": mantissa=1, suffix=.gibi
    suffix: Suffix,

    pub const Suffix = enum {
        // Decimal SI
        nano,   // n  = 10^-9
        micro,  // u  = 10^-6
        milli,  // m  = 10^-3
        none,   //    = 10^0
        kilo,   // k  = 10^3
        mega,   // M  = 10^6
        giga,   // G  = 10^9
        tera,   // T  = 10^12
        peta,   // P  = 10^15
        exa,    // E  = 10^18

        // Binary
        kibi,   // Ki = 2^10
        mebi,   // Mi = 2^20
        gibi,   // Gi = 2^30
        tebi,   // Ti = 2^40
        pebi,   // Pi = 2^50
        exbi,   // Ei = 2^60

        pub fn multiplier(self: Suffix) i64 {
            return switch (self) {
                .nano => 1, // special: divide by 10^9
                .micro => 1, // special: divide by 10^6
                .milli => 1, // special: divide by 10^3
                .none => 1,
                .kilo => 1_000,
                .mega => 1_000_000,
                .giga => 1_000_000_000,
                .tera => 1_000_000_000_000,
                .peta => 1_000_000_000_000_000,
                .exa => 1_000_000_000_000_000_000,
                .kibi => 1_024,
                .mebi => 1_048_576,
                .gibi => 1_073_741_824,
                .tebi => 1_099_511_627_776,
                .pebi => 1_125_899_906_842_624,
                .exbi => 1_152_921_504_606_846_976,
            };
        }

        pub fn divisor(self: Suffix) i64 {
            return switch (self) {
                .nano => 1_000_000_000,
                .micro => 1_000_000,
                .milli => 1_000,
                else => 1,
            };
        }
    };

    /// Parse a K8s quantity string like "100m", "1Gi", "500", "2.5".
    pub fn parse(s: []const u8) !Quantity {
        if (s.len == 0) return error.InvalidQuantity;

        // Find where the number ends and suffix begins
        var num_end: usize = s.len;
        for (s, 0..) |c, i| {
            if (!isNumChar(c)) {
                num_end = i;
                break;
            }
        }

        if (num_end == 0) return error.InvalidQuantity;

        const num_str = s[0..num_end];
        const suffix_str = s[num_end..];

        const suffix = parseSuffix(suffix_str) orelse return error.InvalidQuantity;

        // Handle decimal point — convert to milli-units to preserve precision.
        // e.g., "1.5" → mantissa=1500, suffix=milli
        //        "0.5" → mantissa=500, suffix=milli
        //        "2.5Gi" is not valid K8s — binary suffixes don't mix with decimals
        if (mem.indexOfScalar(u8, num_str, '.')) |dot| {
            if (suffix != .none) return error.InvalidQuantity; // decimals only with no suffix or decimal SI
            const int_part = std.fmt.parseInt(i64, num_str[0..dot], 10) catch return error.InvalidQuantity;
            const frac_str = num_str[dot + 1 ..];
            if (frac_str.len == 0) return error.InvalidQuantity;
            const frac_val = std.fmt.parseInt(i64, frac_str, 10) catch return error.InvalidQuantity;

            // Normalize to milli: "1.5" → 1500m, "0.001" → 1m
            var frac_millis: i64 = frac_val;
            var digits = frac_str.len;
            // Scale frac to 3 decimal places (millis)
            while (digits < 3) : (digits += 1) frac_millis *= 10;
            while (digits > 3) : (digits -= 1) frac_millis = @divTrunc(frac_millis, 10);

            const sign: i64 = if (int_part < 0) -1 else 1;
            const abs_int = if (int_part < 0) -int_part else int_part;
            return .{ .mantissa = sign * (abs_int * 1000 + frac_millis), .suffix = .milli };
        }

        const mantissa = std.fmt.parseInt(i64, num_str, 10) catch return error.InvalidQuantity;
        return .{ .mantissa = mantissa, .suffix = suffix };
    }

    /// Convert to a base-unit integer value (lossy for sub-unit quantities).
    /// e.g., "1Gi" → 1073741824, "500m" → 0, "1500m" → 1
    /// Returns null on overflow.
    pub fn asInt(self: Quantity) ?i64 {
        const mul = std.math.mul(i64, self.mantissa, self.suffix.multiplier()) catch return null;
        return @divTrunc(mul, self.suffix.divisor());
    }

    /// Convert to milli-units for CPU-style quantities.
    /// e.g., "1" → 1000, "500m" → 500, "1500m" → 1500
    /// Returns null on overflow.
    pub fn asMillis(self: Quantity) ?i64 {
        return switch (self.suffix) {
            .milli => self.mantissa,
            .none => std.math.mul(i64, self.mantissa, 1000) catch return null,
            else => {
                const scaled = std.math.mul(i64, self.suffix.multiplier(), 1000) catch return null;
                return std.math.mul(i64, self.mantissa, @divTrunc(scaled, self.suffix.divisor())) catch return null;
            },
        };
    }

    /// Format back to a K8s quantity string.
    pub fn format(self: Quantity, buf: []u8) ![]const u8 {
        const suffix_str = formatSuffix(self.suffix);
        return std.fmt.bufPrint(buf, "{d}{s}", .{ self.mantissa, suffix_str });
    }
};

fn isNumChar(c: u8) bool {
    return (c >= '0' and c <= '9') or c == '.' or c == '+' or c == '-';
}

fn parseSuffix(s: []const u8) ?Quantity.Suffix {
    if (s.len == 0) return .none;
    if (mem.eql(u8, s, "n")) return .nano;
    if (mem.eql(u8, s, "u")) return .micro;
    if (mem.eql(u8, s, "m")) return .milli;
    if (mem.eql(u8, s, "k")) return .kilo;
    if (mem.eql(u8, s, "M")) return .mega;
    if (mem.eql(u8, s, "G")) return .giga;
    if (mem.eql(u8, s, "T")) return .tera;
    if (mem.eql(u8, s, "P")) return .peta;
    if (mem.eql(u8, s, "E")) return .exa;
    if (mem.eql(u8, s, "Ki")) return .kibi;
    if (mem.eql(u8, s, "Mi")) return .mebi;
    if (mem.eql(u8, s, "Gi")) return .gibi;
    if (mem.eql(u8, s, "Ti")) return .tebi;
    if (mem.eql(u8, s, "Pi")) return .pebi;
    if (mem.eql(u8, s, "Ei")) return .exbi;
    return null;
}

fn formatSuffix(s: Quantity.Suffix) []const u8 {
    return switch (s) {
        .nano => "n",
        .micro => "u",
        .milli => "m",
        .none => "",
        .kilo => "k",
        .mega => "M",
        .giga => "G",
        .tera => "T",
        .peta => "P",
        .exa => "E",
        .kibi => "Ki",
        .mebi => "Mi",
        .gibi => "Gi",
        .tebi => "Ti",
        .pebi => "Pi",
        .exbi => "Ei",
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "parse: decimal SI suffixes" {
    const cases = .{
        .{ "100m", @as(i64, 100), Quantity.Suffix.milli },
        .{ "500m", @as(i64, 500), Quantity.Suffix.milli },
        .{ "1", @as(i64, 1), Quantity.Suffix.none },
        .{ "100", @as(i64, 100), Quantity.Suffix.none },
        .{ "1k", @as(i64, 1), Quantity.Suffix.kilo },
        .{ "1M", @as(i64, 1), Quantity.Suffix.mega },
        .{ "1G", @as(i64, 1), Quantity.Suffix.giga },
        .{ "100n", @as(i64, 100), Quantity.Suffix.nano },
    };
    inline for (cases) |c| {
        const q = try Quantity.parse(c[0]);
        try testing.expectEqual(c[1], q.mantissa);
        try testing.expectEqual(c[2], q.suffix);
    }
}

test "parse: binary suffixes" {
    const cases = .{
        .{ "1Ki", @as(i64, 1), Quantity.Suffix.kibi },
        .{ "1Mi", @as(i64, 1), Quantity.Suffix.mebi },
        .{ "1Gi", @as(i64, 1), Quantity.Suffix.gibi },
        .{ "512Mi", @as(i64, 512), Quantity.Suffix.mebi },
    };
    inline for (cases) |c| {
        const q = try Quantity.parse(c[0]);
        try testing.expectEqual(c[1], q.mantissa);
        try testing.expectEqual(c[2], q.suffix);
    }
}

test "asInt: conversion to base units" {
    const cases = .{
        .{ "1Gi", @as(i64, 1_073_741_824) },
        .{ "512Mi", @as(i64, 536_870_912) },
        .{ "1k", @as(i64, 1_000) },
        .{ "100", @as(i64, 100) },
        .{ "1500m", @as(i64, 1) },
        .{ "500m", @as(i64, 0) },
    };
    inline for (cases) |c| {
        const q = try Quantity.parse(c[0]);
        try testing.expectEqual(c[1], q.asInt().?);
    }
}

test "asMillis: CPU-style conversion" {
    const cases = .{
        .{ "1", @as(i64, 1000) },
        .{ "500m", @as(i64, 500) },
        .{ "1500m", @as(i64, 1500) },
        .{ "100m", @as(i64, 100) },
        .{ "250m", @as(i64, 250) },
        .{ "0.5", @as(i64, 500) },
        .{ "1.5", @as(i64, 1500) },
        .{ "0.001", @as(i64, 1) },
    };
    inline for (cases) |c| {
        const q = try Quantity.parse(c[0]);
        try testing.expectEqual(c[1], q.asMillis().?);
    }
}

test "asInt and asMillis: overflow returns null" {
    // Large mantissa with large multiplier overflows i64
    const large = Quantity{ .mantissa = std.math.maxInt(i64), .suffix = .gibi };
    try testing.expect(large.asInt() == null);

    const large_milli = Quantity{ .mantissa = std.math.maxInt(i64), .suffix = .none };
    try testing.expect(large_milli.asMillis() == null);

    // Normal values don't overflow
    const normal = Quantity{ .mantissa = 1, .suffix = .gibi };
    try testing.expectEqual(@as(i64, 1073741824), normal.asInt().?);

    const normal_milli = Quantity{ .mantissa = 1, .suffix = .none };
    try testing.expectEqual(@as(i64, 1000), normal_milli.asMillis().?);
}

test "format: roundtrip" {
    const cases = .{ "100m", "1Gi", "512Mi", "1k", "500" };
    inline for (cases) |c| {
        const q = try Quantity.parse(c);
        var buf: [64]u8 = undefined;
        const s = try q.format(&buf);
        try testing.expectEqualStrings(c, s);
    }
}

test "parse: invalid" {
    const cases = .{ "", "abc", "m", "Ki" };
    inline for (cases) |c| {
        try testing.expectError(error.InvalidQuantity, Quantity.parse(c));
    }
}

test "parse: negative quantities" {
    const cases = .{
        .{ "-100m", @as(i64, -100), Quantity.Suffix.milli },
        .{ "-1", @as(i64, -1), Quantity.Suffix.none },
        .{ "-500m", @as(i64, -500), Quantity.Suffix.milli },
        .{ "-1Gi", @as(i64, -1), Quantity.Suffix.gibi },
    };
    inline for (cases) |c| {
        const q = try Quantity.parse(c[0]);
        try testing.expectEqual(c[1], q.mantissa);
        try testing.expectEqual(c[2], q.suffix);
    }
}

test "asInt: overflow returns null" {
    const cases = .{
        Quantity{ .mantissa = std.math.maxInt(i64), .suffix = .gibi },
        Quantity{ .mantissa = std.math.maxInt(i64), .suffix = .mebi },
        Quantity{ .mantissa = std.math.maxInt(i64), .suffix = .kilo },
        Quantity{ .mantissa = std.math.maxInt(i64), .suffix = .mega },
        Quantity{ .mantissa = std.math.maxInt(i64), .suffix = .giga },
    };
    inline for (cases) |q| {
        try testing.expect(q.asInt() == null);
    }
}

test "asMillis: overflow returns null" {
    const cases = .{
        Quantity{ .mantissa = std.math.maxInt(i64), .suffix = .none },
        Quantity{ .mantissa = std.math.maxInt(i64), .suffix = .kilo },
        Quantity{ .mantissa = std.math.maxInt(i64), .suffix = .giga },
    };
    inline for (cases) |q| {
        try testing.expect(q.asMillis() == null);
    }
}

test "fuzz: Quantity.parse never crashes on arbitrary input" {
    try std.testing.fuzz({}, fuzzQuantityParse, .{});
}

fn fuzzQuantityParse(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    const len = smith.valueRangeAtMost(u8, 0, 64);
    var buf: [64]u8 = undefined;
    for (buf[0..len]) |*b| {
        b.* = smith.valueRangeAtMost(u8, 0, 127); // ASCII range
    }
    if (Quantity.parse(buf[0..len])) |q| {
        _ = q.asInt();
        _ = q.asMillis();
        var fmt_buf: [128]u8 = undefined;
        _ = q.format(&fmt_buf) catch {};
    } else |_| {}
}
