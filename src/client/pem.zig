//! PEM private-key normalization for client TLS auth.
//!
//! `ianic/tls.zig` only accepts PKCS#8 (`-----BEGIN PRIVATE KEY-----`) and
//! SEC1 EC (`-----BEGIN EC PRIVATE KEY-----`) private keys. Kubernetes tools
//! like `kubeadm`, `kind`, and older `kubectl` versions commonly ship PKCS#1
//! RSA keys (`-----BEGIN RSA PRIVATE KEY-----`). This module normalizes an
//! arbitrary private-key PEM into a form that `tls.zig` accepts by wrapping
//! PKCS#1 RSA keys in a PKCS#8 `PrivateKeyInfo` structure — pure ASN.1
//! manipulation, no key material parsing.
//!
//! Unsupported inputs (encrypted keys, DSA, OpenSSH) fail with a descriptive
//! error rather than a cryptic `error.MissingEndMarker` deep in the TLS
//! handshake.

const std = @import("std");
const mem = std.mem;
const ascii = std.ascii;
const testing = std.testing;
const Allocator = mem.Allocator;

const base64_decoder = std.base64.standard.decoderWithIgnore(" \t\r\n");
const base64_encoder = std.base64.standard.Encoder;

/// Canonical PEM labels for private-key block types.
pub const label_pkcs8: []const u8 = "PRIVATE KEY";
pub const label_pkcs8_encrypted: []const u8 = "ENCRYPTED PRIVATE KEY";
pub const label_pkcs1_rsa: []const u8 = "RSA PRIVATE KEY";
pub const label_sec1_ec: []const u8 = "EC PRIVATE KEY";
pub const label_dsa: []const u8 = "DSA PRIVATE KEY";
pub const label_openssh: []const u8 = "OPENSSH PRIVATE KEY";

/// Private-key format classified from its PEM label.
pub const KeyFormat = enum {
    /// PKCS#8 unencrypted. Accepted by tls.zig directly.
    pkcs8,
    /// PKCS#8 encrypted — `EncryptedPrivateKeyInfo`. Not supported.
    pkcs8_encrypted,
    /// PKCS#1 `RSAPrivateKey` DER. Must be wrapped in PKCS#8.
    pkcs1_rsa,
    /// SEC1 `ECPrivateKey` DER. Accepted by tls.zig directly.
    sec1_ec,
    /// DSA private key. Not usable for modern TLS.
    dsa,
    /// OpenSSH private key format. Not a TLS key.
    openssh,
    /// Label recognized as a PEM block but not a known private-key type.
    unknown,

    pub fn fromLabel(label: []const u8) KeyFormat {
        if (mem.eql(u8, label, label_pkcs8)) return .pkcs8;
        if (mem.eql(u8, label, label_pkcs8_encrypted)) return .pkcs8_encrypted;
        if (mem.eql(u8, label, label_pkcs1_rsa)) return .pkcs1_rsa;
        if (mem.eql(u8, label, label_sec1_ec)) return .sec1_ec;
        if (mem.eql(u8, label, label_dsa)) return .dsa;
        if (mem.eql(u8, label, label_openssh)) return .openssh;
        return .unknown;
    }
};

pub const Error = error{
    /// PEM envelope is malformed: missing BEGIN/END armor, mismatched labels,
    /// truncated block, or invalid base64 body.
    InvalidPem,
    /// No private-key PEM block found in the input (e.g., only certificates).
    NoPrivateKey,
    /// Legacy PKCS#1/PEM encryption (`Proc-Type: 4,ENCRYPTED`) or PKCS#8
    /// `EncryptedPrivateKeyInfo`. We would need to prompt for a passphrase —
    /// unsupported for kubeconfig auth.
    EncryptedKeyNotSupported,
    /// Recognized label but not usable for TLS client auth (DSA, OpenSSH,
    /// unknown labels).
    UnsupportedKeyFormat,
} || Allocator.Error;

/// Normalize a PEM-encoded private key so that `ianic/tls.zig` can parse it.
///
/// - PKCS#8 and SEC1 EC keys are re-emitted with canonical whitespace.
/// - PKCS#1 RSA keys are losslessly wrapped in a PKCS#8 `PrivateKeyInfo`.
/// - Encrypted or unsupported formats return a descriptive error.
///
/// Accepts multi-block PEMs (e.g. a concatenated cert chain plus key); the
/// first private-key block wins. Returns a newly-allocated PEM — caller owns.
pub fn normalizePrivateKey(allocator: Allocator, pem: []const u8) Error![]u8 {
    const block = try findPrivateKeyBlock(allocator, pem);
    defer allocator.free(block.der);

    if (block.encrypted_header) return error.EncryptedKeyNotSupported;

    return switch (KeyFormat.fromLabel(block.label)) {
        .pkcs8 => try encodePem(allocator, label_pkcs8, block.der),
        .sec1_ec => try encodePem(allocator, label_sec1_ec, block.der),
        .pkcs1_rsa => blk: {
            const pkcs8_der = try wrapPkcs1InPkcs8(allocator, block.der);
            defer allocator.free(pkcs8_der);
            break :blk try encodePem(allocator, label_pkcs8, pkcs8_der);
        },
        .pkcs8_encrypted => error.EncryptedKeyNotSupported,
        .dsa, .openssh, .unknown => error.UnsupportedKeyFormat,
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// PEM parser
// ─────────────────────────────────────────────────────────────────────────────

const ParsedBlock = struct {
    /// Label text — a slice into the input PEM. Does not outlive it.
    label: []const u8,
    /// Decoded DER body, heap-allocated. Caller owns.
    der: []u8,
    /// True if the PEM has `Proc-Type: 4,ENCRYPTED` or `DEK-Info:` headers
    /// (legacy OpenSSL PKCS#1 encryption).
    encrypted_header: bool,
};

/// Scan `pem` for the first block whose label ends with `PRIVATE KEY` and
/// decode its base64 body to DER. Non-private-key blocks (certificates, etc.)
/// are skipped.
fn findPrivateKeyBlock(allocator: Allocator, pem: []const u8) Error!ParsedBlock {
    const begin_prefix = "-----BEGIN ";
    const armor_suffix = "-----";

    var cursor: usize = 0;
    while (cursor < pem.len) {
        const begin_start = mem.indexOfPos(u8, pem, cursor, begin_prefix) orelse
            return error.NoPrivateKey;
        const label_start = begin_start + begin_prefix.len;
        const label_end = mem.indexOfPos(u8, pem, label_start, armor_suffix) orelse
            return error.InvalidPem;
        // Raw label preserves any interior whitespace — used verbatim to
        // build the matching END needle. The trimmed form is what we return
        // for classification, tolerant of tools that emit stray space around
        // the label.
        const label_raw = pem[label_start..label_end];
        const label = mem.trim(u8, label_raw, " \t");
        const after_begin = label_end + armor_suffix.len;

        // Build the matching `-----END <label>-----` needle.
        var end_needle_buf: [96]u8 = undefined;
        if (label_raw.len + "-----END -----".len > end_needle_buf.len)
            return error.InvalidPem;
        const end_needle = std.fmt.bufPrint(&end_needle_buf, "-----END {s}-----", .{label_raw}) catch
            return error.InvalidPem;
        const end_start = mem.indexOfPos(u8, pem, after_begin, end_needle) orelse
            return error.InvalidPem;

        if (!mem.endsWith(u8, label, "PRIVATE KEY")) {
            // Skip non-private-key block (cert chain, public key, etc.).
            cursor = end_start + end_needle.len;
            continue;
        }

        // Body starts after the BEGIN line's newline.
        var body_start = after_begin;
        if (body_start < pem.len and pem[body_start] == '\r') body_start += 1;
        if (body_start < pem.len and pem[body_start] == '\n') body_start += 1;

        const body_slice = pem[body_start..end_start];

        // Optional RFC 1421 header block (Proc-Type, DEK-Info, …). Terminated
        // by a blank line. If the first line is not a header, treat the whole
        // body as base64.
        var encrypted_header = false;
        var body_cursor: usize = 0;
        if (looksLikePemHeader(body_slice)) {
            while (body_cursor < body_slice.len) {
                const line_end = mem.indexOfScalarPos(u8, body_slice, body_cursor, '\n') orelse break;
                const line = trimCr(body_slice[body_cursor..line_end]);
                body_cursor = line_end + 1;
                if (line.len == 0) break;
                if (lineStartsWithIgnoreCase(line, "Proc-Type:") or
                    lineStartsWithIgnoreCase(line, "DEK-Info:"))
                {
                    encrypted_header = true;
                }
            }
        }

        const b64_body = body_slice[body_cursor..];

        // Upper bound: decoderWithIgnore strips whitespace, so decoded length
        // ≤ b64_body.len. Add slack for padding math.
        const upper_bound = (b64_body.len / 4 + 2) * 3;
        const buf = try allocator.alloc(u8, upper_bound);
        errdefer allocator.free(buf);

        const decoded_len = base64_decoder.decode(buf, b64_body) catch
            return error.InvalidPem;
        // A zero-byte private-key body is never legitimate — reject rather
        // than silently emit a structurally-corrupt DER down the pipeline.
        if (decoded_len == 0) return error.InvalidPem;
        const der = try allocator.realloc(buf, decoded_len);

        return .{ .label = label, .der = der, .encrypted_header = encrypted_header };
    }
    return error.NoPrivateKey;
}

fn trimCr(line: []const u8) []const u8 {
    if (line.len > 0 and line[line.len - 1] == '\r') return line[0 .. line.len - 1];
    return line;
}

fn lineStartsWithIgnoreCase(line: []const u8, prefix: []const u8) bool {
    if (line.len < prefix.len) return false;
    return ascii.eqlIgnoreCase(line[0..prefix.len], prefix);
}

/// A PEM body starts with RFC 1421 headers if the first non-empty line
/// contains a `:` and no `/` (base64 alphabet does not include `:`).
fn looksLikePemHeader(body: []const u8) bool {
    const line_end = mem.indexOfScalar(u8, body, '\n') orelse body.len;
    const first = trimCr(body[0..line_end]);
    if (first.len == 0) return false;
    return mem.indexOfScalar(u8, first, ':') != null;
}

// ─────────────────────────────────────────────────────────────────────────────
// PEM encoder
// ─────────────────────────────────────────────────────────────────────────────

const pem_line_width: usize = 64;

/// Encode DER bytes as a canonical PEM block with 64-char line wrapping.
fn encodePem(allocator: Allocator, label: []const u8, der: []const u8) ![]u8 {
    const b64_len = base64_encoder.calcSize(der.len);
    const num_lines = (b64_len + pem_line_width - 1) / pem_line_width;
    const header_fixed = "-----BEGIN -----\n".len;
    const footer_fixed = "-----END -----\n".len;
    const total = header_fixed + label.len + b64_len + num_lines + footer_fixed + label.len;

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var pos: usize = 0;
    const header = std.fmt.bufPrint(buf[pos..], "-----BEGIN {s}-----\n", .{label}) catch unreachable;
    pos += header.len;

    const b64_scratch = try allocator.alloc(u8, b64_len);
    defer allocator.free(b64_scratch);
    _ = base64_encoder.encode(b64_scratch, der);

    var line_start: usize = 0;
    while (line_start < b64_scratch.len) {
        const line_end = @min(line_start + pem_line_width, b64_scratch.len);
        const chunk = b64_scratch[line_start..line_end];
        @memcpy(buf[pos..][0..chunk.len], chunk);
        pos += chunk.len;
        buf[pos] = '\n';
        pos += 1;
        line_start = line_end;
    }

    const footer = std.fmt.bufPrint(buf[pos..], "-----END {s}-----\n", .{label}) catch unreachable;
    pos += footer.len;

    std.debug.assert(pos <= total);
    return try allocator.realloc(buf, pos);
}

// ─────────────────────────────────────────────────────────────────────────────
// PKCS#1 → PKCS#8 DER wrapping
// ─────────────────────────────────────────────────────────────────────────────
//
// PKCS#8 PrivateKeyInfo ::= SEQUENCE {
//     version                   INTEGER (0),
//     privateKeyAlgorithm       AlgorithmIdentifier,
//     privateKey                OCTET STRING
// }
// AlgorithmIdentifier ::= SEQUENCE {
//     algorithm                 OBJECT IDENTIFIER,
//     parameters                ANY OPTIONAL
// }
//
// For RSA: algorithm = rsaEncryption (1.2.840.113549.1.1.1), parameters = NULL.
// The algorithm-identifier SEQUENCE is a fixed 15-byte DER encoding, and the
// version field is a fixed 3-byte INTEGER 0. We copy the PKCS#1 blob verbatim
// into an OCTET STRING — no key parsing required.

const rsa_algorithm_identifier = [_]u8{
    0x30, 0x0d, // SEQUENCE (len = 13)
    0x06, 0x09, // OBJECT IDENTIFIER (len = 9)
    0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, // 1.2.840.113549.1.1.1
    0x05, 0x00, // NULL
};
const pkcs8_version_zero = [_]u8{ 0x02, 0x01, 0x00 };

/// Wrap a `RSAPrivateKey` (PKCS#1) DER blob in a `PrivateKeyInfo` (PKCS#8)
/// structure. Returns newly-allocated DER; caller owns.
///
/// **Precondition:** `pkcs1_der` must be a valid PKCS#1 `RSAPrivateKey` DER
/// blob. This function performs no validation — it copies the bytes verbatim
/// into an `OCTET STRING`. A malformed input produces structurally-corrupt
/// PKCS#8 that will fail downstream in the TLS key parser.
fn wrapPkcs1InPkcs8(allocator: Allocator, pkcs1_der: []const u8) ![]u8 {
    const octet_header_len = derHeaderLen(pkcs1_der.len);
    const inner_len = pkcs8_version_zero.len + rsa_algorithm_identifier.len +
        octet_header_len + pkcs1_der.len;

    const outer_header_len = derHeaderLen(inner_len);
    const total = outer_header_len + inner_len;

    const out = try allocator.alloc(u8, total);
    errdefer allocator.free(out);

    var pos: usize = 0;
    pos += writeDerHeader(out[pos..], 0x30, inner_len);
    @memcpy(out[pos..][0..pkcs8_version_zero.len], &pkcs8_version_zero);
    pos += pkcs8_version_zero.len;
    @memcpy(out[pos..][0..rsa_algorithm_identifier.len], &rsa_algorithm_identifier);
    pos += rsa_algorithm_identifier.len;
    pos += writeDerHeader(out[pos..], 0x04, pkcs1_der.len);
    @memcpy(out[pos..][0..pkcs1_der.len], pkcs1_der);
    pos += pkcs1_der.len;

    std.debug.assert(pos == total);
    return out;
}

/// Number of bytes a DER tag+length header uses to encode `len` content bytes.
fn derHeaderLen(len: usize) usize {
    if (len < 0x80) return 2;
    if (len <= 0xff) return 3;
    if (len <= 0xffff) return 4;
    if (len <= 0xffffff) return 5;
    return 6;
}

/// Write DER tag + length into `buf`. Returns number of bytes written.
fn writeDerHeader(buf: []u8, tag: u8, len: usize) usize {
    buf[0] = tag;
    if (len < 0x80) {
        buf[1] = @intCast(len);
        return 2;
    }
    if (len <= 0xff) {
        buf[1] = 0x81;
        buf[2] = @intCast(len);
        return 3;
    }
    if (len <= 0xffff) {
        buf[1] = 0x82;
        buf[2] = @intCast((len >> 8) & 0xff);
        buf[3] = @intCast(len & 0xff);
        return 4;
    }
    if (len <= 0xffffff) {
        buf[1] = 0x83;
        buf[2] = @intCast((len >> 16) & 0xff);
        buf[3] = @intCast((len >> 8) & 0xff);
        buf[4] = @intCast(len & 0xff);
        return 5;
    }
    buf[1] = 0x84;
    buf[2] = @intCast((len >> 24) & 0xff);
    buf[3] = @intCast((len >> 16) & 0xff);
    buf[4] = @intCast((len >> 8) & 0xff);
    buf[5] = @intCast(len & 0xff);
    return 6;
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "KeyFormat.fromLabel: table-driven" {
    const Case = struct { label: []const u8, expected: KeyFormat };
    const cases = [_]Case{
        .{ .label = "PRIVATE KEY", .expected = .pkcs8 },
        .{ .label = "ENCRYPTED PRIVATE KEY", .expected = .pkcs8_encrypted },
        .{ .label = "RSA PRIVATE KEY", .expected = .pkcs1_rsa },
        .{ .label = "EC PRIVATE KEY", .expected = .sec1_ec },
        .{ .label = "DSA PRIVATE KEY", .expected = .dsa },
        .{ .label = "OPENSSH PRIVATE KEY", .expected = .openssh },
        .{ .label = "CERTIFICATE", .expected = .unknown },
        .{ .label = "PUBLIC KEY", .expected = .unknown },
        .{ .label = "", .expected = .unknown },
    };
    for (cases) |c| try testing.expectEqual(c.expected, KeyFormat.fromLabel(c.label));
}

test "derHeaderLen: boundary cases" {
    try testing.expectEqual(@as(usize, 2), derHeaderLen(0));
    try testing.expectEqual(@as(usize, 2), derHeaderLen(127));
    try testing.expectEqual(@as(usize, 3), derHeaderLen(128));
    try testing.expectEqual(@as(usize, 3), derHeaderLen(255));
    try testing.expectEqual(@as(usize, 4), derHeaderLen(256));
    try testing.expectEqual(@as(usize, 4), derHeaderLen(65535));
    try testing.expectEqual(@as(usize, 5), derHeaderLen(65536));
    try testing.expectEqual(@as(usize, 5), derHeaderLen(0xffffff));
    try testing.expectEqual(@as(usize, 6), derHeaderLen(0x1000000));
}

test "writeDerHeader: short + long form" {
    var buf: [8]u8 = undefined;

    try testing.expectEqual(@as(usize, 2), writeDerHeader(&buf, 0x30, 0));
    try testing.expectEqualSlices(u8, &[_]u8{ 0x30, 0x00 }, buf[0..2]);

    try testing.expectEqual(@as(usize, 2), writeDerHeader(&buf, 0x04, 127));
    try testing.expectEqualSlices(u8, &[_]u8{ 0x04, 0x7f }, buf[0..2]);

    try testing.expectEqual(@as(usize, 3), writeDerHeader(&buf, 0x04, 128));
    try testing.expectEqualSlices(u8, &[_]u8{ 0x04, 0x81, 0x80 }, buf[0..3]);

    try testing.expectEqual(@as(usize, 3), writeDerHeader(&buf, 0x30, 255));
    try testing.expectEqualSlices(u8, &[_]u8{ 0x30, 0x81, 0xff }, buf[0..3]);

    try testing.expectEqual(@as(usize, 4), writeDerHeader(&buf, 0x30, 1190));
    try testing.expectEqualSlices(u8, &[_]u8{ 0x30, 0x82, 0x04, 0xa6 }, buf[0..4]);

    try testing.expectEqual(@as(usize, 5), writeDerHeader(&buf, 0x30, 0xffffff));
    try testing.expectEqualSlices(u8, &[_]u8{ 0x30, 0x83, 0xff, 0xff, 0xff }, buf[0..5]);
}

test "wrapPkcs1InPkcs8: structure is correct" {
    // Fake PKCS#1 blob — wrapping doesn't parse the contents.
    const pkcs1 = [_]u8{ 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe };
    const pkcs8 = try wrapPkcs1InPkcs8(testing.allocator, &pkcs1);
    defer testing.allocator.free(pkcs8);

    // Walk the output and verify every field.
    var pos: usize = 0;
    try testing.expectEqual(@as(u8, 0x30), pkcs8[pos]);
    pos += 1;
    const outer_len = try readDerLength(pkcs8, &pos);
    try testing.expectEqual(pkcs8.len - pos, outer_len);

    // version INTEGER 0
    try testing.expectEqualSlices(u8, &[_]u8{ 0x02, 0x01, 0x00 }, pkcs8[pos .. pos + 3]);
    pos += 3;

    // algorithm identifier (rsaEncryption + NULL)
    try testing.expectEqualSlices(u8, &rsa_algorithm_identifier, pkcs8[pos .. pos + 15]);
    pos += 15;

    // OCTET STRING wrapping the PKCS#1 blob verbatim
    try testing.expectEqual(@as(u8, 0x04), pkcs8[pos]);
    pos += 1;
    const oct_len = try readDerLength(pkcs8, &pos);
    try testing.expectEqual(pkcs1.len, oct_len);
    try testing.expectEqualSlices(u8, &pkcs1, pkcs8[pos .. pos + oct_len]);
    try testing.expectEqual(pkcs8.len, pos + oct_len);
}

test "wrapPkcs1InPkcs8: length boundaries for short + long form" {
    // 200-byte payload: inner content fits in a single length byte (0x81 + 1).
    {
        const pkcs1 = [_]u8{0xaa} ** 200;
        const pkcs8 = try wrapPkcs1InPkcs8(testing.allocator, &pkcs1);
        defer testing.allocator.free(pkcs8);
        try testing.expectEqual(@as(u8, 0x30), pkcs8[0]);
        try testing.expectEqual(@as(u8, 0x81), pkcs8[1]);
    }
    // 300-byte payload: inner content needs 2 length bytes (0x82 + 2).
    {
        const pkcs1 = [_]u8{0xbb} ** 300;
        const pkcs8 = try wrapPkcs1InPkcs8(testing.allocator, &pkcs1);
        defer testing.allocator.free(pkcs8);
        try testing.expectEqual(@as(u8, 0x30), pkcs8[0]);
        try testing.expectEqual(@as(u8, 0x82), pkcs8[1]);
    }
}

test "wrapPkcs1InPkcs8: realistic RSA-2048 sized blob" {
    // Typical RSA-2048 PKCS#1 blob is ~1190 bytes.
    const pkcs1 = [_]u8{0x55} ** 1190;
    const pkcs8 = try wrapPkcs1InPkcs8(testing.allocator, &pkcs1);
    defer testing.allocator.free(pkcs8);

    var pos: usize = 1;
    const outer_len = try readDerLength(pkcs8, &pos);
    try testing.expectEqual(pkcs8.len - pos, outer_len);
    try testing.expectEqualSlices(u8, &[_]u8{ 0x02, 0x01, 0x00 }, pkcs8[pos .. pos + 3]);
}

test "encodePem: round-trip with 64-char line wrapping" {
    const der = [_]u8{0xff} ** 100;
    const pem = try encodePem(testing.allocator, label_pkcs8, &der);
    defer testing.allocator.free(pem);

    try testing.expect(mem.startsWith(u8, pem, "-----BEGIN PRIVATE KEY-----\n"));
    try testing.expect(mem.endsWith(u8, pem, "-----END PRIVATE KEY-----\n"));

    // Verify 64-char lines between header and footer.
    const body_start = "-----BEGIN PRIVATE KEY-----\n".len;
    const footer_start = pem.len - "-----END PRIVATE KEY-----\n".len;
    const body = pem[body_start..footer_start];
    var line_it = mem.splitScalar(u8, body, '\n');
    while (line_it.next()) |line| {
        if (line.len == 0) continue;
        try testing.expect(line.len <= pem_line_width);
    }

    // Round-trip: parse the produced PEM back and verify DER matches.
    const parsed = try findPrivateKeyBlock(testing.allocator, pem);
    defer testing.allocator.free(parsed.der);
    try testing.expectEqualStrings(label_pkcs8, parsed.label);
    try testing.expectEqualSlices(u8, &der, parsed.der);
}

test "findPrivateKeyBlock: skips certificate chain" {
    const pem =
        "-----BEGIN CERTIFICATE-----\n" ++
        "Zm9v\n" ++
        "-----END CERTIFICATE-----\n" ++
        "-----BEGIN PRIVATE KEY-----\n" ++
        "YmFy\n" ++
        "-----END PRIVATE KEY-----\n";
    const parsed = try findPrivateKeyBlock(testing.allocator, pem);
    defer testing.allocator.free(parsed.der);
    try testing.expectEqualStrings("PRIVATE KEY", parsed.label);
    try testing.expectEqualSlices(u8, "bar", parsed.der);
}

test "findPrivateKeyBlock: CRLF line endings" {
    const pem =
        "-----BEGIN RSA PRIVATE KEY-----\r\n" ++
        "YmF6\r\n" ++
        "-----END RSA PRIVATE KEY-----\r\n";
    const parsed = try findPrivateKeyBlock(testing.allocator, pem);
    defer testing.allocator.free(parsed.der);
    try testing.expectEqualStrings("RSA PRIVATE KEY", parsed.label);
    try testing.expectEqualSlices(u8, "baz", parsed.der);
}

test "findPrivateKeyBlock: detects encrypted header (Proc-Type/DEK-Info)" {
    const pem =
        "-----BEGIN RSA PRIVATE KEY-----\n" ++
        "Proc-Type: 4,ENCRYPTED\n" ++
        "DEK-Info: AES-256-CBC,0123456789ABCDEF\n" ++
        "\n" ++
        "Zm9vYmFy\n" ++
        "-----END RSA PRIVATE KEY-----\n";
    const parsed = try findPrivateKeyBlock(testing.allocator, pem);
    defer testing.allocator.free(parsed.der);
    try testing.expect(parsed.encrypted_header);
    try testing.expectEqualSlices(u8, "foobar", parsed.der);
}

test "findPrivateKeyBlock: no private key" {
    const pem =
        "-----BEGIN CERTIFICATE-----\n" ++
        "Zm9v\n" ++
        "-----END CERTIFICATE-----\n";
    try testing.expectError(error.NoPrivateKey, findPrivateKeyBlock(testing.allocator, pem));
}

test "findPrivateKeyBlock: empty input" {
    try testing.expectError(error.NoPrivateKey, findPrivateKeyBlock(testing.allocator, ""));
}

test "findPrivateKeyBlock: mismatched end label" {
    const pem =
        "-----BEGIN PRIVATE KEY-----\n" ++
        "Zm9v\n" ++
        "-----END RSA PRIVATE KEY-----\n";
    try testing.expectError(error.InvalidPem, findPrivateKeyBlock(testing.allocator, pem));
}

test "findPrivateKeyBlock: invalid base64 body" {
    const pem =
        "-----BEGIN PRIVATE KEY-----\n" ++
        "not valid base64!!!\n" ++
        "-----END PRIVATE KEY-----\n";
    try testing.expectError(error.InvalidPem, findPrivateKeyBlock(testing.allocator, pem));
}

test "findPrivateKeyBlock: empty body rejected" {
    // A private-key block with zero bytes decoded is never legitimate — must
    // fail with InvalidPem rather than silently emitting corrupt DER.
    const cases = [_][]const u8{
        "-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----\n",
        "-----BEGIN PRIVATE KEY-----\n\n-----END PRIVATE KEY-----\n",
        "-----BEGIN RSA PRIVATE KEY-----\n  \n\t\n-----END RSA PRIVATE KEY-----\n",
    };
    for (cases) |pem| {
        try testing.expectError(error.InvalidPem, findPrivateKeyBlock(testing.allocator, pem));
        try testing.expectError(error.InvalidPem, normalizePrivateKey(testing.allocator, pem));
    }
}

test "findPrivateKeyBlock: label with leading/trailing whitespace is trimmed" {
    // Some lenient tools emit extra spaces in the label. Classification must
    // succeed by trimming; the END line is matched verbatim against the raw
    // label text.
    const pem =
        "-----BEGIN  PRIVATE KEY -----\n" ++
        "Zm9v\n" ++
        "-----END  PRIVATE KEY -----\n";
    const parsed = try findPrivateKeyBlock(testing.allocator, pem);
    defer testing.allocator.free(parsed.der);
    try testing.expectEqualStrings("PRIVATE KEY", parsed.label);
    try testing.expectEqualSlices(u8, "foo", parsed.der);
}

test "normalizePrivateKey: label with stray whitespace classifies correctly" {
    const pem =
        "-----BEGIN  RSA PRIVATE KEY -----\n" ++
        "MAYCAQACAQE=\n" ++ // 6-byte PKCS#1-ish DER (nonsense contents, valid shape)
        "-----END  RSA PRIVATE KEY -----\n";
    const output = try normalizePrivateKey(testing.allocator, pem);
    defer testing.allocator.free(output);

    // Must now be a PKCS#8 block, not UnsupportedKeyFormat.
    const reparsed = try findPrivateKeyBlock(testing.allocator, output);
    defer testing.allocator.free(reparsed.der);
    try testing.expectEqualStrings(label_pkcs8, reparsed.label);
}

test "normalizePrivateKey: PKCS#8 passes through" {
    const der = [_]u8{ 0x30, 0x03, 0x02, 0x01, 0x00 };
    const input = try encodePem(testing.allocator, label_pkcs8, &der);
    defer testing.allocator.free(input);

    const output = try normalizePrivateKey(testing.allocator, input);
    defer testing.allocator.free(output);

    const reparsed = try findPrivateKeyBlock(testing.allocator, output);
    defer testing.allocator.free(reparsed.der);
    try testing.expectEqualStrings(label_pkcs8, reparsed.label);
    try testing.expectEqualSlices(u8, &der, reparsed.der);
}

test "normalizePrivateKey: SEC1 EC passes through" {
    const der = [_]u8{ 0x30, 0x03, 0x02, 0x01, 0x01 };
    const input = try encodePem(testing.allocator, label_sec1_ec, &der);
    defer testing.allocator.free(input);

    const output = try normalizePrivateKey(testing.allocator, input);
    defer testing.allocator.free(output);

    const reparsed = try findPrivateKeyBlock(testing.allocator, output);
    defer testing.allocator.free(reparsed.der);
    try testing.expectEqualStrings(label_sec1_ec, reparsed.label);
    try testing.expectEqualSlices(u8, &der, reparsed.der);
}

test "normalizePrivateKey: PKCS#1 RSA is wrapped in PKCS#8" {
    // A plausible PKCS#1 RSAPrivateKey outer structure (fake internals).
    const pkcs1 = [_]u8{ 0x30, 0x0a, 0x02, 0x01, 0x00, 0x02, 0x05, 0xde, 0xad, 0xbe, 0xef, 0x00 };
    const input = try encodePem(testing.allocator, label_pkcs1_rsa, &pkcs1);
    defer testing.allocator.free(input);

    const output = try normalizePrivateKey(testing.allocator, input);
    defer testing.allocator.free(output);

    // Re-parse: must now be a PKCS#8 block with the original PKCS#1 embedded
    // as the OCTET STRING payload.
    const reparsed = try findPrivateKeyBlock(testing.allocator, output);
    defer testing.allocator.free(reparsed.der);
    try testing.expectEqualStrings(label_pkcs8, reparsed.label);

    var pos: usize = 1;
    const outer_len = try readDerLength(reparsed.der, &pos);
    try testing.expectEqual(reparsed.der.len - pos, outer_len);
    try testing.expectEqualSlices(u8, &[_]u8{ 0x02, 0x01, 0x00 }, reparsed.der[pos .. pos + 3]);
    pos += 3;
    try testing.expectEqualSlices(u8, &rsa_algorithm_identifier, reparsed.der[pos .. pos + 15]);
    pos += 15;
    try testing.expectEqual(@as(u8, 0x04), reparsed.der[pos]);
    pos += 1;
    const oct_len = try readDerLength(reparsed.der, &pos);
    try testing.expectEqual(pkcs1.len, oct_len);
    try testing.expectEqualSlices(u8, &pkcs1, reparsed.der[pos .. pos + oct_len]);
}

test "normalizePrivateKey: encrypted PKCS#1 returns EncryptedKeyNotSupported" {
    const pem =
        "-----BEGIN RSA PRIVATE KEY-----\n" ++
        "Proc-Type: 4,ENCRYPTED\n" ++
        "DEK-Info: AES-256-CBC,0123456789ABCDEF\n" ++
        "\n" ++
        "Zm9v\n" ++
        "-----END RSA PRIVATE KEY-----\n";
    try testing.expectError(
        error.EncryptedKeyNotSupported,
        normalizePrivateKey(testing.allocator, pem),
    );
}

test "normalizePrivateKey: ENCRYPTED PRIVATE KEY label returns EncryptedKeyNotSupported" {
    const pem =
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" ++
        "Zm9v\n" ++
        "-----END ENCRYPTED PRIVATE KEY-----\n";
    try testing.expectError(
        error.EncryptedKeyNotSupported,
        normalizePrivateKey(testing.allocator, pem),
    );
}

test "normalizePrivateKey: DSA and OpenSSH return UnsupportedKeyFormat" {
    const dsa =
        "-----BEGIN DSA PRIVATE KEY-----\n" ++
        "Zm9v\n" ++
        "-----END DSA PRIVATE KEY-----\n";
    try testing.expectError(
        error.UnsupportedKeyFormat,
        normalizePrivateKey(testing.allocator, dsa),
    );

    const openssh =
        "-----BEGIN OPENSSH PRIVATE KEY-----\n" ++
        "Zm9v\n" ++
        "-----END OPENSSH PRIVATE KEY-----\n";
    try testing.expectError(
        error.UnsupportedKeyFormat,
        normalizePrivateKey(testing.allocator, openssh),
    );
}

test "normalizePrivateKey: no key returns NoPrivateKey" {
    try testing.expectError(error.NoPrivateKey, normalizePrivateKey(testing.allocator, ""));
    try testing.expectError(
        error.NoPrivateKey,
        normalizePrivateKey(testing.allocator, "-----BEGIN CERTIFICATE-----\nZm9v\n-----END CERTIFICATE-----\n"),
    );
}

test "normalizePrivateKey: cert chain before key is skipped" {
    const pkcs1 = [_]u8{ 0x30, 0x06, 0x02, 0x01, 0x00, 0x02, 0x01, 0x01 };
    const pkcs1_pem = try encodePem(testing.allocator, label_pkcs1_rsa, &pkcs1);
    defer testing.allocator.free(pkcs1_pem);

    var combined: std.ArrayList(u8) = .empty;
    defer combined.deinit(testing.allocator);
    try combined.appendSlice(testing.allocator,
        "-----BEGIN CERTIFICATE-----\nZm9v\n-----END CERTIFICATE-----\n",
    );
    try combined.appendSlice(testing.allocator, pkcs1_pem);

    const output = try normalizePrivateKey(testing.allocator, combined.items);
    defer testing.allocator.free(output);

    const reparsed = try findPrivateKeyBlock(testing.allocator, output);
    defer testing.allocator.free(reparsed.der);
    try testing.expectEqualStrings(label_pkcs8, reparsed.label);
}

test "fuzz: normalizePrivateKey never crashes" {
    try std.testing.fuzz({}, fuzzNormalize, .{});
}

fn fuzzNormalize(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    const len = smith.valueRangeAtMost(u16, 0, 512);
    var buf: [512]u8 = undefined;
    for (buf[0..len]) |*b| b.* = smith.valueRangeAtMost(u8, 0, 127);
    if (normalizePrivateKey(testing.allocator, buf[0..len])) |out| {
        testing.allocator.free(out);
    } else |_| {}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Minimal DER length decoder for test assertions. Mirrors `writeDerHeader`.
fn readDerLength(der: []const u8, pos: *usize) !usize {
    if (pos.* >= der.len) return error.TruncatedDer;
    const first = der[pos.*];
    pos.* += 1;
    if (first < 0x80) return first;
    const n = @as(usize, first & 0x7f);
    if (n == 0 or n > 4) return error.InvalidDerLength;
    if (pos.* + n > der.len) return error.TruncatedDer;
    var result: usize = 0;
    for (der[pos.* .. pos.* + n]) |b| result = (result << 8) | b;
    pos.* += n;
    return result;
}
