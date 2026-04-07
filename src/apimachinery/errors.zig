const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const json = std.json;
const meta_v1 = @import("k8s_api").meta_v1;

/// StatusError wraps a K8s API Status response for use as an error context.
/// Parse from API response body with `fromResponse`, then classify with `is*` functions.
pub const StatusError = struct {
    status: meta_v1.Status,
    /// Owns the parsed JSON arena. Must call deinit() to free.
    _parsed: ?json.Parsed(meta_v1.Status) = null,

    /// Parse a K8s Status from JSON response body. Caller must call deinit().
    pub fn fromResponse(allocator: std.mem.Allocator, body: []const u8) !StatusError {
        const parsed = try json.parseFromSlice(meta_v1.Status, allocator, body, .{ .ignore_unknown_fields = true });
        return .{ .status = parsed.value, ._parsed = parsed };
    }

    /// Free the parsed JSON arena.
    pub fn deinit(self: *StatusError) void {
        if (self._parsed) |p| p.deinit();
        self._parsed = null;
    }

    /// The HTTP status code, or 0 if not set.
    pub fn code(self: StatusError) i32 {
        return self.status.code orelse 0;
    }

    /// The reason string, or empty.
    pub fn reason(self: StatusError) []const u8 {
        return self.status.reason orelse "";
    }

    /// The human-readable message.
    pub fn message(self: StatusError) []const u8 {
        return self.status.message orelse "";
    }

    /// Suggested retry delay in seconds, or null if not suggested.
    pub fn retryAfterSeconds(self: StatusError) ?i32 {
        if (self.status.details) |details| {
            if (details.retryAfterSeconds) |s| {
                if (s > 0) return s;
            }
        }
        return null;
    }
};

// ── Status reason constants ───────────────────────────────────────────────
// Mirrors metav1.StatusReason* constants from Go.

pub const reason_unauthorized = "Unauthorized";
pub const reason_forbidden = "Forbidden";
pub const reason_not_found = "NotFound";
pub const reason_already_exists = "AlreadyExists";
pub const reason_conflict = "Conflict";
pub const reason_gone = "Gone";
pub const reason_expired = "Expired";
pub const reason_invalid = "Invalid";
pub const reason_bad_request = "BadRequest";
pub const reason_method_not_allowed = "MethodNotAllowed";
pub const reason_not_acceptable = "NotAcceptable";
pub const reason_request_entity_too_large = "RequestEntityTooLarge";
pub const reason_unsupported_media_type = "UnsupportedMediaType";
pub const reason_internal_error = "InternalError";
pub const reason_server_timeout = "ServerTimeout";
pub const reason_timeout = "Timeout";
pub const reason_too_many_requests = "TooManyRequests";
pub const reason_service_unavailable = "ServiceUnavailable";

// ── Classification functions ──────────────────────────────────────────────
// Each checks reason string first, then falls back to HTTP code for
// non-standard API servers that don't set reason.

pub fn isNotFound(err: StatusError) bool {
    return reasonOrCode(err, reason_not_found, 404);
}

pub fn isAlreadyExists(err: StatusError) bool {
    return reasonEql(err, reason_already_exists);
}

pub fn isConflict(err: StatusError) bool {
    return reasonOrCode(err, reason_conflict, 409);
}

pub fn isInvalid(err: StatusError) bool {
    return reasonOrCode(err, reason_invalid, 422);
}

pub fn isGone(err: StatusError) bool {
    return reasonOrCode(err, reason_gone, 410);
}

pub fn isResourceExpired(err: StatusError) bool {
    return reasonEql(err, reason_expired);
}

pub fn isUnauthorized(err: StatusError) bool {
    return reasonOrCode(err, reason_unauthorized, 401);
}

pub fn isForbidden(err: StatusError) bool {
    return reasonOrCode(err, reason_forbidden, 403);
}

pub fn isBadRequest(err: StatusError) bool {
    return reasonOrCode(err, reason_bad_request, 400);
}

pub fn isMethodNotSupported(err: StatusError) bool {
    return reasonOrCode(err, reason_method_not_allowed, 405);
}

pub fn isNotAcceptable(err: StatusError) bool {
    return reasonOrCode(err, reason_not_acceptable, 406);
}

pub fn isUnsupportedMediaType(err: StatusError) bool {
    return reasonOrCode(err, reason_unsupported_media_type, 415);
}

pub fn isRequestEntityTooLarge(err: StatusError) bool {
    // Matches Go: always checks code, doesn't require known reason
    if (reasonEql(err, reason_request_entity_too_large)) return true;
    return err.code() == 413;
}

pub fn isInternalError(err: StatusError) bool {
    return reasonOrCode(err, reason_internal_error, 500);
}

pub fn isServerTimeout(err: StatusError) bool {
    // Go: does NOT fall back to code — no HTTP code for retryable timeouts
    return reasonEql(err, reason_server_timeout);
}

pub fn isTimeout(err: StatusError) bool {
    return reasonOrCode(err, reason_timeout, 504);
}

pub fn isTooManyRequests(err: StatusError) bool {
    // Matches Go: always checks code, doesn't require known reason
    if (reasonEql(err, reason_too_many_requests)) return true;
    return err.code() == 429;
}

pub fn isServiceUnavailable(err: StatusError) bool {
    return reasonOrCode(err, reason_service_unavailable, 503);
}

/// Returns true if the error suggests the client should retry after a delay.
pub fn isRetryable(err: StatusError) bool {
    return isServerTimeout(err) or
        isTooManyRequests(err) or
        isServiceUnavailable(err) or
        isInternalError(err) or
        isTimeout(err) or
        err.retryAfterSeconds() != null;
}

/// Returns true if the error is a client error (4xx) that should not be retried.
pub fn isPermanent(err: StatusError) bool {
    const c = err.code();
    if (c == 0) return false;
    return c >= 400 and c < 500 and !isTooManyRequests(err) and !isConflict(err);
}

// ── Internal helpers ──────────────────────────────────────────────────────

/// Known reasons — used for fallback-to-code logic.
/// If reason is known, we trust it. If unknown, we fall back to HTTP code.
const known_reasons = [_][]const u8{
    reason_unauthorized,
    reason_forbidden,
    reason_not_found,
    reason_already_exists,
    reason_conflict,
    reason_gone,
    reason_expired,
    reason_invalid,
    reason_bad_request,
    reason_method_not_allowed,
    reason_not_acceptable,
    reason_request_entity_too_large,
    reason_unsupported_media_type,
    reason_internal_error,
    reason_server_timeout,
    reason_timeout,
    reason_too_many_requests,
    reason_service_unavailable,
};

fn isKnownReason(r: []const u8) bool {
    for (known_reasons) |kr| {
        if (mem.eql(u8, kr, r)) return true;
    }
    return false;
}

fn reasonEql(err: StatusError, expected: []const u8) bool {
    return mem.eql(u8, err.reason(), expected);
}

/// Check reason first. If reason matches, return true.
/// If reason is unknown (empty or unrecognized), fall back to HTTP code.
/// This matches Go's behavior: known reasons are trusted, unknown reasons
/// defer to the HTTP status code.
fn reasonOrCode(err: StatusError, expected_reason: []const u8, expected_code: i32) bool {
    const r = err.reason();
    if (mem.eql(u8, r, expected_reason)) return true;
    if (!isKnownReason(r)) return err.code() == expected_code;
    return false;
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

fn makeStatus(code_val: i32, reason_val: ?[]const u8) StatusError {
    return .{ .status = .{ .code = code_val, .reason = reason_val } };
}

test "classification: reason-based" {
    const cases = .{
        .{ makeStatus(404, "NotFound"), "isNotFound", true },
        .{ makeStatus(409, "Conflict"), "isConflict", true },
        .{ makeStatus(410, "Gone"), "isGone", true },
        .{ makeStatus(422, "Invalid"), "isInvalid", true },
        .{ makeStatus(401, "Unauthorized"), "isUnauthorized", true },
        .{ makeStatus(403, "Forbidden"), "isForbidden", true },
        .{ makeStatus(400, "BadRequest"), "isBadRequest", true },
        .{ makeStatus(500, "InternalError"), "isInternalError", true },
        .{ makeStatus(429, "TooManyRequests"), "isTooManyRequests", true },
        .{ makeStatus(503, "ServiceUnavailable"), "isServiceUnavailable", true },
        .{ makeStatus(504, "Timeout"), "isTimeout", true },
        .{ makeStatus(200, "ServerTimeout"), "isServerTimeout", true },
        .{ makeStatus(409, "AlreadyExists"), "isAlreadyExists", true },
        .{ makeStatus(410, "Expired"), "isResourceExpired", true },
    };
    inline for (cases) |c| {
        const result = @field(@This(), c[1])(c[0]);
        try testing.expectEqual(c[2], result);
    }
}

test "classification: code fallback for unknown reason" {
    // When reason is empty/unknown, fall back to HTTP code
    const cases = .{
        .{ makeStatus(404, null), isNotFound, true },
        .{ makeStatus(409, null), isConflict, true },
        .{ makeStatus(410, null), isGone, true },
        .{ makeStatus(422, null), isInvalid, true },
        .{ makeStatus(401, null), isUnauthorized, true },
        .{ makeStatus(403, null), isForbidden, true },
        .{ makeStatus(500, null), isInternalError, true },
        .{ makeStatus(503, null), isServiceUnavailable, true },
        .{ makeStatus(429, null), isTooManyRequests, true },
    };
    inline for (cases) |c| {
        try testing.expectEqual(c[2], c[1](c[0]));
    }
}

test "classification: no false positives with known reasons" {
    // Known reason that doesn't match — don't fall back to code
    // e.g., code=404 but reason="Forbidden" → isNotFound should be false
    const err = makeStatus(404, "Forbidden");
    try testing.expect(!isNotFound(err));
    try testing.expect(isForbidden(err)); // reason takes priority
}

test "classification: isRetryable" {
    const retryable = .{
        makeStatus(500, "InternalError"),
        makeStatus(503, "ServiceUnavailable"),
        makeStatus(429, "TooManyRequests"),
        makeStatus(504, "Timeout"),
        makeStatus(200, "ServerTimeout"),
    };
    inline for (retryable) |err| {
        try testing.expect(isRetryable(err));
    }

    const not_retryable = .{
        makeStatus(404, "NotFound"),
        makeStatus(409, "Conflict"),
        makeStatus(403, "Forbidden"),
        makeStatus(422, "Invalid"),
    };
    inline for (not_retryable) |err| {
        try testing.expect(!isRetryable(err));
    }
}

test "classification: isPermanent" {
    const permanent = .{
        makeStatus(404, "NotFound"),
        makeStatus(403, "Forbidden"),
        makeStatus(422, "Invalid"),
        makeStatus(400, "BadRequest"),
        makeStatus(401, "Unauthorized"),
    };
    inline for (permanent) |err| {
        try testing.expect(isPermanent(err));
    }

    // Not permanent: retryable 4xx or 5xx
    try testing.expect(!isPermanent(makeStatus(429, "TooManyRequests")));
    try testing.expect(!isPermanent(makeStatus(409, "Conflict")));
    try testing.expect(!isPermanent(makeStatus(500, "InternalError")));
}

test "classification: retryAfterSeconds" {
    // No details → null
    try testing.expect(makeStatus(429, "TooManyRequests").retryAfterSeconds() == null);

    // With retryAfterSeconds
    var err = StatusError{ .status = .{
        .code = 429,
        .reason = "TooManyRequests",
        .details = .{ .retryAfterSeconds = 30 },
    } };
    try testing.expectEqual(@as(i32, 30), err.retryAfterSeconds().?);
    try testing.expect(isRetryable(err));

    // Zero retryAfterSeconds → null
    err.status.details.?.retryAfterSeconds = 0;
    try testing.expect(err.retryAfterSeconds() == null);
}

test "classification: StatusError accessors" {
    const err = StatusError{ .status = .{
        .code = 404,
        .reason = "NotFound",
        .message = "the thing is gone",
    } };
    try testing.expectEqual(@as(i32, 404), err.code());
    try testing.expectEqualStrings("NotFound", err.reason());
    try testing.expectEqualStrings("the thing is gone", err.message());
}
