const std = @import("std");
const testing = std.testing;
const meta_v1 = @import("k8s/io/apimachinery/pkg/apis/meta/v1.pb.zig");
const resource = @import("k8s/io/apimachinery/pkg/api/resource.pb.zig");
const intstr = @import("k8s/io/apimachinery/pkg/util/intstr.pb.zig");

// ── Real K8s API JSON payloads ────────────────────────────────────────────

const status_404_json =
    \\{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"deployments.apps \"nginx\" not found","reason":"NotFound","details":{"name":"nginx","group":"apps","kind":"deployments"},"code":404}
;

const status_409_json =
    \\{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Operation cannot be fulfilled","reason":"Conflict","code":409}
;

const status_422_json =
    \\{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"spec.replicas: Invalid value","reason":"Invalid","code":422}
;

const objectmeta_json =
    \\{"name":"nginx","namespace":"default","uid":"abc-123","resourceVersion":"12345","generation":3,"creationTimestamp":"2024-01-01T00:00:00Z","labels":{"app":"nginx","tier":"frontend"},"annotations":{"note":"test"},"finalizers":["my.io/cleanup"]}
;

const objectmeta_with_deletion_json =
    \\{"name":"nginx","namespace":"default","uid":"abc-123","creationTimestamp":"2024-01-01T00:00:00Z","deletionTimestamp":"2024-06-01T12:00:00Z","deletionGracePeriodSeconds":30}
;

const objectmeta_with_ownerrefs_json =
    \\{"name":"nginx-pod","namespace":"default","uid":"pod-uid","creationTimestamp":"2024-01-01T00:00:00Z","ownerReferences":[{"apiVersion":"apps/v1","kind":"ReplicaSet","name":"nginx-rs","uid":"rs-uid","controller":true,"blockOwnerDeletion":true}]}
;

const condition_json =
    \\{"type":"Available","status":"True","lastTransitionTime":"2024-03-15T08:00:00Z","reason":"MinimumReplicasAvailable","message":"Deployment has minimum availability."}
;

const list_meta_json =
    \\{"resourceVersion":"99999","continue":"abc123","remainingItemCount":42}
;

// ── Status parsing tests ──────────────────────────────────────────────────

test "K8s JSON: Status responses" {
    const cases = .{
        .{ status_404_json, @as(i32, 404), "NotFound" },
        .{ status_409_json, @as(i32, 409), "Conflict" },
        .{ status_422_json, @as(i32, 422), "Invalid" },
    };
    inline for (cases) |c| {
        const parsed = try std.json.parseFromSlice(meta_v1.Status, testing.allocator, c[0], .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try testing.expectEqual(c[1], parsed.value.code.?);
        try testing.expectEqualStrings(c[2], parsed.value.reason.?);
        try testing.expectEqualStrings("Failure", parsed.value.status.?);
    }
}

test "K8s JSON: Status details" {
    const parsed = try std.json.parseFromSlice(meta_v1.Status, testing.allocator, status_404_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const details = parsed.value.details.?;
    try testing.expectEqualStrings("nginx", details.name.?);
    try testing.expectEqualStrings("apps", details.group.?);
    try testing.expectEqualStrings("deployments", details.kind.?);
}

// ── ObjectMeta parsing tests ──────────────────────────────────────────────

test "K8s JSON: ObjectMeta identity fields" {
    const parsed = try std.json.parseFromSlice(meta_v1.ObjectMeta, testing.allocator, objectmeta_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const meta = parsed.value;

    try testing.expectEqualStrings("nginx", meta.name.?);
    try testing.expectEqualStrings("default", meta.namespace.?);
    try testing.expectEqualStrings("abc-123", meta.uid.?);
    try testing.expectEqualStrings("12345", meta.resourceVersion.?);
    try testing.expectEqual(@as(i64, 3), meta.generation.?);
}

test "K8s JSON: ObjectMeta creationTimestamp as RFC3339 string" {
    const parsed = try std.json.parseFromSlice(meta_v1.ObjectMeta, testing.allocator, objectmeta_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const ts = parsed.value.creationTimestamp.?;
    try testing.expectEqualStrings("2024-01-01T00:00:00Z", ts.timestamp.?);
}

test "K8s JSON: ObjectMeta deletionTimestamp" {
    const parsed = try std.json.parseFromSlice(meta_v1.ObjectMeta, testing.allocator, objectmeta_with_deletion_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const meta = parsed.value;

    try testing.expectEqualStrings("2024-06-01T12:00:00Z", meta.deletionTimestamp.?.timestamp.?);
    try testing.expectEqual(@as(i64, 30), meta.deletionGracePeriodSeconds.?);
}

test "K8s JSON: ObjectMeta labels as JSON object" {
    const parsed = try std.json.parseFromSlice(meta_v1.ObjectMeta, testing.allocator, objectmeta_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    // Labels parsed as std.json.Value (object)
    const labels = parsed.value.labels.?;
    try testing.expect(labels == .object);
    try testing.expectEqualStrings("nginx", labels.object.get("app").?.string);
    try testing.expectEqualStrings("frontend", labels.object.get("tier").?.string);
}

test "K8s JSON: ObjectMeta ownerReferences" {
    const parsed = try std.json.parseFromSlice(meta_v1.ObjectMeta, testing.allocator, objectmeta_with_ownerrefs_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    try testing.expectEqual(@as(usize, 1), parsed.value.ownerReferences.items.len);
    const ref = parsed.value.ownerReferences.items[0];
    try testing.expectEqualStrings("apps/v1", ref.apiVersion.?);
    try testing.expectEqualStrings("ReplicaSet", ref.kind.?);
    try testing.expectEqualStrings("nginx-rs", ref.name.?);
    try testing.expectEqualStrings("rs-uid", ref.uid.?);
    try testing.expectEqual(true, ref.controller.?);
    try testing.expectEqual(true, ref.blockOwnerDeletion.?);
}

// ── Condition parsing tests ───────────────────────────────────────────────

test "K8s JSON: Condition with lastTransitionTime" {
    const parsed = try std.json.parseFromSlice(meta_v1.Condition, testing.allocator, condition_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const cond = parsed.value;

    try testing.expectEqualStrings("Available", cond.type.?);
    try testing.expectEqualStrings("True", cond.status.?);
    try testing.expectEqualStrings("MinimumReplicasAvailable", cond.reason.?);
    try testing.expectEqualStrings("2024-03-15T08:00:00Z", cond.lastTransitionTime.?.timestamp.?);
}

// ── ListMeta parsing tests ────────────────────────────────────────────────

test "K8s JSON: ListMeta" {
    const parsed = try std.json.parseFromSlice(meta_v1.ListMeta, testing.allocator, list_meta_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    try testing.expectEqualStrings("99999", parsed.value.resourceVersion.?);
    try testing.expectEqualStrings("abc123", parsed.value.@"continue".?);
    try testing.expectEqual(@as(i64, 42), parsed.value.remainingItemCount.?);
}

// ── Time custom type tests ────────────────────────────────────────────────

test "K8s JSON: Time null handling" {
    const json_with_null =
        \\{"name":"test","creationTimestamp":"2024-01-01T00:00:00Z","deletionTimestamp":null}
    ;
    const parsed = try std.json.parseFromSlice(meta_v1.ObjectMeta, testing.allocator, json_with_null, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    try testing.expectEqualStrings("2024-01-01T00:00:00Z", parsed.value.creationTimestamp.?.timestamp.?);
    // deletionTimestamp is present but the Time's inner timestamp is null
    if (parsed.value.deletionTimestamp) |dt| {
        try testing.expect(dt.timestamp == null);
    }
}

// ── Quantity custom type tests ────────────────────────────────────────────

test "K8s JSON: Quantity string values" {
    const cases = .{
        .{ "\"100m\"", "100m" },
        .{ "\"1Gi\"", "1Gi" },
        .{ "\"500\"", "500" },
        .{ "\"2.5\"", "2.5" },
    };
    inline for (cases) |c| {
        const parsed = try std.json.parseFromSlice(resource.Quantity, testing.allocator, c[0], .{});
        defer parsed.deinit();
        try testing.expectEqualStrings(c[1], parsed.value.raw.?);
    }
}

// ── IntOrString custom type tests ─────────────────────────────────────────

test "K8s JSON: IntOrString integer" {
    const parsed = try std.json.parseFromSlice(intstr.IntOrString, testing.allocator, "8080", .{});
    defer parsed.deinit();
    try testing.expectEqual(@as(i32, 8080), parsed.value.value.int);
}

test "K8s JSON: IntOrString string" {
    const parsed = try std.json.parseFromSlice(intstr.IntOrString, testing.allocator, "\"http\"", .{});
    defer parsed.deinit();
    try testing.expectEqualStrings("http", parsed.value.value.string);
}
