pub const scheme = @import("scheme.zig");
pub const unstructured = @import("unstructured.zig");
pub const errors = @import("errors.zig");
pub const selector = @import("selector.zig");
pub const field_selector = @import("field_selector.zig");
pub const quantity = @import("quantity.zig");
pub const time = @import("time.zig");

pub const GroupVersion = scheme.GroupVersion;
pub const GroupVersionKind = scheme.GroupVersionKind;
pub const GroupVersionResource = scheme.GroupVersionResource;
pub const Unstructured = unstructured.Unstructured;
pub const Nav = unstructured.Nav;
pub const StatusError = errors.StatusError;

test {
    _ = scheme;
    _ = unstructured;
    _ = errors;
    _ = selector;
    _ = field_selector;
    _ = quantity;
    _ = time;
}
