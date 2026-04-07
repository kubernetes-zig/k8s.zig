/// Generated Kubernetes API types from protobuf definitions.
/// Do not edit — regenerate with `make generate`.
pub const meta_v1 = @import("k8s/io/apimachinery/pkg/apis/meta/v1.pb.zig");
pub const runtime = @import("k8s/io/apimachinery/pkg/runtime.pb.zig");
pub const runtime_schema = @import("k8s/io/apimachinery/pkg/runtime/schema.pb.zig");
pub const resource = @import("k8s/io/apimachinery/pkg/api/resource.pb.zig");
pub const intstr = @import("k8s/io/apimachinery/pkg/util/intstr.pb.zig");
pub const apidiscovery_v2 = @import("k8s/io/api/apidiscovery/v2.pb.zig");

// Re-export commonly used types
pub const ObjectMeta = meta_v1.ObjectMeta;
pub const TypeMeta = meta_v1.TypeMeta;
pub const Status = meta_v1.Status;
pub const StatusDetails = meta_v1.StatusDetails;
pub const StatusCause = meta_v1.StatusCause;
pub const ListMeta = meta_v1.ListMeta;
pub const WatchEvent = meta_v1.WatchEvent;
pub const Time = meta_v1.Time;
pub const Timestamp = meta_v1.Timestamp;
pub const APIResource = meta_v1.APIResource;
pub const APIResourceList = meta_v1.APIResourceList;
pub const APIGroup = meta_v1.APIGroup;
pub const APIGroupList = meta_v1.APIGroupList;
pub const APIGroupDiscovery = apidiscovery_v2.APIGroupDiscovery;
pub const APIGroupDiscoveryList = apidiscovery_v2.APIGroupDiscoveryList;
pub const APIResourceDiscovery = apidiscovery_v2.APIResourceDiscovery;
pub const APISubresourceDiscovery = apidiscovery_v2.APISubresourceDiscovery;
pub const APIVersionDiscovery = apidiscovery_v2.APIVersionDiscovery;

test {
    _ = meta_v1;
    _ = runtime;
    _ = runtime_schema;
    _ = resource;
    _ = intstr;
    _ = apidiscovery_v2;
    _ = @import("json_test.zig");
}
