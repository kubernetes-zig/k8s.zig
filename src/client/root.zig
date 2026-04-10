pub const kubeconfig = @import("kubeconfig.zig");
pub const config = @import("config.zig");
pub const url = @import("url.zig");
pub const transport = @import("transport.zig");
pub const dynamic = @import("dynamic.zig");
pub const watch = @import("watch.zig");
pub const discovery = @import("discovery.zig");
pub const discovery_cache = @import("discovery_cache.zig");
pub const restmapper = @import("restmapper.zig");
pub const tls_transport = @import("tls_transport.zig");
pub const pem = @import("pem.zig");

pub const Kubeconfig = kubeconfig.Kubeconfig;
pub const Config = config.Config;
pub const UrlBuilder = url.UrlBuilder;
pub const Transport = transport.Transport;
pub const DynamicClient = dynamic.DynamicClient;
pub const ResourceClient = dynamic.ResourceClient;
pub const Watcher = watch.Watcher;
pub const WatchEvent = watch.Event;
pub const WatchEventType = watch.EventType;
pub const DiscoveryClient = discovery.DiscoveryClient;
pub const CachedDiscoveryClient = discovery_cache.CachedDiscoveryClient;
pub const RESTMapper = restmapper.RESTMapper;
pub const DynamicRESTMapper = restmapper.DynamicRESTMapper;
pub const RESTMapping = restmapper.RESTMapping;
pub const RESTScope = restmapper.RESTScope;
pub const TlsTransport = tls_transport.TlsTransport;

test {
    _ = kubeconfig;
    _ = config;
    _ = url;
    _ = watch;
    _ = discovery;
    _ = discovery_cache;
    _ = restmapper;
    _ = tls_transport;
    _ = pem;
    // transport and dynamic have no unit tests — they need a real API server
}
