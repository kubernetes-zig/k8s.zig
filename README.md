# k8s.zig

> **Work in progress. Experimental. APIs will change.**

Kubernetes API machinery and controller runtime in Zig. Targets
`std.Io` async for all concurrency — no raw threads, no custom event
loops.

Requires Zig `0.16.0-dev.3128+ad7a02822` or later.

## What's here

- **apimachinery** — Unstructured (zero-copy JSON navigation), label/field
  selectors, resource quantities, RFC3339 time, K8s status errors, GVK/GVR
  scheme
- **client** — kubeconfig parsing, TLS/mTLS transport, bearer/basic/exec
  auth, streaming watch, dynamic unstructured CRUD, API discovery, REST
  mapper
- **cache** — thread-safe store with secondary indexes, reflector
  (list+watch with backoff and jitter), informer (event handlers, sync
  tracking), rate-limited work queue, reconciler worker pool

## Build and test

```bash
zig build test
```

## Status

Core controller loop works end-to-end (reflector, informer, queue,
reconciler). Transport handles most real-cluster auth scenarios. Not
production-ready — missing chunked list, leader election, SSA,
metrics, and integration tests.

## License

Apache 2.0
