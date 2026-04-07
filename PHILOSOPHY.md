# Philosophy

**Zig-idiomatic, not Go-translated.** Don't port Go patterns to Zig syntax. Design for Zig's strengths.

- **Comptime over runtime.** Use comptime for type construction, known GVK constants, field path validation, codegen. If the compiler can catch it, don't defer to runtime.
- **Explicit allocation.** Every function that allocates takes `std.mem.Allocator`. The library never owns an allocator. Caller picks the strategy: arena per reconcile, pool, GPA, whatever. Same pattern as stdlib.
- **Explicit I/O.** Every function that does I/O takes `std.Io`. Caller picks the backend: Threaded, IoUring, Kqueue. Library code is agnostic.
- **Optionals for absence, errors for failures.** Missing JSON field? Return `null`. Allocation failed? Return `error.OutOfMemory`. Don't conflate "doesn't exist" with "something broke."
- **Views over copies.** Metadata is a zero-cost lens into Unstructured, not a separate struct. Avoid copying data when a reference suffices.
- **Error sets, not sentinel values.** Zig error unions with K8s-specific error sets (`NotFound`, `Conflict`, `Gone`). Compiler-enforced exhaustive handling replaces Go's `if apierrors.IsNotFound(err)`.
- **Codegen when suitable.** For CRD-specific typed clients, generate Zig code from OpenAPI schemas. Don't hand-write what a machine can produce.
- **Composition, not inheritance.** No vtable hierarchies. Small, focused types that compose. `Informer` = `Reflector` + `Store` + event handlers, not an abstract base class.
- **`std.Io` for all concurrency.** All async work goes through `std.Io` — the standard library's async interface. No raw thread spawning, no custom event loops. Code runs on `Io.Threaded` (thread pool) today and `Io.Uring`/`Io.Kqueue` (fibers) tomorrow with zero changes. Use `Io.Group` for fan-out, `Io.Queue` for channels, `Io.select` for multiplexing.
- **Readable and friendly.** Controller authors will read and write against this API daily. Optimize for the common case. Chained field access, sensible defaults, minimal boilerplate.
- **Zero dependencies beyond std.** No third-party deps unless absolutely necessary. Everything from stdlib or hand-built. OpenSSL is opt-in, not required — prefer `std.crypto.tls` by default.
- **No global state.** No singletons, no global registries, no package-level vars. Everything threaded through explicitly — allocator, io, client, config. This makes testing trivial and concurrency safe.
- **Wire compatibility, not code compatibility.** We match the Kubernetes API protocol (HTTP, JSON, watch semantics, SSA). We don't mirror client-go's internal structure. If Zig offers a better way to express something, we take it.
- **Fail fast.** `unreachable` for impossible states, assertions for invariants in debug mode. Don't silently swallow errors or return defaults when something is wrong. A panic during development beats a subtle bug in production.
- **Testable by design.** Every component takes interfaces (allocator, io, client) so you can inject mocks. No test-only code paths or conditional compilation for testing. If it's hard to test, the design is wrong.
- **Linux primary, macOS for dev.** Production is Linux containers. macOS must work for development.
- **100% test coverage or GTFO.** Every public function has tests. Every edge case is covered. No untested code ships. Tests are the specification.
- **Table-driven tests.** Prefer test case tables over individual test functions. Group related assertions into a single test with a cases array. Clearer, more compact, easier to extend.
- **Learn from the best.** When lost or dealing with complex design problems, study how Tigerbeetle, Bun, and Zig's stdlib solve similar issues. Don't reinvent patterns that are already proven in production Zig codebases.
- **Doc comments on all public API.** Zig's `///` doc comments generate documentation. Public functions get doc comments. Internal code speaks for itself.

## Related Projects

Prior art and reference implementations worth studying:

- **[inge4pres/k8s.zig](https://github.com/inge4pres/k8s.zig)** — Typed K8s client in Zig with protobuf. Comptime `ResourceClient(T)` pattern is elegant. No unstructured support.
- **[guanchzhou/zig-klient](https://github.com/guanchzhou/zig-klient)** — 62 K8s resource types, WebSocket exec/attach. Very early.
- **[berdon/zig-json](https://github.com/berdon/zig-json)** — Chainable `.get()` JSON navigation. Panics on missing fields (not suitable for K8s). Validates the Nav pattern.
- **[EzequielRamis/zimdjson](https://github.com/EzequielRamis/zimdjson)** — SIMD JSON parser with `.at()` chaining. High perf but heavy.
- **[Tigerbeetle](https://github.com/tigerbeetle/tigerbeetle)** — io_uring/kqueue I/O, static allocation, zero-copy patterns. Reference for I/O architecture.
- **[Bun](https://github.com/oven-sh/bun)** — epoll/kqueue event loop, thread pool, lock-free queues. Reference for concurrency patterns.
