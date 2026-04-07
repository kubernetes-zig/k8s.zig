const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Dependencies ──────────────────────────────────────────────────────

    const protobuf_dep = b.dependency("protobuf", .{
        .target = target,
        .optimize = optimize,
    });

    const yaml_dep = b.dependency("zig_yaml", .{
        .target = target,
        .optimize = optimize,
    });

    // ── Modules ───────────────────────────────────────────────────────────

    // Generated K8s API types
    const api_module = b.addModule("k8s_api", .{
        .root_source_file = b.path("api/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    api_module.addImport("protobuf", protobuf_dep.module("protobuf"));

    // API machinery (hand-written)
    const apimachinery_module = b.addModule("k8s_zig", .{
        .root_source_file = b.path("src/apimachinery/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    apimachinery_module.addImport("k8s_api", api_module);

    const tls_dep = b.dependency("tls", .{});

    // Client
    const client_module = b.addModule("k8s_client", .{
        .root_source_file = b.path("src/client/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    client_module.addImport("k8s_zig", apimachinery_module);
    client_module.addImport("k8s_api", api_module);
    client_module.addImport("yaml", yaml_dep.module("yaml"));
    client_module.addImport("tls", tls_dep.module("tls"));

    // Cache (store, informer, reflector)
    const cache_module = b.addModule("k8s_cache", .{
        .root_source_file = b.path("src/cache/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    cache_module.addImport("k8s_zig", apimachinery_module);
    cache_module.addImport("k8s_client", client_module);

    const configmap_controller = b.addExecutable(.{
        .name = "configmap-controller",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/configmap_controller.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    configmap_controller.root_module.addImport("k8s_zig", apimachinery_module);
    configmap_controller.root_module.addImport("k8s_client", client_module);
    configmap_controller.root_module.addImport("k8s_cache", cache_module);
    b.installArtifact(configmap_controller);

    const list_pods = b.addExecutable(.{
        .name = "list-pods",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/list_pods.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    list_pods.root_module.addImport("k8s_zig", apimachinery_module);
    list_pods.root_module.addImport("k8s_client", client_module);
    b.installArtifact(list_pods);

    const example_list_pods_step = b.step("example-list-pods", "Build the list-pods example");
    example_list_pods_step.dependOn(&list_pods.step);

    const run_list_pods = b.addRunArtifact(list_pods);
    if (b.args) |args| run_list_pods.addArgs(args);
    const run_list_pods_step = b.step("run-list-pods", "Run the list-pods example");
    run_list_pods_step.dependOn(&run_list_pods.step);

    const mem_profile = b.addExecutable(.{
        .name = "mem-profile",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/mem_profile.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    mem_profile.root_module.addImport("k8s_zig", apimachinery_module);
    mem_profile.root_module.addImport("k8s_client", client_module);
    mem_profile.root_module.addImport("k8s_cache", cache_module);
    b.installArtifact(mem_profile);

    const run_mem_profile = b.addRunArtifact(mem_profile);
    if (b.args) |args| run_mem_profile.addArgs(args);
    const run_mem_profile_step = b.step("run-mem-profile", "Run memory profiler");
    run_mem_profile_step.dependOn(&run_mem_profile.step);

    const cache_bench = b.addExecutable(.{
        .name = "cache-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/cache_bench.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    cache_bench.root_module.addImport("k8s_zig", apimachinery_module);
    cache_bench.root_module.addImport("k8s_cache", cache_module);
    b.installArtifact(cache_bench);

    const example_step = b.step("example-configmap-controller", "Build the configmap controller example");
    example_step.dependOn(&configmap_controller.step);

    const run_configmap_controller = b.addRunArtifact(configmap_controller);
    if (b.args) |args| run_configmap_controller.addArgs(args);
    const run_example_step = b.step("run-configmap-controller", "Run the configmap controller example");
    run_example_step.dependOn(&run_configmap_controller.step);

    const run_cache_bench = b.addRunArtifact(cache_bench);
    if (b.args) |args| run_cache_bench.addArgs(args);
    const bench_step = b.step("bench-cache", "Run cache/controller throughput benchmarks");
    bench_step.dependOn(&run_cache_bench.step);

    // ── Tests ─────────────────────────────────────────────────────────────

    const test_step = b.step("test", "Run tests");

    const api_tests = b.addTest(.{ .root_module = api_module });
    const run_api_tests = b.addRunArtifact(api_tests);
    test_step.dependOn(&run_api_tests.step);

    const apimachinery_tests = b.addTest(.{ .root_module = apimachinery_module });
    const run_apimachinery_tests = b.addRunArtifact(apimachinery_tests);
    test_step.dependOn(&run_apimachinery_tests.step);

    const client_tests = b.addTest(.{ .root_module = client_module });
    const run_client_tests = b.addRunArtifact(client_tests);
    test_step.dependOn(&run_client_tests.step);

    const cache_tests = b.addTest(.{ .root_module = cache_module });
    const run_cache_tests = b.addRunArtifact(cache_tests);
    test_step.dependOn(&run_cache_tests.step);

    const cache_test_step = b.step("test-cache", "Run cache/controller tests");
    cache_test_step.dependOn(&run_cache_tests.step);
}
