const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const diavasi_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    diavasi_mod.addIncludePath(b.path("c/include"));
    diavasi_mod.linkSystemLibrary("grpc", .{});
    diavasi_mod.addCSourceFiles(.{
        .root = b.path("c"),
        .files = &.{ "src/consume.c", "src/proto.c" },
        .flags = &.{ "-std=c11", "-Wall" },
    });

    const exe = b.addExecutable(.{
        .name = "diavasi-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "diavasi", .module = diavasi_mod },
            },
        }),
    });

    b.installArtifact(exe);
    const run = b.addRunArtifact(exe);
    if (b.args) |args| run.addArgs(args);
    b.step("run", "Join the demo group").dependOn(&run.step);

    const lib_tests = b.addTest(.{ .root_module = diavasi_mod });
    const run_lib_tests = b.addRunArtifact(lib_tests);
    const exe_tests = b.addTest(.{ .root_module = exe.root_module });
    const run_exe_tests = b.addRunArtifact(exe_tests);
    const test_step = b.step("test", "Unit tests, and server regressions when DIAVASI_DATA_ADDR is set");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_exe_tests.step);
}
