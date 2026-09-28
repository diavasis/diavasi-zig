//! Example consumer. Prints each record, then the acked record ids.
const std = @import("std");
const diavasi = @import("diavasi");

fn onRecord(batch_id: u64, record_id: u64, payload: []const u8, ctx: ?*anyopaque) void {
    _ = ctx;
    std.debug.print("batch {d} record {d} ({d} bytes)\n", .{ batch_id, record_id, payload.len });
}

fn flag(args: []const [:0]const u8, name: []const u8, fallback: [:0]const u8) [:0]const u8 {
    var found = fallback;
    var i: usize = 1;
    while (i + 1 < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], name)) found = args[i + 1];
    }
    return found;
}

test "unit: the last flag occurrence wins" {
    const args = [_][:0]const u8{ "diavasi-zig", "--group", "first", "--token", "a", "--group", "second" };
    try std.testing.expectEqualStrings("second", flag(&args, "--group", "demo"));
    try std.testing.expectEqualStrings("a", flag(&args, "--token", "sdk-demo"));
    try std.testing.expectEqualStrings("127.0.0.1:7710", flag(&args, "--addr", "127.0.0.1:7710"));
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    const outcome = try diavasi.consume(allocator, .{
        .addr = flag(args, "--addr", "127.0.0.1:7710"),
        .ca_path = flag(args, "--ca", "/tmp/diavasi-sdk/dataplane-ca.crt"),
        .token = flag(args, "--token", "sdk-demo"),
        .group_id = flag(args, "--group", "demo"),
        .consumer_id = flag(args, "--consumer", "zig"),
        .expect_records = 8,
        .on_record = onRecord,
    });

    switch (outcome) {
        .report => |report| {
            defer report.deinit(allocator);
            std.debug.print("record_ids", .{});
            for (report.record_ids) |id| std.debug.print(" {d}", .{id});
            std.debug.print("\n", .{});
        },
        .failure => |failure| {
            if (failure.code >= 1 and failure.code <= 8) {
                std.debug.print("protocol {d}: {s}\n", .{ failure.code, failure.message });
            } else {
                std.debug.print("{s}\n", .{failure.message});
            }
            const status: u8 = if (failure.code > 0) @intCast(failure.code) else 1;
            failure.deinit(allocator);
            std.process.exit(status);
        },
    }
}
