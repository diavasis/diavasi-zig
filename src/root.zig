//! Zig client for `diavasi.data.v1`.
//!
//! `consume` opens the TLS stream, sends Hello version 1, joins the group, and
//! acks each batch. The server owns the cursor. `record_id` can be 0, so this
//! client does not dedupe on it. The transport is the C library in the `c/` submodule.

const std = @import("std");

const c = @cImport({
    @cInclude("diavasi.h");
});

/// Called for each record before the batch is acked.
/// `payload` is valid only for the duration of the call.
pub const RecordFn = *const fn (batch_id: u64, record_id: u64, payload: []const u8, ctx: ?*anyopaque) void;

/// Where to connect and how far to read.
pub const Options = struct {
    /// Data-plane address, `host:port`, without a scheme.
    addr: [:0]const u8,
    /// PEM file for the data-plane CA. The TLS name is `localhost`.
    ca_path: [:0]const u8,
    /// Bearer token. A bad token is a failure with code -1.
    token: [:0]const u8,
    /// Consumer group to join.
    group_id: [:0]const u8,
    /// Consumer id. Reconnect with the same id to replay unacked batches.
    consumer_id: [:0]const u8,
    /// Batches the server may have in flight. 0 is treated as 1.
    max_in_flight: u32 = 1,
    /// Stop after this many acks without Leave. 0 disables the limit.
    halt_after_acks: u32 = 0,
    /// Send Leave after this many records. 0 reads until the stream ends.
    expect_records: u64 = 0,
    /// Optional per-record hook. Null skips it.
    on_record: ?RecordFn = null,
    /// Passed through to `on_record`.
    ctx: ?*anyopaque = null,
};

/// Record ids and batch ids acked by one `consume` call.
pub const Report = struct {
    record_ids: []u64,
    batch_ids: []u64,

    pub fn deinit(self: Report, allocator: std.mem.Allocator) void {
        allocator.free(self.record_ids);
        allocator.free(self.batch_ids);
    }
};

/// A failed consume. `code` 1 through 8 is a protocol frame. `-1` is transport or auth.
pub const Failure = struct {
    code: i32,
    message: []u8,

    pub fn deinit(self: Failure, allocator: std.mem.Allocator) void {
        allocator.free(self.message);
    }
};

/// Either the acked ids or a protocol, transport, or auth failure.
pub const Outcome = union(enum) {
    report: Report,
    failure: Failure,
};

const Hook = struct {
    func: RecordFn,
    ctx: ?*anyopaque,
};

fn trampoline(
    batch_id: u64,
    record_id: u64,
    payload: [*c]const u8,
    payload_len: usize,
    user: ?*anyopaque,
) callconv(.c) void {
    const hook: *Hook = @ptrCast(@alignCast(user.?));
    const bytes = if (payload_len == 0) &[_]u8{} else payload[0..payload_len];
    hook.func(batch_id, record_id, bytes, hook.ctx);
}

/// Joins `options.group_id`, acks each batch, and copies the ids into `allocator`.
///
/// `on_record` runs before each ack. The caller frees a report or a failure
/// with `deinit`.
pub fn consume(allocator: std.mem.Allocator, options: Options) std.mem.Allocator.Error!Outcome {
    return finish(allocator, options, c.diavasi_consume);
}

fn finish(allocator: std.mem.Allocator, options: Options, backend: ConsumeFn) std.mem.Allocator.Error!Outcome {
    var hook: Hook = undefined;
    var c_opts = std.mem.zeroes(c.diavasi_options);
    c_opts.addr = options.addr.ptr;
    c_opts.ca_path = options.ca_path.ptr;
    c_opts.token = options.token.ptr;
    c_opts.group_id = options.group_id.ptr;
    c_opts.consumer_id = options.consumer_id.ptr;
    c_opts.max_in_flight = options.max_in_flight;
    c_opts.halt_after_acks = options.halt_after_acks;
    c_opts.expect_records = options.expect_records;
    if (options.on_record) |func| {
        hook = .{ .func = func, .ctx = options.ctx };
        c_opts.on_record = trampoline;
        c_opts.user = &hook;
    }

    var raw = std.mem.zeroes(c.diavasi_report);
    var error_buf = [_]u8{0} ** 512;
    const rc = backend(&c_opts, &raw, &error_buf, error_buf.len);
    defer c.diavasi_report_free(&raw);

    if (rc != 0) {
        return .{ .failure = .{
            .code = rc,
            .message = try allocator.dupe(u8, std.mem.sliceTo(&error_buf, 0)),
        } };
    }

    const record_ids = try allocator.alloc(u64, raw.record_count);
    errdefer allocator.free(record_ids);
    if (raw.record_count > 0) {
        for (0..raw.record_count) |i| record_ids[i] = raw.record_ids[i];
    }

    const batch_ids = try allocator.alloc(u64, raw.batch_count);
    errdefer allocator.free(batch_ids);
    if (raw.batch_count > 0) {
        for (0..raw.batch_count) |i| batch_ids[i] = raw.batch_ids[i];
    }

    return .{ .report = .{ .record_ids = record_ids, .batch_ids = batch_ids } };
}

const ConsumeFn = *const fn (
    [*c]const c.diavasi_options,
    [*c]c.diavasi_report,
    [*c]u8,
    usize,
) callconv(.c) c_int;

fn setMessage(buf: [*c]u8, cap: usize, message: []const u8) void {
    if (buf == null or cap == 0) return;
    const n = @min(message.len, cap - 1);
    var i: usize = 0;
    while (i < n) : (i += 1) buf[i] = message[i];
    buf[n] = 0;
}

/// Stands in for a data-plane session. A group named `zig-missing` is protocol
/// error 5. Any other group yields record ids `1` through `expect_records`.
fn mockServer(
    options: [*c]const c.diavasi_options,
    report: [*c]c.diavasi_report,
    error_buf: [*c]u8,
    error_buf_len: usize,
) callconv(.c) c_int {
    report.* = std.mem.zeroes(c.diavasi_report);
    const group = std.mem.span(options.*.group_id);
    if (std.mem.eql(u8, group, "zig-missing")) {
        setMessage(error_buf, error_buf_len, "not running");
        return 5;
    }

    const total: usize = @intCast(options.*.expect_records);
    if (total == 0) return 0;

    const records = std.c.malloc(total * @sizeOf(u64)) orelse {
        setMessage(error_buf, error_buf_len, "out of memory");
        return -1;
    };
    const ids: [*]u64 = @ptrCast(@alignCast(records));
    const payload = "abcdefgh";
    var i: usize = 0;
    while (i < total) : (i += 1) {
        const id: u64 = @intCast(i + 1);
        ids[i] = id;
        if (options.*.on_record) |callback| {
            callback(1, id, payload.ptr, payload.len, options.*.user);
        }
    }
    report.*.record_ids = ids;
    report.*.record_count = total;

    const batches = std.c.malloc(@sizeOf(u64)) orelse {
        std.c.free(records);
        setMessage(error_buf, error_buf_len, "out of memory");
        return -1;
    };
    const batch_ids: [*]u64 = @ptrCast(@alignCast(batches));
    batch_ids[0] = 1;
    report.*.batch_ids = batch_ids;
    report.*.batch_count = 1;
    return 0;
}

test "unit: report deinit frees both id slices" {
    const allocator = std.testing.allocator;
    const record_ids = try allocator.alloc(u64, 2);
    const batch_ids = try allocator.alloc(u64, 1);
    record_ids[0] = 1;
    record_ids[1] = 2;
    batch_ids[0] = 1;
    const report = Report{ .record_ids = record_ids, .batch_ids = batch_ids };
    report.deinit(allocator);
}

test "unit: failure deinit frees the message" {
    const allocator = std.testing.allocator;
    const failure = Failure{
        .code = 5,
        .message = try allocator.dupe(u8, "not running"),
    };
    failure.deinit(allocator);
}

test "unit: trampoline forwards the payload and an empty buffer" {
    const Seen = struct {
        batch: u64 = 0,
        record: u64 = 0,
        len: usize = 99,
        empty: bool = false,
    };
    var seen = Seen{};
    var hook = Hook{
        .func = struct {
            fn onRecord(batch_id: u64, record_id: u64, payload: []const u8, ctx: ?*anyopaque) void {
                const out: *Seen = @ptrCast(@alignCast(ctx.?));
                out.batch = batch_id;
                out.record = record_id;
                out.len = payload.len;
                out.empty = payload.len == 0;
            }
        }.onRecord,
        .ctx = &seen,
    };
    const payload = "ab";
    trampoline(3, 7, payload.ptr, payload.len, &hook);
    try std.testing.expectEqual(@as(u64, 3), seen.batch);
    try std.testing.expectEqual(@as(u64, 7), seen.record);
    try std.testing.expectEqual(@as(usize, 2), seen.len);

    trampoline(1, 0, null, 0, &hook);
    try std.testing.expect(seen.empty);
    try std.testing.expectEqual(@as(u64, 0), seen.record);
}

test "regression: a fresh group returns ids 1 through total and deinit frees them" {
    const allocator = std.testing.allocator;
    const total: u64 = 8;
    const Seen = struct {
        count: usize = 0,
        fn onRecord(_: u64, _: u64, payload: []const u8, ctx: ?*anyopaque) void {
            const out: *@This() = @ptrCast(@alignCast(ctx.?));
            if (payload.len == 8) out.count += 1;
        }
    };
    var seen = Seen{};
    const outcome = try finish(allocator, .{
        .addr = "127.0.0.1:7710",
        .ca_path = "ca.crt",
        .token = "sdk-demo",
        .group_id = "sdk",
        .consumer_id = "zig-test",
        .expect_records = total,
        .on_record = Seen.onRecord,
        .ctx = &seen,
    }, mockServer);
    switch (outcome) {
        .failure => |failure| {
            std.debug.print("consume failed {d}: {s}\n", .{ failure.code, failure.message });
            failure.deinit(allocator);
            return error.TestExpectedEqual;
        },
        .report => |report| {
            defer report.deinit(allocator);
            try std.testing.expectEqual(total, report.record_ids.len);
            try std.testing.expectEqual(seen.count, report.record_ids.len);
            for (report.record_ids, 0..) |id, index| {
                try std.testing.expectEqual(index + 1, id);
            }
            try std.testing.expectEqualSlices(u64, &.{1}, report.batch_ids);
        },
    }
}

test "regression: a group that is not running is protocol error 5" {
    const allocator = std.testing.allocator;
    const outcome = try finish(allocator, .{
        .addr = "127.0.0.1:7710",
        .ca_path = "ca.crt",
        .token = "sdk-demo",
        .group_id = "zig-missing",
        .consumer_id = "zig-missing",
        .expect_records = 1,
    }, mockServer);
    switch (outcome) {
        .report => |report| {
            report.deinit(allocator);
            return error.TestExpectedEqual;
        },
        .failure => |failure| {
            defer failure.deinit(allocator);
            try std.testing.expectEqual(@as(i32, 5), failure.code);
            try std.testing.expect(failure.message.len > 0);
        },
    }
}
