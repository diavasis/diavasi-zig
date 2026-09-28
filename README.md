# Zig client

[![CI](https://github.com/diavasis/diavasi-zig/actions/workflows/ci.yml/badge.svg)](https://github.com/diavasis/diavasi-zig/actions/workflows/ci.yml)
[![tag](https://img.shields.io/github/v/tag/diavasis/diavasi-zig)](https://github.com/diavasis/diavasi-zig/tags)
[![license](https://img.shields.io/github/license/diavasis/diavasi-zig)](https://github.com/diavasis/diavasi-zig/blob/main/LICENSE)

`consume` is a thin client of `diavasi.data.v1`. It opens a TLS stream, sends the bearer token, Hello version 1, then JoinGroup, and acks each batch. The caller stores no cursor and does not dedupe on `record_id`. A dropped stream is how unacked batches return. Reconnect with the same consumer id and the server replays them.

`proto/data.proto` in this repository is the copy of `diavasi.data.v1` from [github.com/diavasis/diavasi](https://github.com/diavasis/diavasi) tag `v0.13.0`. Version 0.1.0 is the git tag `v0.1.0`. The transport is the C library in the `c/` submodule, [diavasi-c](https://github.com/diavasis/diavasi-c). Install gRPC the same way as that repository's Install section so `pkg-config` finds `grpc`. Zig 0.16 is what this `build.zig` targets.

## Library

```zig
const diavasi = @import("diavasi");

fn onRecord(batch_id: u64, record_id: u64, payload: []const u8, ctx: ?*anyopaque) void {
    _ = ctx;
    std.debug.print("batch {d} record {d} ({d} bytes)\n", .{ batch_id, record_id, payload.len });
}

const outcome = try diavasi.consume(allocator, .{
    .addr = "127.0.0.1:7710",
    .ca_path = "/tmp/diavasi-sdk/dataplane-ca.crt",
    .token = "sdk-demo",
    .group_id = "demo",
    .consumer_id = "zig",
    .expect_records = 8,
    .on_record = onRecord,
});
switch (outcome) {
    .report => |report| {
        defer report.deinit(allocator);
        // report.record_ids and report.batch_ids
    },
    .failure => |failure| {
        defer failure.deinit(allocator);
        // failure.code is 1 through 8, or -1 for transport and auth
        // failure.message is the server or gRPC text
    },
}
```

`on_record` runs before the ack. `payload` is valid only for that call. `max_in_flight` defaults to 1. `halt_after_acks` of 0 is ignored. A positive value closes after that many acks and does not send Leave. `expect_records` of 0 reads until the stream ends. A positive value sends Leave once that many records are acked.

`failure.code` 1 through 8 is bad version, bad state, unknown ack, duplicate ack, group not running, unsupported, internal, heartbeat timeout. `-1` is transport or auth. A bad token is `grpc UNAUTHENTICATED: unauthorized`. A group that is not running is protocol code 5.

## Run

Start the server from the repo root:

```bash
cargo build -p diavasi
export PATH="$PWD/target/debug:$PATH"
mkdir -p /tmp/diavasi-sdk
diavasi serve --bind 127.0.0.1:7700 --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi-sdk/state --token sdk-demo
```

In a second terminal, from the repo root:

```bash
curl -fsS -X POST -H "Authorization: Bearer sdk-demo" \
  http://127.0.0.1:7700/v1/groups/demo/pause || true
curl -fsS -X DELETE -H "Authorization: Bearer sdk-demo" \
  http://127.0.0.1:7700/v1/groups/demo || true
curl -fsS -H "Authorization: Bearer sdk-demo" -H "content-type: application/json" \
  -d '{"group_id":"demo","total_records":8,"payload_size":8,"max_buffer_records":64,"max_buffer_bytes":65536,"batch_max_records":4,"batch_timeout_ms":200,"ordering_contract":"synthetic-u64"}' \
  http://127.0.0.1:7700/v1/groups
curl -fsS -X POST -H "Authorization: Bearer sdk-demo" \
  http://127.0.0.1:7700/v1/groups/demo/start

zig build run
```

Pause, delete, create, and start the group before another run. Delete returns 409 while it is running, and start resumes the cursor. A finished synthetic group leaves the client waiting on heartbeats.

## Test

`zig build test` runs the unit tests and the regressions. The regressions use an in-process stand-in for a data-plane session, so they do not need a running server. One checks that a fresh group yields record ids 1 through 8 and that `deinit` frees them. The other checks that a group that is not running is protocol error 5.

`src/main.zig` is that program. Optional flags, last occurrence wins: `--addr`, `--ca`, `--token`, `--group`, `--consumer`.

Another Zig program depends on the module by pointing `build.zig` at `src/root.zig` and linking `libgrpc` the same way `build.zig` in this repository does.
