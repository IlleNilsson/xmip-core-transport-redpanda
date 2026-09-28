# xmip-core-transport-redpanda

Redpanda transport: the Kafka wire protocol to a Redpanda broker, with the
Admin API — brokers, cluster configuration, maintenance — consulted before a
Location takes work. A technology of
[xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location produces on a connection kept per broker (`transport::Pool`). The Admin API is asked on the http technology's kept connections (`endpoint::Connections`) and its status judged by that technology's one rule (`status::judge`); until 2026-09-27 it opened a connection a call and carried a 5xx rule of its own.

A Receive Location fetches on a connection to the partition's leader, found and connected on its first receive and kept; the Admin API is still asked before each receive, since whether a node is in maintenance is what it says now. Until 2026-09-28 every receive asked for metadata and connected. The leader is found by the kafka technology's `Client::to_leader`; until 2026-09-28 this crate carried a copy of it.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
