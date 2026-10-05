# xmip-core-transport-redpanda

Redpanda transport: the Kafka wire protocol to a Redpanda broker, with the
Admin API — brokers, cluster configuration, maintenance — consulted before a
Location takes work. A technology of
[xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location produces on a connection kept per broker (`transport::Pool`). The Admin API is asked on the http technology's kept connections (`endpoint::Connections`) and its status judged by that technology's one rule (`status::judge`); until 2026-09-27 it opened a connection a call and carried a 5xx rule of its own.

A Receive Location fetches on a connection to the partition's leader, found and connected on its first receive and kept; the Admin API is still asked before each receive, since whether a node is in maintenance is what it says now. Until 2026-09-28 every receive asked for metadata and connected. The leader is found by the kafka technology's `Client::to_leader`; until 2026-09-28 this crate carried a copy of it.

A fetched record is acknowledged after the runtime's whole receive cycle, by the capability's `transport::contiguous::Contiguous`, as kafka's is: `Accepted` moves the cursor past the record where the cursor stands at it, `Refused` moves it the same way, since a log has no place to reject a record into and a refused record is not read again, `Failed` leaves it, and the failed record and those after it are fetched again — at least once, never a skip. No consumer group is kept, so the acknowledgement is an in-memory step with no broker round trip. Until 2026-10-02 a fetch moved the cursor past what it fetched.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## The deduplication key

A keyed send (`Transport::send_keyed`, built 2026-10-04) carries the Journey's identifier as the record's key, as the kafka technology does, the same on every attempt of one Journey: what a consumer recognises a repeat by and what a compacted topic keeps one record of, Redpanda appending a repeated record as Kafka does. An unkeyed `send` writes a null key.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
