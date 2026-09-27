# xmip-core-transport-redpanda

Redpanda transport: the Kafka wire protocol to a Redpanda broker, with the
Admin API — brokers, cluster configuration, maintenance — consulted before a
Location takes work. A technology of
[xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location produces on a connection kept per broker (`transport::Pool`). The Admin API is asked on the http technology's kept connections (`endpoint::Connections`) and its status judged by that technology's one rule (`status::judge`); until 2026-09-27 it opened a connection a call and carried a 5xx rule of its own.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
