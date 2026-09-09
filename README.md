# xmip-core-transport-redpanda

Redpanda transport: the Kafka wire protocol to a Redpanda broker, with the
Admin API — brokers, cluster configuration, maintenance — consulted before a
Location takes work. A technology of
[xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
