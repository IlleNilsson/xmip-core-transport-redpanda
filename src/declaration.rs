//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`. Nothing here
//! is the kafka technology's: Redpanda rides on Kafka in its code alone.

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the broker,
/// `host:9092`: where a Location connects and asks for the partition's
/// leader.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "topic",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The topic a Receive Location fetches and a Send Location produces to \
                      when a target names no topic.",
            applies: Applies::Both,
        },
        Setting {
            name: "partition",
            kind: Kind::Integer {
                minimum: 0,
                maximum: 2_147_483_647,
            },
            presence: Presence::Optional,
            meaning: "The partition read and written; partition 0 when left out.",
            applies: Applies::Both,
        },
        Setting {
            name: "offset",
            kind: Kind::Integer {
                minimum: 0,
                maximum: i64::MAX,
            },
            presence: Presence::Optional,
            meaning: "The offset a Receive Location starts fetching from; the partition's \
                      beginning when left out.",
            applies: Applies::Receive,
        },
        Setting {
            name: "admin",
            kind: Kind::Address,
            presence: Presence::Optional,
            meaning: "The Admin API, `host:9644`, consulted before a receive takes work; \
                      not consulted when left out.",
            applies: Applies::Receive,
        },
        Setting {
            name: "node",
            kind: Kind::Integer {
                minimum: 0,
                maximum: i64::MAX,
            },
            presence: Presence::Optional,
            meaning: "The node the broker is, as the Admin API numbers it; node 0 when left \
                      out.",
            applies: Applies::Receive,
        },
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long a broker that stops mid-message is waited on; unbounded when \
             left out.",
        ),
    ],
};

/// This technology's schemes, as [`transport::Configured::SCHEMES`] defines
/// them.
pub const SCHEMES: &[&str] = &["redpanda"];
