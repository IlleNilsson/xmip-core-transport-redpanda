//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48). Nothing here is the kafka technology's: Redpanda rides on
//! Kafka in its code alone.

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

/// The schemes a Location's address is entered with to choose this technology.
pub const SCHEMES: &[&str] = &["redpanda"];
