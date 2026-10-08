#![forbid(unsafe_code)]

//! Streams that arrive as Redpanda records. One record is one Stream, the
//! topic, partition and offset kept beside it.
//!
//! Redpanda is a Kafka broker written to run on one binary without a
//! coordinator, and on the wire it is Kafka: Metadata, Produce and Fetch on
//! port 9092, exactly as the kafka technology speaks them, so the client and
//! the far end here are that crate's. What Redpanda adds is the Admin API on
//! port 9644 — which brokers there are and whether each is alive or in
//! maintenance, the state of the cluster configuration — and a Receive
//! Location that knows the address consults it before taking work: a broker
//! being drained hands its leadership away, and a fetch made then is a
//! fetch made twice.
//!
//! Neither crate creates topics; a topic that is not there is the broker's
//! to create on first use, or the operator's. TLS and SASL are the transport
//! capability's, per ADR-0033.
//!
//! The origin URI carries what the fetch knew:
//! `redpanda://broker/orders/0?offset=41`. A send target is
//! `redpanda://host:9092/orders`, `host:9092/orders`, or a topic alone on
//! the configured broker.

pub mod admin;

use std::net::TcpListener;
use std::time::Duration;

pub use admin::{Admin, AdminRequest, AdminSession, Broker, ConfigStatus};
pub use kafka::{Client, Event, Record, Session, TopicMetadata};
use net::Target;
use transport::ArrivalIdentity;
use transport::contiguous::Contiguous;
use transport::error::{Result, TransportError};
use transport::listening::Listening;
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Configured, Directions, Headers, Pool, Transport};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

pub struct RedpandaTransport {
    broker: String,
    topic: String,
    partition: i32,
    admin: Option<Admin>,
    node: i64,
    client: String,
    /// The offset the next receive reads from, which a record's
    /// acknowledgement moves.
    cursor: Contiguous<i64>,
    timeout: Option<Duration>,
    /// The connections a send produces on, connected once per broker and
    /// kept.
    producers: Pool<Client>,
    /// The connection a receive fetches on, to the partition's leader:
    /// connected on the first receive and kept.
    fetchers: Pool<Client>,
}

impl RedpandaTransport {
    /// Speak to the broker at `broker` about `topic`, partition 0, reading
    /// from its beginning.
    #[must_use]
    pub fn new(broker: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            broker: broker.into(),
            topic: topic.into(),
            partition: 0,
            admin: None,
            node: 0,
            client: "xmip".to_string(),
            cursor: Contiguous::new(0),
            timeout: None,
            producers: Pool::new(),
            fetchers: Pool::new(),
        }
    }

    /// Consult the Admin API at `address`, `host:9644`, before receiving,
    /// on connections kept to it. The timeout is the one set before.
    ///
    /// # Errors
    /// Where `address` is not a host and port.
    fn with_admin(mut self, address: &str) -> Result<Self> {
        self.admin = Some(Admin::new(address, self.timeout)?);
        Ok(self)
    }

    /// The node the broker is, as the Admin API numbers it; 0 until said.
    #[must_use]
    const fn on_node(mut self, node: i64) -> Self {
        self.node = node;
        self
    }

    /// This partition rather than 0.
    #[must_use]
    pub const fn on_partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }

    /// Start reading from this offset rather than the beginning.
    #[must_use]
    pub fn from_offset(self, offset: i64) -> Self {
        self.cursor.set(offset);
        self
    }

    /// Give up on a broker that stops mid-message.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The offset the next receive reads from.
    #[must_use]
    pub fn cursor(&self) -> i64 {
        self.cursor.at()
    }

    /// Connect to the partition's leader, asking the configured broker who
    /// that is.
    ///
    /// # Errors
    /// Where no broker could be reached or the topic has no leader.
    pub fn connect(&self) -> Result<Client> {
        Client::to_leader(&self.broker, &self.topic, &self.client, self.timeout)
    }

    /// Bind as the far end clients connect to, and report the address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.broker)
    }

    /// Accept one client on an already-bound listener.
    ///
    /// # Errors
    /// Where the connection could not be accepted.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Session> {
        Session::accept(listener, self.timeout)
    }

    /// Ask the Admin API, where one is configured, whether this node is fit
    /// to take work.
    ///
    /// # Errors
    /// Retryable where the node is draining or not alive — it will be back;
    /// permanent where the API knows no such node.
    fn check_ready(&self) -> Result<()> {
        let Some(admin) = &self.admin else {
            return Ok(());
        };
        let brokers = admin.brokers()?;
        let broker = brokers
            .iter()
            .find(|b| b.node_id == self.node)
            .ok_or_else(|| {
                TransportError::permanent(format!("the Admin API knows no node {}", self.node))
            })?;
        if broker.draining {
            return Err(TransportError::retryable(format!(
                "node {} is in maintenance",
                self.node
            )));
        }
        if !broker.is_alive {
            return Err(TransportError::retryable(format!(
                "node {} is not alive",
                self.node
            )));
        }
        Ok(())
    }

    /// Where a target names the broker and topic itself, or is a topic
    /// alone on this transport's broker.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str) {
        match Target::naming_server(&["redpanda"], target) {
            Some(named) if named.path().is_empty() => (named.authority(), &self.topic),
            Some(named) => (named.authority(), named.path()),
            None => (&self.broker, target),
        }
    }
}

impl Transport for RedpandaTransport {
    fn name(&self) -> &'static str {
        "redpanda"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered("a cursor moves only contiguously")
    }

    /// The records from the cursor on — once the Admin API, where
    /// consulted, says the node is taking work — fetched on the connection
    /// the first receive opened to the leader and kept. Nothing moves the
    /// cursor here: a record's acknowledgement does
    /// ([`transport::contiguous::Contiguous`]) — `Accepted` past that record
    /// where the cursor stands at it, `Refused` the same way, as a log has no
    /// place to reject a record into and a refused one is not read again,
    /// `Failed` not at all, so the failed record is fetched again.
    fn receive(&self) -> Result<Vec<Arrived>> {
        self.check_ready()?;
        let records = self.fetchers.exchange(
            self.broker.as_str(),
            || self.connect(),
            |client| client.fetch(&self.topic, self.partition, self.cursor()),
        )?;
        let mut arrived = Vec::with_capacity(records.len());
        for record in records {
            let offset = record.offset;
            arrived.push(
                Arrived::whole(
                    format!(
                        "redpanda://{}/{}/{}?offset={offset}",
                        self.broker, self.topic, self.partition
                    ),
                    record.value.unwrap_or_default(),
                    self.cursor.advancing(offset, offset + 1),
                )
                .detected()
                .with_headers(Headers::of("kafka").octets(record.headers)),
            );
        }
        Ok(arrived)
    }

    /// Produce on the connection kept for the broker, connected on the
    /// first send to it, acknowledged by the leader.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        self.produce(target, bytes, None)
    }

    /// The key is the record's key, as the kafka technology puts it: what
    /// a consumer recognises a repeat by, and what a compacted topic keeps
    /// one record of.
    fn send_keyed(&self, target: &str, bytes: &[u8], key: &str) -> Result<()> {
        self.produce(target, bytes, Some(key))
    }
}

impl RedpandaTransport {
    /// The one send: one record on the target's topic, under `key` where
    /// there is one, on the connection kept for its broker.
    fn produce(&self, target: &str, bytes: &[u8], key: Option<&str>) -> Result<()> {
        let (broker, topic) = self.resolve(target);
        self.producers.exchange(
            broker,
            || Client::connect(broker, &self.client, self.timeout),
            |client| {
                client
                    .produce(topic, self.partition, key.map(str::as_bytes), bytes)
                    .map(|_| ())
            },
        )
    }
}

impl Configured for RedpandaTransport {
    /// The address is the broker, `host:9092`: where a Location connects
    /// and asks for the partition's leader.
    const SETTINGS: &'static Settings = &Settings {
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
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long a broker that stops mid-message is waited on; unbounded when \
                          left out.",
                applies: Applies::Both,
            },
        ],
    };

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let mut transport = Self::new(address, settings.text("topic"));
        if let Some(partition) = settings.optional_integer("partition") {
            let partition = i32::try_from(partition)
                .map_err(|_| TransportError::permanent("the partition is out of range"))?;
            transport = transport.on_partition(partition);
        }
        if let Some(offset) = settings.optional_integer("offset") {
            transport = transport.from_offset(offset);
        }
        if let Some(node) = settings.optional_integer("node") {
            transport = transport.on_node(node);
        }
        if let Some(timeout) = settings.optional_duration("timeout") {
            transport = transport.timing_out_after(timeout);
        }
        // After the timeout, which the Admin API is asked within.
        if let Some(admin) = settings.optional_text("admin") {
            transport = transport.with_admin(admin)?;
        }
        Ok(transport)
    }
}

impl RedpandaTransport {
    /// Both ends on this machine: an ephemeral local port, the loopback
    /// timeout, one topic called `probe`. The Admin API is not consulted; a
    /// send never does.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "probe").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Loopback for RedpandaTransport {
    fn arrival_identity(&self) -> ArrivalIdentity {
        ArrivalIdentity::Unnamed(
            "the broker delivers it: its headers say who sent it, the peer is the broker",
        )
    }

    /// On the wire it is Kafka, so the far end is that crate's: one
    /// producer, one record.
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(
            kafka::producing(self.timeout),
            self.bind()?,
        )))
    }

    /// A fresh producer to `address`, one record on this transport's topic,
    /// acknowledged by the leader before it returns.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        let mut near = Self::new(address, &self.topic).on_partition(self.partition);
        near.timeout = self.timeout;
        near.send(&self.topic, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::Refusal;
    use transport::arrived::next_arrival;
    use transport::payload::{edge_payloads, sized_payloads};

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn redpanda_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(RedpandaTransport::SETTINGS.problems(), Vec::<String>::new());
        let given = [
            ("topic".to_string(), Given::Text("orders".to_string())),
            ("partition".to_string(), Given::Integer(3)),
            ("offset".to_string(), Given::Integer(41)),
            ("admin".to_string(), Given::Text("broker:9644".to_string())),
            ("node".to_string(), Given::Integer(2)),
            ("timeout".to_string(), Given::Text("2s".to_string())),
        ];
        let built =
            RedpandaTransport::open("broker:9092", Applies::Receive, &given).expect("configured");
        assert_eq!(built.topic, "orders");
        assert_eq!(built.partition, 3);
        assert_eq!(built.cursor(), 41);
        assert_eq!(
            built.admin.as_ref().map(Admin::address).as_deref(),
            Some("broker:9644")
        );
        assert_eq!(built.node, 2);
        assert_eq!(built.timeout, Some(secs(2)));
        let Err(refused) = RedpandaTransport::open("broker:9092", Applies::Send, &[]) else {
            panic!("the topic is required");
        };
        assert!(refused.message.contains("\"topic\""), "{refused}");
    }

    #[test]
    fn the_transport_sends_and_receives_over_kafka_with_its_own_origin() {
        let far_end = RedpandaTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let near = std::thread::spawn(move || {
            let near = RedpandaTransport::new(address.clone(), "orders")
                .from_offset(1)
                .timing_out_after(secs(2));
            near.send(&format!("redpanda://{address}/orders"), b"produced")?;
            near.send(&format!("{address}/orders"), b"again")?;
            let mut arrived = near.receive()?.into_iter();
            let one = arrived.next().expect("offset 1").taken()?;
            arrived
                .next()
                .expect("offset 2")
                .refused(Refusal::Forbidden)?;
            let refused = near.cursor();
            arrived.next().expect("offset 3").failed()?;
            let failed = near.cursor();
            let again = next_arrival(near.receive()?, "offset 3 again")?.taken()?;
            Ok::<_, TransportError>((one, [refused, failed, near.cursor()], again))
        });
        // One broker, so one connection for both sends; the receive opens
        // its own.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        for expected in [&b"produced"[..], b"again"] {
            let one = session.next_produce().expect("produce").expect("one");
            assert_eq!(one.bytes, expected);
        }
        drop(session);
        let mut session = far_end
            .accept_one(&listener)
            .expect("third")
            .with_records("orders", &[b"zero", b"one", b"two", b"three"]);
        let mut fetched_from = Vec::new();
        while let Some(event) = session.next_event().expect("serving") {
            if let Event::Fetched { offset, .. } = event {
                fetched_from.push(offset);
            }
        }
        let (one, cursors, again) = near.join().expect("thread").expect("round trip");
        assert_eq!(one.bytes, b"one");
        assert!(one.origin_uri.starts_with("redpanda://127.0.0.1:"));
        assert_eq!(
            cursors,
            [3, 3, 4],
            "a refused offset 2 moves the cursor past it, a failed offset 3 leaves it"
        );
        assert_eq!(again.bytes, b"three");
        assert!(again.origin_uri.ends_with("/orders/0?offset=3"));
        assert_eq!(
            fetched_from,
            [1, 3],
            "only the failed record is fetched again"
        );
        assert!(far_end.claims().is_none());
        assert_eq!(far_end.name(), "redpanda");
    }

    #[test]
    fn a_node_in_maintenance_takes_no_work_until_it_is_out() {
        let (admin_listener, admin_address) = AdminSession::bind("127.0.0.1:0").expect("bind");
        let far_end = RedpandaTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let near = std::thread::spawn(move || {
            let near = RedpandaTransport::new(address, "orders")
                .timing_out_after(secs(2))
                .with_admin(&admin_address)?;
            let refused = near.receive().expect_err("draining");
            let arrived = near
                .receive()?
                .into_iter()
                .map(Arrived::taken)
                .collect::<Result<Vec<_>>>()?;
            assert_eq!(arrived[0].bytes, b"one");
            // Its kept connection closes with it, which ends the serving.
            drop(near);
            let unknown = RedpandaTransport::new("127.0.0.1:1", "orders")
                .on_node(7)
                .timing_out_after(secs(2))
                .with_admin(&admin_address)?
                .check_ready()
                .expect_err("no node 7");
            Ok::<_, TransportError>((refused, arrived, unknown))
        });
        let mut admin = AdminSession::new(Some(secs(2))).with_brokers(vec![Broker {
            node_id: 0,
            is_alive: true,
            membership_status: "active".to_string(),
            draining: true,
        }]);
        admin.serve_one(&admin_listener).expect("first look");
        admin
            .with_brokers(vec![Broker {
                node_id: 0,
                is_alive: true,
                membership_status: "active".to_string(),
                draining: false,
            }])
            .serve_one(&admin_listener)
            .expect("second look");
        let mut session = far_end
            .accept_one(&listener)
            .expect("accepting")
            .with_records("orders", &[b"one"]);
        while session.next_event().expect("serving").is_some() {}
        AdminSession::new(Some(secs(2)))
            .serve_one(&admin_listener)
            .expect("third look");
        let (refused, arrived, unknown) = near.join().expect("thread").expect("round trip");
        assert!(refused.retryable);
        assert!(refused.message.contains("in maintenance"));
        assert_eq!(arrived.len(), 1);
        assert!(!unknown.retryable);
    }

    #[test]
    fn a_thousand_receives_connect_once_and_a_connection_the_broker_closed_is_replaced() {
        const RECEIVES: usize = 1000;
        let far_end = RedpandaTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = RedpandaTransport::new(address, "orders").timing_out_after(secs(5));
        let fetched = |session: &mut Session, from: i64| {
            let event = session.next_event().expect("fetch");
            assert!(
                matches!(event, Some(Event::Fetched { offset, .. }) if offset == from),
                "{event:?}"
            );
        };
        std::thread::scope(|scope| {
            let receiver = scope.spawn(|| {
                let began = std::time::Instant::now();
                let mut arrived = 0;
                for _ in 0..RECEIVES {
                    for record in near.receive()? {
                        record.taken()?;
                        arrived += 1;
                    }
                }
                let took = began.elapsed();
                // Generous for a debug build under load: a millisecond a fetch.
                assert!(took < Duration::from_millis(RECEIVES as u64), "{took:?}");
                for record in near.receive()? {
                    record.taken()?;
                    arrived += 1;
                }
                Ok::<_, transport::TransportError>(arrived)
            });
            // One connection for every fetch: one session accepted.
            let mut session = far_end
                .accept_one(&listener)
                .expect("accepting")
                .with_records("orders", &[b"zero"]);
            fetched(&mut session, 0);
            for _ in 1..RECEIVES {
                fetched(&mut session, 1);
            }
            drop(session);
            let mut again = far_end
                .accept_one(&listener)
                .expect("a new connection")
                .with_records("orders", &[b"zero", b"one"]);
            fetched(&mut again, 1);
            assert_eq!(receiver.join().expect("thread").expect("fetched"), 2);
        });
        assert_eq!(near.cursor(), 2);
        assert_eq!(near.fetchers.opened(), 2);
    }

    #[test]
    fn the_loopback_round_returns_the_payload_and_its_origin() {
        let loopback = RedpandaTransport::loopback();
        let arrived = loopback.round(b"record").expect("round");
        assert_eq!(arrived.bytes, b"record");
        assert!(arrived.origin_uri.ends_with("/probe/0?offset=0"));
        assert!(loopback.ceiling().is_none());
        assert!(loopback.refuses(b"anything").is_none());
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let loopback = RedpandaTransport::loopback();
        for (name, payload) in [edge_payloads(), sized_payloads()].concat() {
            let arrived = loopback.round(&payload).expect(name);
            assert!(arrived.bytes == payload, "{name} came back changed");
        }
    }
}
