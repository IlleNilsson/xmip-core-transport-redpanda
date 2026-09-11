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
use std::sync::Mutex;
use std::time::Duration;

pub use admin::{Admin, AdminRequest, AdminSession, Broker, ConfigStatus};
pub use kafka::{Client, Event, Record, Session, TopicMetadata};
use transport::error::{Result, TransportError, protocol_error};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};

pub struct RedpandaTransport {
    broker: String,
    topic: String,
    partition: i32,
    admin: Option<String>,
    node: i64,
    client: String,
    cursor: Mutex<i64>,
    timeout: Option<Duration>,
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
            cursor: Mutex::new(0),
            timeout: None,
        }
    }

    /// Consult the Admin API at `address`, `host:9644`, before receiving.
    #[must_use]
    pub fn with_admin(mut self, address: impl Into<String>) -> Self {
        self.admin = Some(address.into());
        self
    }

    /// The node the broker is, as the Admin API numbers it; 0 until said.
    #[must_use]
    pub const fn on_node(mut self, node: i64) -> Self {
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
        *self
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = offset;
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
        *self
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Connect to the partition's leader, asking the configured broker who
    /// that is.
    ///
    /// # Errors
    /// Where no broker could be reached or the topic has no leader.
    pub fn connect(&self) -> Result<Client> {
        let mut client = Client::connect(&self.broker, &self.client, self.timeout)?;
        let metadata = client.metadata(&self.topic)?;
        if metadata.leader.is_empty() || metadata.leader == self.broker {
            return Ok(client);
        }
        Client::connect(&metadata.leader, &self.client, self.timeout)
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
    pub fn check_ready(&self) -> Result<()> {
        let Some(address) = &self.admin else {
            return Ok(());
        };
        let brokers = Admin::new(address, self.timeout).brokers()?;
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
        match socket::target("redpanda", target) {
            Some((peer, "")) => (peer, &self.topic),
            Some(pair) => pair,
            None => match target.split_once('/') {
                Some((peer, topic)) if peer.contains(':') => (peer, topic),
                _ => (&self.broker, target),
            },
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

    /// The records from the cursor on, the cursor moved past the last — once
    /// the Admin API, where consulted, says the node is taking work.
    fn receive(&self) -> Result<Vec<Arrived>> {
        self.check_ready()?;
        let mut client = self.connect()?;
        let records = client.fetch(&self.topic, self.partition, self.cursor())?;
        let mut arrived = Vec::with_capacity(records.len());
        for record in records {
            arrived.push(Arrived::new(
                format!(
                    "redpanda://{}/{}/{}?offset={}",
                    self.broker, self.topic, self.partition, record.offset
                ),
                record.value.unwrap_or_default(),
            ));
            *self
                .cursor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = record.offset + 1;
        }
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (broker, topic) = self.resolve(target);
        let mut client = Client::connect(broker, &self.client, self.timeout)?;
        client
            .produce(topic, self.partition, None, bytes)
            .map(|_| ())
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

/// A bound listener waiting for its one producer and its one record — on
/// the wire it is Kafka, so the session is that crate's. It holds the
/// timeout rather than the transport: the transport carries a cursor under
/// a lock, and a far end has no offset to keep.
struct Listening {
    timeout: Option<Duration>,
    listener: TcpListener,
    address: String,
}

impl FarEnd for Listening {
    fn address(&self) -> &str {
        &self.address
    }

    fn take_one(self: Box<Self>) -> Result<Arrived> {
        let mut session = Session::accept(&self.listener, self.timeout)?;
        session
            .next_produce()?
            .ok_or_else(|| protocol_error("the client closed without producing"))
    }
}

impl Loopback for RedpandaTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let (listener, address) = self.bind()?;
        Ok(Box::new(Listening {
            timeout: self.timeout,
            listener,
            address,
        }))
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

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
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
            let arrived = near.receive()?;
            Ok::<_, TransportError>((arrived, near.cursor()))
        });
        for expected in [&b"produced"[..], b"again"] {
            let mut session = far_end.accept_one(&listener).expect("accepting");
            let one = session.next_produce().expect("produce").expect("one");
            assert_eq!(one.bytes, expected);
            assert!(session.next_produce().expect("closed").is_none());
        }
        let mut session = far_end
            .accept_one(&listener)
            .expect("third")
            .with_records("orders", &[b"zero", b"one", b"two"]);
        while session.next_event().expect("serving").is_some() {}
        let (arrived, cursor) = near.join().expect("thread").expect("round trip");
        assert_eq!(arrived.len(), 2, "from offset 1");
        assert_eq!(arrived[0].bytes, b"one");
        assert!(arrived[0].origin_uri.starts_with("redpanda://127.0.0.1:"));
        assert!(arrived[1].origin_uri.ends_with("/orders/0?offset=2"));
        assert_eq!(cursor, 3);
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
                .with_admin(admin_address.clone())
                .timing_out_after(secs(2));
            let refused = near.receive().expect_err("draining");
            let arrived = near.receive()?;
            let unknown = RedpandaTransport::new("127.0.0.1:1", "orders")
                .with_admin(admin_address)
                .on_node(7)
                .timing_out_after(secs(2))
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
        assert_eq!(arrived[0].bytes, b"one");
        assert!(!unknown.retryable);
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
        for (name, payload) in edge_payloads() {
            let arrived = loopback.round(&payload).expect(name);
            assert!(arrived.bytes == payload, "{name} came back changed");
        }
    }

    /// The Playground's edge payloads, written here so the crate does not
    /// depend on it: the shapes a framing fault changes.
    fn edge_payloads() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
            ("mtu minus one", patterned(1_471)),
            ("mtu", patterned(1_472)),
            ("mtu plus one", patterned(1_473)),
            ("udp maximum", patterned(65_507)),
            ("sixteen bits plus one", patterned(65_537)),
            ("a mebibyte", patterned(1 << 20)),
        ]
    }

    /// `len` bytes a truncation, a reorder or a duplicate would change.
    fn patterned(len: usize) -> Vec<u8> {
        (0..len)
            .map(|at| u8::try_from((at * 31 + at / 251) % 256).unwrap_or(0))
            .collect()
    }
}
