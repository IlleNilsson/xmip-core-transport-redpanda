//! The Redpanda Admin API, on port 9644: what a Redpanda broker says about
//! itself that a Kafka broker does not. JSON over HTTP/1.1, one request per
//! connection: the client side is the estate's minimal HTTP/1.1 client,
//! `net::http`, and the far end reads `Content-Length` and nothing cleverer.
//!
//! Three calls, the ones a Location consults before taking work: the
//! brokers and whether each is alive or draining, the cluster
//! configuration's status, and putting a broker into or out of maintenance.
//! The far end, [`AdminSession`], answers those three from memory so a test
//! and the playground can stand in for a broker.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use net::NetError;
use net::http::{self, Request};
use serde_json::{Value, json};
use transport::error::{Result, TransportError, classify, protocol_error};
use transport::socket;
use transport::wire::{header, read_head};

/// One broker as `GET /v1/brokers` describes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Broker {
    pub node_id: i64,
    pub is_alive: bool,
    /// `active`, or `draining` while leaving.
    pub membership_status: String,
    /// In maintenance: leadership moved away, no work taken.
    pub draining: bool,
}

/// One node's view of the cluster configuration, from
/// `GET /v1/cluster_config/status`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigStatus {
    pub node_id: i64,
    /// Whether a change is waiting for a restart to take.
    pub restart: bool,
    pub config_version: i64,
    /// Properties the node could not read.
    pub invalid: Vec<String>,
}

/// The client's side: one request, one answer, over a fresh connection.
pub struct Admin {
    address: String,
    timeout: Option<Duration>,
}

impl Admin {
    /// Speak to the Admin API at `address`, `host:9644`.
    #[must_use]
    pub fn new(address: impl Into<String>, timeout: Option<Duration>) -> Self {
        Self {
            address: address.into(),
            timeout,
        }
    }

    /// Every broker in the cluster.
    ///
    /// # Errors
    /// Where the API could not be reached or answered something else.
    pub fn brokers(&self) -> Result<Vec<Broker>> {
        let answer = self.call("GET", "/v1/brokers")?;
        let brokers = answer
            .as_array()
            .ok_or_else(|| protocol_error("brokers that are not a list"))?;
        Ok(brokers
            .iter()
            .map(|b| Broker {
                node_id: b["node_id"].as_i64().unwrap_or(-1),
                is_alive: b["is_alive"].as_bool().unwrap_or(false),
                membership_status: b["membership_status"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                draining: b["maintenance_status"]["draining"]
                    .as_bool()
                    .unwrap_or(false),
            })
            .collect())
    }

    /// The configuration status of every node.
    ///
    /// # Errors
    /// Where the API could not be reached or answered something else.
    pub fn cluster_config_status(&self) -> Result<Vec<ConfigStatus>> {
        let answer = self.call("GET", "/v1/cluster_config/status")?;
        let nodes = answer
            .as_array()
            .ok_or_else(|| protocol_error("a status that is not a list"))?;
        Ok(nodes
            .iter()
            .map(|n| ConfigStatus {
                node_id: n["node_id"].as_i64().unwrap_or(-1),
                restart: n["restart"].as_bool().unwrap_or(false),
                config_version: n["config_version"].as_i64().unwrap_or(0),
                invalid: n["invalid"]
                    .as_array()
                    .map(|i| {
                        i.iter()
                            .filter_map(Value::as_str)
                            .map(String::from)
                            .collect()
                    })
                    .unwrap_or_default(),
            })
            .collect())
    }

    /// Put `node_id` into maintenance, or take it out.
    ///
    /// # Errors
    /// Where the API could not be reached or refused.
    pub fn maintenance(&self, node_id: i64, enable: bool) -> Result<()> {
        let method = if enable { "PUT" } else { "DELETE" };
        self.call(method, &format!("/v1/brokers/{node_id}/maintenance"))
            .map(|_| ())
    }

    fn call(&self, method: &str, path: &str) -> Result<Value> {
        let stream = socket::connect_tcp(&self.address, self.timeout)?;
        let request = Request::new(method, path).header("Accept", "application/json");
        let answer = http::exchange(stream, &self.address, &request).map_err(failed)?;
        let (code, body) = (answer.status, answer.body);
        if !(200..300).contains(&code) {
            let message = format!("the Admin API answered {method} {path} with {code}");
            return Err(if code >= 500 {
                TransportError::retryable(message)
            } else {
                TransportError::permanent(message)
            });
        }
        if body.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&body)
            .map_err(|e| protocol_error(format!("an answer that is not JSON: {e}")))
    }
}

/// The client's failure as the transport judges it: the connection's
/// retryable as its kind says, an answer that is not HTTP never.
fn failed(error: NetError) -> TransportError {
    match error.io {
        Some(kind) => classify(
            "asking the Admin API",
            &std::io::Error::new(kind, error.message),
        ),
        None => protocol_error(error.message),
    }
}

fn read_body(reader: &mut impl Read, lines: &[String]) -> Result<Vec<u8>> {
    let length: usize = header(lines, "content-length")
        .map(|l| {
            l.parse()
                .map_err(|_| protocol_error("a length that is not a number"))
        })
        .transpose()?
        .unwrap_or(0);
    if length > transport::wire::MAX_BODY {
        return Err(protocol_error("a body over what Xmip will read"));
    }
    let mut body = vec![0u8; length];
    reader
        .read_exact(&mut body)
        .map_err(|e| classify("reading the body", &e))?;
    Ok(body)
}

/// What a request to [`AdminSession`] asked, as it reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminRequest {
    pub method: String,
    pub path: String,
}

/// The API's side, from memory: one cluster of brokers, each in or out of
/// maintenance as the requests leave it.
pub struct AdminSession {
    brokers: Vec<Broker>,
    config_version: i64,
    timeout: Option<Duration>,
}

impl AdminSession {
    /// One broker, node 0, alive and active.
    #[must_use]
    pub fn new(timeout: Option<Duration>) -> Self {
        Self {
            brokers: vec![Broker {
                node_id: 0,
                is_alive: true,
                membership_status: "active".to_string(),
                draining: false,
            }],
            config_version: 1,
            timeout,
        }
    }

    /// These brokers rather than the one.
    #[must_use]
    pub fn with_brokers(mut self, brokers: Vec<Broker>) -> Self {
        self.brokers = brokers;
        self
    }

    /// The brokers as the requests so far have left them.
    #[must_use]
    pub fn brokers(&self) -> &[Broker] {
        &self.brokers
    }

    /// Bind the listener and report the address actually assigned.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(bind: &str) -> Result<(TcpListener, String)> {
        socket::bind_tcp(bind)
    }

    /// Accept one connection, answer its one request, and report what it
    /// asked.
    ///
    /// # Errors
    /// Where the connection could not be accepted or the request was not
    /// HTTP.
    pub fn serve_one(&mut self, listener: &TcpListener) -> Result<AdminRequest> {
        let (stream, _) = socket::accept_tcp(listener, self.timeout)?;
        let (mut reader, mut writer) = socket::split(stream)?;
        let lines = read_head(&mut reader)?;
        let mut words = lines
            .first()
            .map(|l| l.split_whitespace())
            .into_iter()
            .flatten();
        let (Some(method), Some(path)) = (words.next(), words.next()) else {
            return Err(protocol_error("a request line that is not HTTP"));
        };
        let request = AdminRequest {
            method: method.to_string(),
            path: path.to_string(),
        };
        read_body(&mut reader, &lines)?;
        let (code, body) = self.answer(&request);
        let body = body.to_string();
        let head = format!(
            "HTTP/1.1 {code} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n",
            if code == 200 { "OK" } else { "Not Found" },
            body.len()
        );
        writer
            .write_all(head.as_bytes())
            .and_then(|()| writer.write_all(body.as_bytes()))
            .map_err(|e| classify("writing the answer", &e))?;
        Ok(request)
    }

    fn answer(&mut self, request: &AdminRequest) -> (u16, Value) {
        let segments: Vec<&str> = request.path.trim_matches('/').split('/').collect();
        match (request.method.as_str(), segments.as_slice()) {
            ("GET", ["v1", "brokers"]) => (200, self.brokers_json()),
            ("GET", ["v1", "cluster_config", "status"]) => {
                let nodes: Vec<Value> = self
                    .brokers
                    .iter()
                    .map(|b| {
                        json!({
                            "node_id": b.node_id,
                            "restart": false,
                            "config_version": self.config_version,
                            "invalid": [],
                            "unknown": [],
                        })
                    })
                    .collect();
                (200, Value::Array(nodes))
            }
            ("PUT" | "DELETE", ["v1", "brokers", id, "maintenance"]) => {
                let id: i64 = id.parse().unwrap_or(-1);
                match self.brokers.iter_mut().find(|b| b.node_id == id) {
                    Some(broker) => {
                        broker.draining = request.method == "PUT";
                        (200, json!({}))
                    }
                    None => (404, json!({ "message": "node not found" })),
                }
            }
            _ => (404, json!({ "message": "not served here" })),
        }
    }

    fn brokers_json(&self) -> Value {
        let brokers: Vec<Value> = self
            .brokers
            .iter()
            .map(|b| {
                json!({
                    "node_id": b.node_id,
                    "num_cores": 1,
                    "membership_status": b.membership_status,
                    "is_alive": b.is_alive,
                    "maintenance_status": { "draining": b.draining, "finished": b.draining },
                })
            })
            .collect();
        Value::Array(brokers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn the_three_calls_are_answered_and_maintenance_sticks() {
        let (listener, address) = AdminSession::bind("127.0.0.1:0").expect("bind");
        let client = std::thread::spawn(move || {
            let admin = Admin::new(address, Some(secs(2)));
            let before = admin.brokers()?;
            admin.maintenance(0, true)?;
            let during = admin.brokers()?;
            admin.maintenance(0, false)?;
            let status = admin.cluster_config_status()?;
            let missing = admin.maintenance(9, true);
            Ok::<_, TransportError>((before, during, status, missing))
        });
        let mut session = AdminSession::new(Some(secs(2)));
        let mut asked = Vec::new();
        for _ in 0..6 {
            asked.push(session.serve_one(&listener).expect("serving"));
        }
        assert_eq!(asked[1].method, "PUT");
        assert_eq!(asked[1].path, "/v1/brokers/0/maintenance");
        assert_eq!(asked[3].method, "DELETE");
        assert!(!session.brokers()[0].draining);
        let (before, during, status, missing) = client.join().expect("thread").expect("calls");
        assert_eq!(before.len(), 1);
        assert!(before[0].is_alive && !before[0].draining);
        assert_eq!(before[0].membership_status, "active");
        assert!(during[0].draining);
        assert_eq!(status[0].config_version, 1);
        assert!(status[0].invalid.is_empty());
        assert!(!missing.expect_err("node 9").retryable);
    }

    #[test]
    fn what_is_not_the_admin_api_is_refused() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        std::thread::spawn(move || {
            for answer in [
                &b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nnot"[..],
                &b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"[..],
                &b"220 mail.example ESMTP\r\n\r\n"[..],
            ] {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut sink = [0u8; 1024];
                let _ = stream.read(&mut sink);
                stream.write_all(answer).expect("write");
            }
        });
        let admin = Admin::new(address, Some(secs(2)));
        assert!(!admin.brokers().expect_err("not json").retryable);
        assert!(admin.brokers().expect_err("503").retryable);
        assert!(!admin.brokers().expect_err("not http").retryable);
        let (listener, address) = AdminSession::bind("127.0.0.1:0").expect("bind");
        std::thread::spawn(move || {
            let mut stream = std::net::TcpStream::connect(address).expect("connect");
            stream.write_all(b"\r\n").expect("write");
        });
        let mut session = AdminSession::new(Some(secs(2)));
        assert!(
            !session
                .serve_one(&listener)
                .expect_err("no request line")
                .retryable
        );
    }
}
