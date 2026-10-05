//! A keyed send carries its deduplication key as the record's key, the
//! same on every attempt of one Journey; an unkeyed send carries none.

use std::thread;

use transport::Transport;
use transport::loopback::LOOPBACK_TIMEOUT;
use xmip_core_transport_redpanda::{RedpandaTransport, Session};

/// A Journey's identifier, as the runtime hands it.
const KEY: &str = "0b6f5a52-7c1e-4d0a-9a4e-3f1d2c8b9e70";

#[test]
fn a_keyed_record_carries_the_journey_id_as_its_key_on_every_attempt() {
    let (listener, address) = RedpandaTransport::loopback().bind().expect("bound");
    let sender = thread::spawn(move || {
        let near = RedpandaTransport::new(address, "probe");
        near.send_keyed("probe", b"order", KEY)?;
        near.send_keyed("probe", b"order", KEY)?;
        near.send("probe", b"order")
    });
    let mut session = Session::accept(&listener, Some(LOOPBACK_TIMEOUT)).expect("accepted");
    for _ in 0..3 {
        session.next_produce().expect("read").expect("produced");
    }
    sender.join().expect("sender").expect("sent");
    let keys: Vec<Option<&[u8]>> = session
        .log("probe", 0)
        .iter()
        .map(|record| record.key.as_deref())
        .collect();
    let key = Some(KEY.as_bytes());
    assert_eq!(keys, [key, key, None]);
}
