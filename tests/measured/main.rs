//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): this transport held to how long Xmip takes — the millisecond rule
//! (`CONTRIBUTING.md`, *Rust is here for one reason*). A plain `cargo test`
//! leaves it out; the landing runs it after every other test of the module,
//! alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - a thousand fetches on one connection take a millisecond each at most.

use std::time::{Duration, Instant};

use transport::Transport;
use xmip_core_transport_redpanda::{Event, RedpandaTransport};

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[test]
fn a_thousand_fetches_take_a_millisecond_each_at_most() {
    const RECEIVES: usize = 1000;
    let far_end = RedpandaTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let near = RedpandaTransport::new(address, "orders").timing_out_after(secs(5));
    std::thread::scope(|scope| {
        let receiver = scope.spawn(|| {
            let began = Instant::now();
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
            Ok::<_, transport::TransportError>(arrived)
        });
        let mut session = far_end
            .accept_one(&listener)
            .expect("accepting")
            .with_records("orders", &[b"zero"]);
        for _ in 0..RECEIVES {
            let event = session.next_event().expect("fetch");
            assert!(matches!(event, Some(Event::Fetched { .. })), "{event:?}");
        }
        assert_eq!(receiver.join().expect("thread").expect("fetched"), 1);
    });
}
