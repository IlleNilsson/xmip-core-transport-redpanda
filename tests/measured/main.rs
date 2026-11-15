//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): this transport held to how long Xmip takes — the millisecond rule
//! (`CONTRIBUTING.md`, *Rust is here for one reason*). A plain `cargo test`
//! leaves it out; the landing runs it after every other test of the module,
//! alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - a thousand fetches on one connection take a millisecond each at most
//!   beyond their wakes, round by round beside a plain wake, the fastest of
//!   five (`xcore::latency::Rounds`).

use std::assert_matches;
use std::time::Duration;

use transport::Transport;
use xcore::latency::Rounds;
use xmip_core_transport_redpanda::{Event, RedpandaTransport};

/// What a fetch wakes where a plain wake wakes one: the far end at the
/// request, and this end at its answer.
const WAKES: u32 = 2;

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
            let (mut rounds, mut arrived) = (Rounds::crossing(WAKES), 0);
            for _ in 0..RECEIVES {
                arrived += rounds.round(|| {
                    let mut taken = 0;
                    for record in near.receive()? {
                        record.taken()?;
                        taken += 1;
                    }
                    Ok::<_, transport::TransportError>(taken)
                })?;
            }
            Ok::<_, transport::TransportError>((rounds, arrived))
        });
        let mut session = far_end
            .accept_one(&listener)
            .expect("accepting")
            .with_records("orders", &[b"zero"]);
        for _ in 0..RECEIVES {
            let event = session.next_event().expect("fetch");
            assert_matches!(event, Some(Event::Fetched { .. }), "{event:?}");
        }
        let (rounds, arrived) = receiver.join().expect("thread").expect("fetched");
        assert_eq!(arrived, 1);
        rounds.hold("a fetch");
    });
}
