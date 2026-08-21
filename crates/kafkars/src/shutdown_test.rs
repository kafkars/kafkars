//! Public shutdown admission fencing and terminal observation scenarios.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use crate::{Client, ErrorKind, Record};

#[test]
fn public_shutdown_fences_new_work_before_returning_its_observer() {
    fn require_send_future<T: Future + Send>() {}
    require_send_future::<crate::Shutdown>();

    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("build client: {error}"));
    let producer = client
        .producer()
        .delivery_timeout(Duration::from_millis(50))
        .build()
        .unwrap_or_else(|error| panic!("build producer: {error}"));

    let shutdown = client.shutdown();

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let rejection = producer.try_send(Record::to("orders").partition(0));
        match rejection {
            Err(error) if error.error().kind() == ErrorKind::Backpressure => {
                assert!(
                    Instant::now() < deadline,
                    "shutdown fencing must outlive transient owner contention"
                );
                std::hint::spin_loop();
            }
            Err(error) => {
                assert_eq!(error.error().kind(), ErrorKind::State);
                break;
            }
            Ok(_delivery) => panic!("shutdown call boundary must fence producer admission"),
        }
    }
    assert!(shutdown.wait().is_ok());
    assert!(client.shutdown().wait().is_ok());
}
