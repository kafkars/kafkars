//! Driver, runtime, thread, and fresh-deadline theft forbidden by this fixture.

use kafka_driver as raw_driver;
use std::{thread, time::Duration};

fn steal(
    _driver: raw_driver::Driver,
    _runtime: tokio::runtime::Runtime,
    _capture: DeadlineCapture,
) {
    let _worker = thread::spawn(|| ());
    let _fresh = Duration::from_nanos(1);
}

struct DeadlineCapture;
