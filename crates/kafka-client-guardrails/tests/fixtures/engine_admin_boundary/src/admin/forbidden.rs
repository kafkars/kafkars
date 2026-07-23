//! Deliberately invalid engine-admin ownership fixture.

use crate::{consumer, driver, producer, transaction};
use kafka_driver as native_driver;
use kafka_wire as generated_protocol;

fn touches_forbidden_owners() {
    let _ = (
        consumer::OWNER,
        driver::OWNER,
        producer::OWNER,
        transaction::OWNER,
        native_driver::OWNER,
        generated_protocol::OWNER,
    );
}
