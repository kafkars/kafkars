//! Invalid sibling, driver, runtime, and generic-policy imports.

use crate::{admin, consumer, driver, producer};
use async_std;
use kafka_driver;
use kafka_wire;
use smol;
use tokio;

struct Transport;
struct Retry;

fn steal(_transport: Transport, _retry: Retry) {}
