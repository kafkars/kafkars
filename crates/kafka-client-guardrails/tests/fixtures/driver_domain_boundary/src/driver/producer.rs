//! Deliberate shared-driver dependency on producer policy.

use crate::producer::ProducerShardWake;

fn leak(value: &dyn ProducerShardWake) {
    let _value = value;
}
