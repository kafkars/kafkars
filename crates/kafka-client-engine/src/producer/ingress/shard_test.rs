//! Producer-shard linearity and thread-safety contracts.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::{ProducerAdmissionPort, ProducerShardOwner, ProducerShardWake, ProducerShardWakeError};
use crate::producer::host_limits_test::{start, valid_limits};

#[test]
fn admission_port_is_cloneable_send_and_sync_while_owner_remains_unique() {
    assert_send_sync::<ProducerAdmissionPort>();
    let wake = Arc::new(CountingWake::default());
    let owner = ProducerShardOwner::new(start(valid_limits()), Arc::clone(&wake));
    let first = owner.admission_port();
    let second = first.clone();

    assert_send_sync_value(first);
    assert_send_sync_value(second);
    assert_eq!(wake.count(), 0);
}

fn assert_send_sync<T: Send + Sync>() {}

fn assert_send_sync_value<T: Send + Sync>(_value: T) {}

#[derive(Default)]
pub(super) struct CountingWake {
    count: AtomicUsize,
}

impl CountingWake {
    pub(super) fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }
}

impl ProducerShardWake for CountingWake {
    fn wake(&self) -> Result<(), ProducerShardWakeError> {
        self.count.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}
