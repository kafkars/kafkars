//! Producer close ownership after post-acceptance wake failure.

use std::{io, sync::Arc};

use kafka_client_core::Moment;

use super::{
    ProducerPortAcceptedFault, ProducerPortFlushError, ProducerShardOwner, ProducerShardWake,
};
use crate::producer::host_limits_test::{start, valid_limits};

#[test]
fn wake_failure_keeps_the_clone_shared_close_fence() {
    let wake = Arc::new(FailingWake);
    let owner = ProducerShardOwner::new(start(valid_limits()), wake);
    let port = owner.admission_port();
    let clone = port.clone();
    let accepted = port
        .try_admit_close(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("close must remain accepted: {error:?}"));
    let (observer, flush_id, fault) = accepted.into_parts();

    assert!(flush_id.is_some());
    assert!(matches!(fault, Err(ProducerPortAcceptedFault::Wake(_))));
    assert!(matches!(
        clone.try_admit_close(Moment::from_tick(2)),
        Err(ProducerPortFlushError::Rejected(
            super::super::flush::FlushRejectionReason::Closed
        ))
    ));
    assert!(matches!(
        clone.try_admit_flush(Moment::from_tick(2)),
        Err(ProducerPortFlushError::Rejected(
            super::super::flush::FlushRejectionReason::Closed
        ))
    ));
    assert_eq!(observer.wait(), Ok(()));
}

struct FailingWake;

impl ProducerShardWake for FailingWake {
    fn wake(&self) -> Result<(), super::ProducerShardWakeError> {
        Err(super::ProducerShardWakeError::from_io(io::Error::other(
            "close wake failed",
        )))
    }
}
