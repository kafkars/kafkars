//! Stable registry-local failures before any group byte lease transfers.

use kafka_client_core::{ClassicProcessingLeaseError, ClassicProcessingLeaseExpiration};

use super::classic_group_fetch::ClassicGroupFetchDeliveryError;
use crate::{clock::ClockError, consumer::GroupConsumerPositionFailureKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerDeliveryError {
    UnknownGroup,
    Closing,
    EntryFault,
    PositionFailure(GroupConsumerPositionFailureKind),
    Fetch(ClassicGroupFetchDeliveryError),
    Clock {
        error: ClockError,
        delivery_retained: bool,
    },
    Processing {
        error: ClassicProcessingLeaseError,
        delivery_retained: bool,
    },
    ProcessingEffect {
        delivery_retained: bool,
    },
    ProcessingExpired {
        expiration: ClassicProcessingLeaseExpiration,
        delivery_retained: bool,
    },
}
