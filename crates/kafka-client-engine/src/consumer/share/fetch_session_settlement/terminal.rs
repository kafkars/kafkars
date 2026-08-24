//! Terminal share-fetch outcomes retained across settlement and public observation.

use core::num::NonZeroI16;

use kafka_client_core::{DeliveryStatus, ShareFetchSettlementErrorKind};

use crate::{
    driver::{ShareFetchFailureKind, ShareFetchRoute},
    protocol::consumer::share_fetch::ShareFetchEndpoint,
};

use super::super::{
    fetch_acquisition_decode::ShareFetchAcquisitionDecodeError,
    fetch_delivery::ShareFetchDeliveryPartition, fetch_session::ShareFetchSessionOwnerError,
    fetch_session_set::ShareFetchSessionRecovery,
};

/// Decoded records and route receipt retained after atomic core admission.
#[must_use = "staged share delivery must be exposed or released"]
pub(in crate::consumer::share) struct StagedShareFetchDelivery {
    pub(in crate::consumer::share) fence: kafka_client_core::ShareFetchSessionFence,
    pub(in crate::consumer::share) route: ShareFetchRoute,
    pub(in crate::consumer::share) throttle_time_ms: u32,
    pub(in crate::consumer::share) endpoints: Vec<ShareFetchEndpoint>,
    pub(in crate::consumer::share) partitions: Vec<ShareFetchDeliveryPartition>,
    pub(in crate::consumer::share) acquisitions: usize,
}

pub(in crate::consumer::share) enum ShareFetchSettlementTurn {
    Empty,
    Acquired(usize),
    Recover(ShareFetchSessionRecovery),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareFetchTerminalSettlementError {
    Occupied,
    MissingTerminal,
    MissingLockTimeout,
    LockDeadlineOverflow,
    ThrottleDeadlineOverflow,
    Decode(ShareFetchAcquisitionDecodeError),
    Core(ShareFetchSettlementErrorKind),
    BrokerRejected(NonZeroI16),
    Driver {
        kind: ShareFetchFailureKind,
        delivery: DeliveryStatus,
    },
    Session(ShareFetchSessionOwnerError),
}

impl ShareFetchTerminalSettlementError {
    pub(in crate::consumer::share) const fn kind(&self) -> ShareFetchTerminalSettlementErrorKind {
        match self {
            Self::Occupied => ShareFetchTerminalSettlementErrorKind::Occupied,
            Self::MissingTerminal => ShareFetchTerminalSettlementErrorKind::MissingTerminal,
            Self::MissingLockTimeout => ShareFetchTerminalSettlementErrorKind::MissingLockTimeout,
            Self::LockDeadlineOverflow => {
                ShareFetchTerminalSettlementErrorKind::LockDeadlineOverflow
            }
            Self::ThrottleDeadlineOverflow => {
                ShareFetchTerminalSettlementErrorKind::ThrottleDeadlineOverflow
            }
            Self::Decode(_) => ShareFetchTerminalSettlementErrorKind::Decode,
            Self::Core(kind) => ShareFetchTerminalSettlementErrorKind::Core(*kind),
            Self::BrokerRejected(code) => {
                ShareFetchTerminalSettlementErrorKind::BrokerRejected(code.get())
            }
            Self::Driver { kind, delivery } => ShareFetchTerminalSettlementErrorKind::Driver {
                kind: *kind,
                delivery: *delivery,
            },
            Self::Session(error) => ShareFetchTerminalSettlementErrorKind::Session(*error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareFetchTerminalSettlementErrorKind {
    Occupied,
    MissingTerminal,
    MissingLockTimeout,
    LockDeadlineOverflow,
    ThrottleDeadlineOverflow,
    Decode,
    Core(ShareFetchSettlementErrorKind),
    BrokerRejected(i16),
    Driver {
        kind: ShareFetchFailureKind,
        delivery: DeliveryStatus,
    },
    Session(ShareFetchSessionOwnerError),
}
