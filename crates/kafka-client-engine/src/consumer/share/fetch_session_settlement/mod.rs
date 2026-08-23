//! Declarative facade for staged share-fetch settlement and retirement.

mod retirement;
mod settlement;

pub(super) use settlement::{ShareFetchTerminalSettlementErrorKind, StagedShareFetchDelivery};

#[cfg(test)]
pub(super) use settlement::{ShareFetchSettlementTurn, ShareFetchTerminalSettlementError};

#[cfg(test)]
mod retirement_test;
#[cfg(test)]
pub(super) mod settlement_test;
