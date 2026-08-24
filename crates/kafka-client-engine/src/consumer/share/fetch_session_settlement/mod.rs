//! Declarative facade for staged share-fetch settlement and retirement.

mod recovery;
mod retirement;
mod settlement;
mod terminal;

pub(super) use terminal::{
    ShareFetchSettlementTurn, ShareFetchTerminalSettlementErrorKind, StagedShareFetchDelivery,
};

#[cfg(test)]
pub(super) use terminal::ShareFetchTerminalSettlementError;

#[cfg(test)]
mod recovery_test;
#[cfg(test)]
mod retirement_test;
#[cfg(test)]
pub(super) mod settlement_test;
