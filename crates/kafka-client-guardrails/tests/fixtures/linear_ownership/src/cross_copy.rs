//! Forbidden cross-file manual Copy implementation.

use super::owner::CompletionLedger as Ledger;

impl Copy for Ledger {}
