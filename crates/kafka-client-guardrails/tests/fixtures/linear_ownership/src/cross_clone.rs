//! Forbidden cross-file manual Clone implementation.

use super::owner::CompletionLedger as Ledger;

impl Clone for Ledger {
    fn clone(&self) -> Self {
        Self { slots: self.slots }
    }
}
