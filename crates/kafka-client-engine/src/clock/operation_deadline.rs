//! One immutable operation deadline shared by core policy and transport execution.

use std::time::Instant;

use kafka_client_core::Deadline;

/// Original absolute deadline representations captured at one timing boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OperationDeadline {
    core: Deadline,
    transport: Instant,
}

impl OperationDeadline {
    pub(in crate::clock) const fn from_boundary_parts(core: Deadline, transport: Instant) -> Self {
        Self { core, transport }
    }

    #[cfg(test)]
    pub(crate) const fn from_parts_for_test(core: Deadline, transport: Instant) -> Self {
        Self { core, transport }
    }

    #[cfg(test)]
    pub(crate) fn from_core_for_test(core: Deadline) -> Self {
        Self {
            core,
            transport: Instant::now(),
        }
    }

    /// Returns the deterministic deadline consumed by core policy.
    pub(crate) const fn core(self) -> Deadline {
        self.core
    }

    /// Returns the unchanged operating-system deadline reserved for the driver.
    pub(crate) const fn transport(self) -> Instant {
        self.transport
    }
}
