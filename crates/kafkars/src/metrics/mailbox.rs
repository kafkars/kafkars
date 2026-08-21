//! Public bounded driver-mailbox pressure metrics.

use crate::bridge::client::metrics::ClientMailboxMetrics;

/// Current bounded driver mailbox pressure and cumulative rejection totals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MailboxMetrics(ClientMailboxMetrics);

impl MailboxMetrics {
    pub(super) const fn from_bridge(inner: ClientMailboxMetrics) -> Self {
        Self(inner)
    }

    /// Returns each independent work and control command bound.
    pub const fn capacity_per_lane(self) -> usize {
        self.0.capacity_per_lane()
    }

    /// Returns each independent work and control retained-byte bound.
    pub const fn byte_capacity_per_lane(self) -> usize {
        self.0.byte_capacity_per_lane()
    }

    /// Returns ordinary commands awaiting interpretation.
    pub const fn queued_work(self) -> usize {
        self.0.queued_work()
    }

    /// Returns bytes retained by ordinary queued commands.
    pub const fn queued_work_bytes(self) -> usize {
        self.0.queued_work_bytes()
    }

    /// Returns priority control commands awaiting interpretation.
    pub const fn queued_control(self) -> usize {
        self.0.queued_control()
    }

    /// Returns bytes retained by priority control commands.
    pub const fn queued_control_bytes(self) -> usize {
        self.0.queued_control_bytes()
    }

    /// Returns cumulative ordinary count-capacity rejections.
    pub const fn work_full(self) -> u64 {
        self.0.work_full()
    }

    /// Returns cumulative ordinary byte-capacity rejections.
    pub const fn work_byte_full(self) -> u64 {
        self.0.work_byte_full()
    }

    /// Returns cumulative control count-capacity rejections.
    pub const fn control_full(self) -> u64 {
        self.0.control_full()
    }

    /// Returns cumulative control byte-capacity rejections.
    pub const fn control_byte_full(self) -> u64 {
        self.0.control_byte_full()
    }

    /// Returns commands rejected after driver admission closed.
    pub const fn closed_rejections(self) -> u64 {
        self.0.closed_rejections()
    }

    /// Returns admitted commands returned because the poller wake failed.
    pub const fn wake_failures(self) -> u64 {
        self.0.wake_failures()
    }
}
