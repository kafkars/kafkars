//! Fixed-size operational projection retained inside the driver adapter.

use std::time::Duration;

use kafka_driver::{
    CallCounters, CallLatencySnapshot, DriverSnapshot, FailureCounters, LatencyMetric,
    MailboxSnapshot,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DriverMetricsSnapshot {
    inner: DriverSnapshot,
}

impl DriverMetricsSnapshot {
    pub(super) const fn from_driver(inner: DriverSnapshot) -> Self {
        Self { inner }
    }

    pub(crate) const fn calls(&self) -> DriverCallMetrics {
        DriverCallMetrics(self.inner.calls())
    }

    pub(crate) const fn failures(&self) -> DriverFailureMetrics {
        DriverFailureMetrics(self.inner.failures())
    }

    pub(crate) const fn mailbox(&self) -> DriverMailboxMetrics {
        DriverMailboxMetrics(*self.inner.mailbox())
    }

    pub(crate) const fn latency(&self) -> DriverLatencyMetrics {
        DriverLatencyMetrics(self.inner.latency())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DriverCallMetrics(CallCounters);

impl DriverCallMetrics {
    pub(crate) const fn admitted(self) -> u64 {
        self.0.admitted()
    }

    pub(crate) const fn succeeded(self) -> u64 {
        self.0.succeeded()
    }

    pub(crate) const fn failed(self) -> u64 {
        self.0.failed()
    }

    pub(crate) const fn observer_abandoned(self) -> u64 {
        self.0.receiver_abandoned()
    }

    pub(crate) const fn not_sent(self) -> u64 {
        self.0.not_sent()
    }

    pub(crate) const fn possibly_sent(self) -> u64 {
        self.0.possibly_sent()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DriverFailureMetrics(FailureCounters);

impl DriverFailureMetrics {
    pub(crate) const fn dns(self) -> u64 {
        self.0.dns()
    }

    pub(crate) const fn connect(self) -> u64 {
        self.0.connect()
    }

    pub(crate) const fn transport(self) -> u64 {
        self.0.transport()
    }

    pub(crate) const fn negotiation(self) -> u64 {
        self.0.negotiation()
    }

    pub(crate) const fn authentication(self) -> u64 {
        self.0.authentication()
    }

    pub(crate) const fn deadline(self) -> u64 {
        self.0.deadline()
    }

    pub(crate) const fn local_rejection(self) -> u64 {
        self.0.local_rejection()
    }

    pub(crate) const fn response_capacity(self) -> u64 {
        self.0.response_capacity()
    }

    pub(crate) const fn route_capacity(self) -> u64 {
        self.0.route_capacity()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DriverMailboxMetrics(MailboxSnapshot);

impl DriverMailboxMetrics {
    pub(crate) const fn capacity_per_lane(self) -> usize {
        self.0.capacity_per_lane()
    }

    pub(crate) const fn byte_capacity_per_lane(self) -> usize {
        self.0.byte_capacity_per_lane()
    }

    pub(crate) const fn queued_work(self) -> usize {
        self.0.queued_work()
    }

    pub(crate) const fn queued_work_bytes(self) -> usize {
        self.0.queued_work_bytes()
    }

    pub(crate) const fn queued_control(self) -> usize {
        self.0.queued_control()
    }

    pub(crate) const fn queued_control_bytes(self) -> usize {
        self.0.queued_control_bytes()
    }

    pub(crate) const fn work_full(self) -> u64 {
        self.0.work_full()
    }

    pub(crate) const fn work_byte_full(self) -> u64 {
        self.0.work_byte_full()
    }

    pub(crate) const fn control_full(self) -> u64 {
        self.0.control_full()
    }

    pub(crate) const fn control_byte_full(self) -> u64 {
        self.0.control_byte_full()
    }

    pub(crate) const fn closed_rejections(self) -> u64 {
        self.0.closed_rejections()
    }

    pub(crate) const fn wake_failures(self) -> u64 {
        self.0.wake_failures()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DriverLatencyMetrics(CallLatencySnapshot);

impl DriverLatencyMetrics {
    pub(crate) const fn mailbox(self) -> DriverLatencyMetric {
        DriverLatencyMetric(self.0.mailbox())
    }

    pub(crate) const fn routing(self) -> DriverLatencyMetric {
        DriverLatencyMetric(self.0.routing())
    }

    pub(crate) const fn preparation(self) -> DriverLatencyMetric {
        DriverLatencyMetric(self.0.preparation())
    }

    pub(crate) const fn writer_admission(self) -> DriverLatencyMetric {
        DriverLatencyMetric(self.0.writer_admission())
    }

    pub(crate) const fn in_flight(self) -> DriverLatencyMetric {
        DriverLatencyMetric(self.0.in_flight())
    }

    pub(crate) const fn end_to_end(self) -> DriverLatencyMetric {
        DriverLatencyMetric(self.0.end_to_end())
    }

    pub(crate) const fn deadline_lateness(self) -> DriverLatencyMetric {
        DriverLatencyMetric(self.0.deadline_lateness())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DriverLatencyMetric(LatencyMetric);

impl DriverLatencyMetric {
    pub(crate) const fn samples(self) -> u64 {
        self.0.samples()
    }

    pub(crate) const fn total(self) -> Duration {
        self.0.total()
    }

    pub(crate) const fn max(self) -> Duration {
        self.0.max()
    }
}
