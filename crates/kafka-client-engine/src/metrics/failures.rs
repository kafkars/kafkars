//! Curated classified broker-call failure counters from driver observation.

use crate::driver::owner::observation::DriverFailureMetrics;

/// Cumulative classified terminal broker-call failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineFailureMetrics(DriverFailureMetrics);

impl EngineFailureMetrics {
    pub(super) const fn from_driver(inner: DriverFailureMetrics) -> Self {
        Self(inner)
    }

    /// Returns broker-name resolution failures.
    pub const fn dns(self) -> u64 {
        self.0.dns()
    }

    /// Returns transport-establishment failures.
    pub const fn connect(self) -> u64 {
        self.0.connect()
    }

    /// Returns established-transport losses.
    pub const fn transport(self) -> u64 {
        self.0.transport()
    }

    /// Returns API negotiation failures.
    pub const fn negotiation(self) -> u64 {
        self.0.negotiation()
    }

    /// Returns authentication failures.
    pub const fn authentication(self) -> u64 {
        self.0.authentication()
    }

    /// Returns absolute-deadline failures.
    pub const fn deadline(self) -> u64 {
        self.0.deadline()
    }

    /// Returns local validation, preparation, or writer rejections.
    pub const fn local_rejection(self) -> u64 {
        self.0.local_rejection()
    }

    /// Returns response-registry capacity rejections.
    pub const fn response_capacity(self) -> u64 {
        self.0.response_capacity()
    }

    /// Returns route, query, or coordinator capacity rejections.
    pub const fn route_capacity(self) -> u64 {
        self.0.route_capacity()
    }
}
