//! Private client-metrics bridge facade.

mod observer;
mod producer;
mod snapshot;

pub(crate) use observer::ClientMetricsObserver;
pub(crate) use producer::ClientProducerMetrics;
pub(crate) use snapshot::{
    ClientCallMetrics, ClientFailureMetrics, ClientLatencyMetric, ClientLatencyMetrics,
    ClientMailboxMetrics, ClientMetricsSnapshot,
};
