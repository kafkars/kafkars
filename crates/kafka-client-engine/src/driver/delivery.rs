//! Authoritative translation of driver-owned terminal delivery certainty.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{Delivery, RequestError};

/// Translates one terminal driver failure without reclassifying its variant.
pub(crate) const fn request_failure_delivery(error: &RequestError) -> DeliveryStatus {
    delivery_status(error.delivery())
}

const fn delivery_status(delivery: Delivery) -> DeliveryStatus {
    match delivery {
        Delivery::NotSent => DeliveryStatus::NotSent,
        Delivery::PossiblySent => DeliveryStatus::PossiblySent,
    }
}
