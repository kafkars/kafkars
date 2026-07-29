//! Exact Kafka configuration-resource type validation at the protocol seam.

pub(super) const fn is_positive_resource_type(resource_type: i8) -> bool {
    resource_type > 0
}
