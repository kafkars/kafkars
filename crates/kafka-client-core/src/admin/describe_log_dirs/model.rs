//! Validated caller-ordered broker selection for `DescribeLogDirs`.

use core::fmt;
use std::collections::BTreeSet;

/// Validated intent for one bounded broker log-directory query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeLogDirsPlan {
    broker_ids: Vec<i32>,
}

impl AdminDescribeLogDirsPlan {
    /// Validates a nonempty caller-ordered set of unique broker IDs.
    pub fn new(broker_ids: Vec<i32>) -> Result<Self, AdminDescribeLogDirsPlanError> {
        if broker_ids.is_empty() {
            return Err(AdminDescribeLogDirsPlanError::EmptyBrokerBatch);
        }
        let mut identities = BTreeSet::new();
        for broker_id in &broker_ids {
            if *broker_id < 0 {
                return Err(AdminDescribeLogDirsPlanError::NegativeBrokerId);
            }
            if !identities.insert(*broker_id) {
                return Err(AdminDescribeLogDirsPlanError::DuplicateBrokerId);
            }
        }
        Ok(Self { broker_ids })
    }

    /// Returns broker IDs in exact caller order.
    pub fn broker_ids(&self) -> &[i32] {
        &self.broker_ids
    }
}

/// Invalid deterministic `DescribeLogDirs` intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeLogDirsPlanError {
    /// At least one broker must be requested.
    EmptyBrokerBatch,
    /// Broker IDs must be nonnegative.
    NegativeBrokerId,
    /// One operation cannot repeat a broker ID.
    DuplicateBrokerId,
}

impl fmt::Display for AdminDescribeLogDirsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBrokerBatch => "DescribeLogDirs broker batch is empty",
            Self::NegativeBrokerId => "DescribeLogDirs broker ID is negative",
            Self::DuplicateBrokerId => "DescribeLogDirs contains a duplicate broker ID",
        })
    }
}

impl std::error::Error for AdminDescribeLogDirsPlanError {}
