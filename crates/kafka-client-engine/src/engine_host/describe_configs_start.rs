//! Concrete startup assembly for configuration-resource administration.

use std::sync::Arc;

use crate::{
    admin::{
        DESCRIBE_CONFIGS_CAPACITY, DescribeConfigsAdmissionPort, DescribeConfigsHost,
        DescribeConfigsPublisher, DescribeConfigsShardOwner, IncrementalAlterConfigsAdmissionPort,
        IncrementalAlterConfigsHost, IncrementalAlterConfigsPublisher,
        IncrementalAlterConfigsShardOwner, LegacyAlterConfigsAdmissionPort, LegacyAlterConfigsHost,
        LegacyAlterConfigsPublisher, LegacyAlterConfigsShardOwner,
    },
    driver::{DescribeConfigsCalls, IncrementalAlterConfigsCalls, ReactorWake},
};

pub(super) struct StartedConfigAdmin {
    pub(super) describe_owner: DescribeConfigsShardOwner,
    pub(super) describe_admission: DescribeConfigsAdmissionPort,
    pub(super) describe_calls: DescribeConfigsCalls,
    pub(super) incremental_owner: IncrementalAlterConfigsShardOwner,
    pub(super) incremental_admission: IncrementalAlterConfigsAdmissionPort,
    pub(super) incremental_calls: IncrementalAlterConfigsCalls,
    pub(super) legacy_owner: LegacyAlterConfigsShardOwner,
    pub(super) legacy_admission: LegacyAlterConfigsAdmissionPort,
}

pub(super) fn start(
    describe_publisher: DescribeConfigsPublisher,
    incremental_publisher: IncrementalAlterConfigsPublisher,
    legacy_publisher: LegacyAlterConfigsPublisher,
    wake: Arc<ReactorWake>,
) -> StartedConfigAdmin {
    let describe_owner = DescribeConfigsShardOwner::new(
        DescribeConfigsHost::new(describe_publisher),
        Arc::clone(&wake),
    );
    let describe_admission = describe_owner.admission_port();
    let incremental_owner = IncrementalAlterConfigsShardOwner::new(
        IncrementalAlterConfigsHost::new(incremental_publisher),
        Arc::clone(&wake),
    );
    let incremental_admission = incremental_owner.admission_port();
    let legacy_owner =
        LegacyAlterConfigsShardOwner::new(LegacyAlterConfigsHost::new(legacy_publisher), wake);
    let legacy_admission = legacy_owner.admission_port();
    StartedConfigAdmin {
        describe_owner,
        describe_admission,
        describe_calls: DescribeConfigsCalls::new(DESCRIBE_CONFIGS_CAPACITY),
        incremental_owner,
        incremental_admission,
        incremental_calls: IncrementalAlterConfigsCalls::new(
            crate::admin::INCREMENTAL_ALTER_CONFIGS_CAPACITY,
        ),
        legacy_owner,
        legacy_admission,
    }
}
