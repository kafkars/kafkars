//! Concrete startup assembly for the topic-scoped `DescribeConfigs` lane.

use std::sync::Arc;

use crate::{
    admin::{
        DESCRIBE_CONFIGS_CAPACITY, DescribeConfigsAdmissionPort, DescribeConfigsHost,
        DescribeConfigsPublisher, DescribeConfigsShardOwner,
    },
    driver::{DescribeConfigsCalls, ReactorWake},
};

pub(super) struct StartedDescribeConfigs {
    pub(super) owner: DescribeConfigsShardOwner,
    pub(super) admission: DescribeConfigsAdmissionPort,
    pub(super) calls: DescribeConfigsCalls,
}

pub(super) fn start(
    publisher: DescribeConfigsPublisher,
    wake: Arc<ReactorWake>,
) -> StartedDescribeConfigs {
    let owner = DescribeConfigsShardOwner::new(DescribeConfigsHost::new(publisher), wake);
    let admission = owner.admission_port();
    StartedDescribeConfigs {
        owner,
        admission,
        calls: DescribeConfigsCalls::new(DESCRIBE_CONFIGS_CAPACITY),
    }
}
