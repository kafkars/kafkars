//! Inert public Admin `ListOffsets` intent translated at the engine boundary.

use kafka_client_engine::{
    AdminListOffsetsRequest as EngineRequest, AdminListOffsetsRequestSpec as EngineSpec,
    AdminListOffsetsRequestTarget as EngineTarget, ConsumerReadIsolation as EngineReadIsolation,
};

use crate::{
    ReadIsolation,
    admin::{ListOffsetsQuery, OffsetSpec},
};

/// Linear request retained by the public builder before submission.
pub(crate) struct ListOffsetsAdminRequest {
    queries: Vec<ListOffsetsQuery>,
    read_isolation: ReadIsolation,
}

impl ListOffsetsAdminRequest {
    pub(crate) const fn new(queries: Vec<ListOffsetsQuery>) -> Self {
        Self {
            queries,
            read_isolation: ReadIsolation::ReadUncommitted,
        }
    }

    pub(crate) const fn with_read_isolation(mut self, read_isolation: ReadIsolation) -> Self {
        self.read_isolation = read_isolation;
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.queries
                .into_iter()
                .map(|query| {
                    let (topic, partition, spec, current_leader_epoch) = query.into_parts();
                    let target = EngineTarget::new(topic, partition, engine_spec(spec));
                    match current_leader_epoch {
                        Some(current_leader_epoch) => {
                            target.current_leader_epoch(current_leader_epoch)
                        }
                        None => target,
                    }
                })
                .collect(),
        )
        .with_read_isolation(engine_read_isolation(self.read_isolation))
    }
}

impl std::fmt::Debug for ListOffsetsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListOffsetsAdminRequest")
            .field("queries", &self.queries)
            .field("read_isolation", &self.read_isolation)
            .finish()
    }
}

const fn engine_spec(spec: OffsetSpec) -> EngineSpec {
    match spec {
        OffsetSpec::Earliest => EngineSpec::Earliest,
        OffsetSpec::Latest => EngineSpec::Latest,
        OffsetSpec::MaxTimestamp => EngineSpec::MaxTimestamp,
        OffsetSpec::EarliestLocal => EngineSpec::EarliestLocal,
        OffsetSpec::LatestTiered => EngineSpec::LatestTiered,
        OffsetSpec::EarliestPendingUpload => EngineSpec::EarliestPendingUpload,
        OffsetSpec::Timestamp(timestamp) => EngineSpec::Timestamp(timestamp),
    }
}

const fn engine_read_isolation(read_isolation: ReadIsolation) -> EngineReadIsolation {
    match read_isolation {
        ReadIsolation::ReadUncommitted => EngineReadIsolation::ReadUncommitted,
        ReadIsolation::ReadCommitted => EngineReadIsolation::ReadCommitted,
    }
}
