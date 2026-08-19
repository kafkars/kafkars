//! Isolated names for the engine-owned batched `StreamsGroup` contract.

pub(super) use kafka_client_engine::{
    DescribeStreamsGroupAccepted as Accepted, DescribeStreamsGroupAdmissionError as AdmissionError,
    DescribeStreamsGroupBatchOutcome as BatchOutcome, DescribeStreamsGroupObserver as Observer,
    DescribeStreamsGroupObserverError as ObserverError, DescribeStreamsGroupOutcome as Outcome,
    DescribeStreamsGroupsRequest as Request,
};
