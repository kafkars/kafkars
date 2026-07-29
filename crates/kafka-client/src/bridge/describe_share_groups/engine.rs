//! Isolated names for the engine-owned batched ShareGroup contract.

pub(super) use kafka_client_engine::{
    DescribeShareGroupAccepted as Accepted, DescribeShareGroupAdmissionError as AdmissionError,
    DescribeShareGroupBatchOutcome as BatchOutcome, DescribeShareGroupObserver as Observer,
    DescribeShareGroupObserverError as ObserverError, DescribeShareGroupOutcome as Outcome,
    DescribeShareGroupsRequest as Request,
};
