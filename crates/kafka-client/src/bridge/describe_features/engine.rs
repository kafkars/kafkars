//! Isolated names for the engine-owned Kafka feature discovery contract.

pub(super) use kafka_client_engine::{
    DescribeFeaturesAccepted as Accepted, DescribeFeaturesAcceptedFaultKind as AcceptedFaultKind,
    DescribeFeaturesAdmissionError as AdmissionError,
    DescribeFeaturesAdmissionErrorKind as AdmissionErrorKind,
    DescribeFeaturesBrokerError as BrokerError, DescribeFeaturesDeliveryStatus as DeliveryStatus,
    DescribeFeaturesDescription as Description, DescribeFeaturesFailure as Failure,
    DescribeFeaturesFailureKind as FailureKind,
    DescribeFeaturesFinalizedFeature as FinalizedFeature, DescribeFeaturesObserver as Observer,
    DescribeFeaturesObserverError as ObserverError, DescribeFeaturesOutcome as Outcome,
    DescribeFeaturesSupportedFeature as SupportedFeature,
};
