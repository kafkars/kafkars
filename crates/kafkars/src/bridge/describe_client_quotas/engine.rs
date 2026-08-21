//! Isolated names for the engine-owned `DescribeClientQuotas` adapter contract.

pub(super) use kafka_client_engine::{
    DescribeClientQuotaEntity as Entity, DescribeClientQuotaEntityComponent as EntityComponent,
    DescribeClientQuotaFilterComponent as FilterComponent, DescribeClientQuotaMatch as Match,
    DescribeClientQuotaValue as Value, DescribeClientQuotasAccepted as Accepted,
    DescribeClientQuotasAcceptedFaultKind as AcceptedFaultKind,
    DescribeClientQuotasAdmissionError as AdmissionError,
    DescribeClientQuotasAdmissionErrorKind as AdmissionErrorKind,
    DescribeClientQuotasBatch as Batch, DescribeClientQuotasBrokerError as BrokerError,
    DescribeClientQuotasDeliveryStatus as DeliveryStatus, DescribeClientQuotasFailure as Failure,
    DescribeClientQuotasFailureKind as FailureKind, DescribeClientQuotasObserver as Observer,
    DescribeClientQuotasObserverError as ObserverError, DescribeClientQuotasOutcome as Outcome,
    DescribeClientQuotasRequest as Request,
};
