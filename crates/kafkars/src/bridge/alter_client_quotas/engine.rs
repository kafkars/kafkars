//! Isolated names for the engine-owned `AlterClientQuotas` adapter contract.

pub(super) use kafka_client_engine::{
    AlterClientQuotaBrokerError as BrokerError, AlterClientQuotaEntity as Entity,
    AlterClientQuotaEntityComponent as EntityComponent, AlterClientQuotaEntry as Entry,
    AlterClientQuotaOperation as Operation, AlterClientQuotaOutcome as EntityOutcome,
    AlterClientQuotasAccepted as Accepted, AlterClientQuotasAcceptedFaultKind as AcceptedFaultKind,
    AlterClientQuotasAdmissionError as AdmissionError,
    AlterClientQuotasAdmissionErrorKind as AdmissionErrorKind, AlterClientQuotasBatch as Batch,
    AlterClientQuotasDeliveryStatus as DeliveryStatus, AlterClientQuotasFailure as Failure,
    AlterClientQuotasFailureKind as FailureKind, AlterClientQuotasObserver as Observer,
    AlterClientQuotasObserverError as ObserverError, AlterClientQuotasOutcome as Outcome,
    AlterClientQuotasRequest as Request,
};
