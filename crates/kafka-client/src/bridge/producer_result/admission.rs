//! Ownership-aware translation of immediate engine producer admission results.

use kafka_client_engine::{
    ProducerAcceptedFault as EngineAcceptedFault,
    ProducerAcceptedFaultKind as EngineAcceptedFaultKind,
    ProducerSendCaptureError as EngineCaptureError,
    ProducerSendCaptureErrorKind as EngineCaptureErrorKind,
    ProducerTrySendError as EngineTrySendError, ProducerTrySendErrorKind as EngineTrySendErrorKind,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, Record, bridge::producer::restore_rejected_record,
};

/// Caller-owned record and semantic reason immediate admission failed.
#[derive(Debug)]
pub(crate) struct ProducerAdmissionRejection {
    record: Record,
    error: KafkaError,
}

impl ProducerAdmissionRejection {
    pub(crate) fn into_parts(self) -> (Record, KafkaError) {
        (self.record, self.error)
    }
}

pub(crate) fn translate_admission_error(error: EngineTrySendError) -> ProducerAdmissionRejection {
    let kind = error.kind();
    let detail = error.detail().map(str::to_owned);
    let record = restore_rejected_record(error.into_record());
    ProducerAdmissionRejection {
        record,
        error: admission_error(kind, detail.as_deref()),
    }
}

pub(crate) fn translate_capture_error(
    record: Record,
    error: EngineCaptureError,
) -> ProducerAdmissionRejection {
    ProducerAdmissionRejection {
        record,
        error: admission_error(capture_error_kind(error.kind()), None),
    }
}

pub(crate) fn translate_accepted_fault(fault: &EngineAcceptedFault) -> KafkaError {
    KafkaError::new(accepted_fault_kind(fault.kind()), fault.to_string())
}

pub(super) const fn capture_error_kind(kind: EngineCaptureErrorKind) -> EngineTrySendErrorKind {
    match kind {
        EngineCaptureErrorKind::DeadlineUnrepresentable => {
            EngineTrySendErrorKind::DeadlineUnrepresentable
        }
        EngineCaptureErrorKind::TimestampUnrepresentable => {
            EngineTrySendErrorKind::TimestampUnrepresentable
        }
    }
}

pub(super) fn admission_error(kind: EngineTrySendErrorKind, detail: Option<&str>) -> KafkaError {
    let message = detail.unwrap_or_else(|| admission_message(kind));
    KafkaError::new(admission_kind(kind), message).with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) const fn admission_kind(kind: EngineTrySendErrorKind) -> ErrorKind {
    match kind {
        EngineTrySendErrorKind::EmptyTopic
        | EngineTrySendErrorKind::MissingExplicitPartition
        | EngineTrySendErrorKind::NegativeExplicitPartition
        | EngineTrySendErrorKind::RecordSizeUnrepresentable => ErrorKind::InvalidRecord,
        EngineTrySendErrorKind::DeadlineUnrepresentable
        | EngineTrySendErrorKind::DeadlineElapsed => ErrorKind::Timeout,
        EngineTrySendErrorKind::Contended
        | EngineTrySendErrorKind::PendingPrecedence
        | EngineTrySendErrorKind::CompletionCapacity
        | EngineTrySendErrorKind::RecordCapacity
        | EngineTrySendErrorKind::ByteCapacity
        | EngineTrySendErrorKind::BatchCapacity
        | EngineTrySendErrorKind::AccumulatorPending => ErrorKind::Backpressure,
        EngineTrySendErrorKind::Closed => ErrorKind::State,
        EngineTrySendErrorKind::TimestampUnrepresentable
        | EngineTrySendErrorKind::LocalIdentityExhausted
        | EngineTrySendErrorKind::HostPoisoned
        | EngineTrySendErrorKind::InternalInvariant => ErrorKind::Internal,
    }
}

const fn admission_message(kind: EngineTrySendErrorKind) -> &'static str {
    match kind {
        EngineTrySendErrorKind::EmptyTopic => "producer record topic is empty",
        EngineTrySendErrorKind::MissingExplicitPartition => {
            "producer record requires an explicit partition"
        }
        EngineTrySendErrorKind::NegativeExplicitPartition => {
            "producer record partition cannot be negative"
        }
        EngineTrySendErrorKind::DeadlineUnrepresentable => {
            "producer delivery deadline cannot be represented"
        }
        EngineTrySendErrorKind::TimestampUnrepresentable => {
            "engine timestamp cannot be represented"
        }
        EngineTrySendErrorKind::Contended => "producer admission is contended",
        EngineTrySendErrorKind::PendingPrecedence => {
            "an older pending producer send owns admission precedence"
        }
        EngineTrySendErrorKind::CompletionCapacity => "producer completion capacity is exhausted",
        EngineTrySendErrorKind::RecordCapacity => "producer record capacity is exhausted",
        EngineTrySendErrorKind::ByteCapacity => "producer byte capacity is exhausted",
        EngineTrySendErrorKind::RecordSizeUnrepresentable => {
            "producer record size cannot be represented"
        }
        EngineTrySendErrorKind::BatchCapacity => "producer batch capacity is exhausted",
        EngineTrySendErrorKind::AccumulatorPending => "producer accumulator requires host progress",
        EngineTrySendErrorKind::DeadlineElapsed => {
            "producer delivery deadline elapsed before admission"
        }
        EngineTrySendErrorKind::Closed => "producer admission is closed",
        EngineTrySendErrorKind::LocalIdentityExhausted => {
            "producer local identity space is exhausted"
        }
        EngineTrySendErrorKind::HostPoisoned => "producer host is unavailable",
        EngineTrySendErrorKind::InternalInvariant => {
            "producer admission violated an internal contract"
        }
    }
}

pub(super) const fn accepted_fault_kind(kind: EngineAcceptedFaultKind) -> ErrorKind {
    match kind {
        EngineAcceptedFaultKind::HostInvariant | EngineAcceptedFaultKind::Wake => {
            ErrorKind::Internal
        }
    }
}
