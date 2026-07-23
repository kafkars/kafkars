//! Lossless record ownership transfer across the private facade-engine seam.

use std::time::Duration;

use kafka_client_engine::{
    ProducerHandle as EngineProducerHandle, ProducerHeader as EngineProducerHeader,
    ProducerRecord as EngineProducerRecord, ProducerSendOptions as EngineProducerSendOptions,
};

use crate::{
    bridge::{
        producer_barrier::{BarrierKind, ProducerBarrier},
        producer_delivery::ProducerDelivery,
        producer_result::admission::{
            ProducerAdmissionRejection, translate_accepted_fault, translate_admission_error,
            translate_capture_error,
        },
        producer_result::{close::translate_close_admission, flush::translate_flush_admission},
    },
    record::{Header, Record, RecordParts},
};

/// Cloneable facade-to-engine producer owner with one compiled timeout.
#[derive(Debug, Clone)]
pub(crate) struct ProducerEngine {
    handle: EngineProducerHandle,
    options: EngineProducerSendOptions,
}

impl ProducerEngine {
    pub(crate) fn new(handle: EngineProducerHandle, delivery_timeout: Duration) -> Self {
        Self {
            handle,
            options: EngineProducerSendOptions::new(delivery_timeout),
        }
    }

    pub(crate) const fn with_delivery_timeout(mut self, delivery_timeout: Duration) -> Self {
        self.options = EngineProducerSendOptions::new(delivery_timeout);
        self
    }

    pub(crate) const fn delivery_timeout(&self) -> Duration {
        self.options.delivery_timeout()
    }

    /// Captures one exact barrier or returns its admission error as ready state.
    pub(crate) fn flush(&self) -> ProducerBarrier {
        match self.handle.try_flush() {
            Ok(accepted) => {
                let diagnostic = accepted.fault().map(translate_accepted_fault);
                ProducerBarrier::accepted(BarrierKind::Flush, accepted.into_observer(), diagnostic)
            }
            Err(error) => {
                ProducerBarrier::ready(BarrierKind::Flush, Err(translate_flush_admission(&error)))
            }
        }
    }

    /// Attempts one atomic close and returns its terminal authority.
    pub(crate) fn close(&self) -> ProducerBarrier {
        match self.handle.try_close() {
            Ok(accepted) => {
                let diagnostic = accepted.fault().map(translate_accepted_fault);
                ProducerBarrier::accepted(BarrierKind::Close, accepted.into_observer(), diagnostic)
            }
            Err(error) => {
                ProducerBarrier::ready(BarrierKind::Close, Err(translate_close_admission(&error)))
            }
        }
    }

    /// Captures the public boundary before converting caller-owned bytes.
    #[allow(
        clippy::result_large_err,
        reason = "pre-admission failure returns the exact facade record"
    )]
    pub(crate) fn try_send(
        &self,
        record: Record,
    ) -> Result<ProducerDelivery, ProducerAdmissionRejection> {
        let capture = match self.handle.capture_send(self.options) {
            Ok(capture) => capture,
            Err(error) => return Err(translate_capture_error(record, error)),
        };
        let topic = record.topic().to_owned();
        let engine_record = into_engine_record(record);
        match self.handle.try_send_captured(capture, engine_record) {
            Ok(accepted) => {
                let diagnostic = accepted.fault().map(translate_accepted_fault);
                Ok(ProducerDelivery::new(
                    topic,
                    accepted.into_observer(),
                    diagnostic,
                ))
            }
            Err(error) => Err(translate_admission_error(error)),
        }
    }
}

pub(crate) fn into_engine_record(record: Record) -> EngineProducerRecord {
    let RecordParts {
        topic,
        partition,
        timestamp_milliseconds,
        key,
        value,
        headers,
    } = record.into_parts();
    let record = EngineProducerRecord::to(topic);
    let record = match partition {
        Some(partition) => record.partition(partition),
        None => record,
    };
    let record = match timestamp_milliseconds {
        Some(timestamp) => record.timestamp_milliseconds(timestamp),
        None => record,
    };
    let record = match key {
        Some(key) => record.key(key),
        None => record,
    };
    let record = match value {
        Some(value) => record.value(value),
        None => record,
    };
    headers.into_iter().fold(record, |record, header| {
        record.header(into_engine_header(header))
    })
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "a rejected engine record transfers ownership back to the facade"
)]
pub(crate) fn restore_rejected_record(record: EngineProducerRecord) -> Record {
    let headers = record
        .headers()
        .iter()
        .map(|header| Header::from_parts(header.name().to_owned(), header.value().cloned()))
        .collect();
    Record::from_parts(RecordParts {
        topic: record.topic().to_owned(),
        partition: record.explicit_partition(),
        timestamp_milliseconds: record.timestamp(),
        key: record.key_bytes().cloned(),
        value: record.value_bytes().cloned(),
        headers,
    })
}

fn into_engine_header(header: Header) -> EngineProducerHeader {
    let (name, value) = header.into_parts();
    match value {
        Some(value) => EngineProducerHeader::new(name, value),
        None => EngineProducerHeader::null(name),
    }
}
