//! One runtime-neutral observer aggregating a producer batch's accepted prefix.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, Record, RecordMetadata, SendBatchResult, TrySendError};

use super::ProducerDelivery;

/// Private aggregate over record-level terminal authorities.
#[must_use = "dropping abandons every accepted delivery observation"]
pub(crate) struct ProducerBatch {
    deliveries: Vec<Option<ProducerDelivery>>,
    results: Vec<Option<Result<RecordMetadata, KafkaError>>>,
    rejection: Option<TrySendError<Vec<Record>>>,
    finished: bool,
}

impl ProducerBatch {
    pub(crate) fn new(
        deliveries: Vec<ProducerDelivery>,
        rejection: Option<TrySendError<Vec<Record>>>,
    ) -> Self {
        let results = std::iter::repeat_with(|| None)
            .take(deliveries.len())
            .collect();
        Self {
            deliveries: deliveries.into_iter().map(Some).collect(),
            results,
            rejection,
            finished: false,
        }
    }

    pub(crate) fn wait(mut self) -> SendBatchResult {
        let deliveries = (0..self.deliveries.len())
            .map(|index| {
                if let Some(result) = self.results[index].take() {
                    return result;
                }
                self.deliveries[index]
                    .take()
                    .unwrap_or_else(|| {
                        unreachable!("batch delivery or stored result remains singly owned")
                    })
                    .wait()
            })
            .collect();
        self.finished = true;
        SendBatchResult::new(deliveries, self.rejection.take())
    }

    #[cfg(test)]
    pub(super) fn from_partially_polled_test_state(
        stored: Result<RecordMetadata, KafkaError>,
        pending: ProducerDelivery,
    ) -> Self {
        Self {
            deliveries: vec![None, Some(pending)],
            results: vec![Some(stored), None],
            rejection: None,
            finished: false,
        }
    }
}

impl Future for ProducerBatch {
    type Output = SendBatchResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.finished, "producer batch polled after completion");
        let mut pending = false;
        for index in 0..this.deliveries.len() {
            let Some(delivery) = this.deliveries[index].as_mut() else {
                continue;
            };
            match Pin::new(delivery).poll(context) {
                Poll::Pending => pending = true,
                Poll::Ready(result) => {
                    this.results[index] = Some(result);
                    this.deliveries[index] = None;
                }
            }
        }
        if pending {
            return Poll::Pending;
        }
        this.finished = true;
        let deliveries = this
            .results
            .iter_mut()
            .map(|slot| {
                slot.take()
                    .unwrap_or_else(|| unreachable!("every batch delivery reached terminal"))
            })
            .collect();
        Poll::Ready(SendBatchResult::new(deliveries, this.rejection.take()))
    }
}

impl fmt::Debug for ProducerBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerBatch")
            .field(
                "pending_deliveries",
                &self.deliveries.iter().filter(|item| item.is_some()).count(),
            )
            .field(
                "completed_results",
                &self.results.iter().filter(|item| item.is_some()).count(),
            )
            .field("rejection", &self.rejection)
            .field("finished", &self.finished)
            .finish()
    }
}
