//! Concrete `kafka-driver` adapter for private transaction lifecycle execution.

use kafka_client_core::{Moment, TransactionEndMode};

use crate::{
    driver::{
        DriverOwner,
        transaction_control::{TransactionEndCall, TransactionEndTerminalFact},
    },
    protocol::transaction::{EndTxnDisposition, EndTxnOutcome},
};

use super::{
    host::{TransactionLifecycleHost, TransactionLifecycleHostError, TransactionLifecycleTurn},
    port::{
        TransactionEndPort, TransactionEndPortCall, TransactionEndPortCallPoll,
        TransactionEndPortTerminal, TransactionEndPortTerminalEvidence, TransactionEndRequest,
    },
};

impl TransactionLifecycleHost {
    pub(crate) fn turn(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<TransactionLifecycleTurn, TransactionLifecycleHostError> {
        let enrollment = self.enrollment.turn(now, driver);
        if self.enrollment.has_fatal_terminal() {
            self.sequencing.fence();
        }
        if enrollment
            == crate::transaction::partition_enrollment::TransactionPartitionEnrollmentTurn::Progress
        {
            return Ok(TransactionLifecycleTurn::Progress);
        }
        self.turn_with_at(now, &mut DriverTransactionEndPort { driver })
    }
}

struct DriverTransactionEndPort<'a> {
    driver: &'a DriverOwner,
}

impl TransactionEndPort for DriverTransactionEndPort<'_> {
    fn submit(
        &mut self,
        request: TransactionEndRequest<'_>,
    ) -> Result<Box<dyn TransactionEndPortCall>, ()> {
        let disposition = match request.mode {
            TransactionEndMode::Commit => EndTxnDisposition::Commit,
            TransactionEndMode::Abort => EndTxnDisposition::Abort,
        };
        TransactionEndCall::submit(
            self.driver,
            request.transactional_id,
            request.producer_id,
            request.producer_epoch,
            disposition,
            request.deadline,
        )
        .map(|call| Box::new(DriverTransactionEndCall { call }) as Box<_>)
        .map_err(|_error| ())
    }
}

struct DriverTransactionEndCall {
    call: TransactionEndCall,
}

impl TransactionEndPortCall for DriverTransactionEndCall {
    fn poll(&mut self, deadline_elapsed: bool) -> TransactionEndPortCallPoll {
        if deadline_elapsed {
            if let Some(terminal) = self.call.expire_refresh() {
                let normalized = normalize_driver_terminal(
                    &terminal.fact(),
                    terminal.retry_safe_after_refresh(),
                );
                return TransactionEndPortCallPoll::DeadlineElapsed(driver_evidence(
                    normalized,
                    move || terminal.discard(),
                ));
            }
        }
        let Some(result) = self.call.try_terminal() else {
            return TransactionEndPortCallPoll::Pending;
        };
        let terminal = match result {
            Ok(terminal) => terminal,
            Err(_completion_error) => {
                return TransactionEndPortCallPoll::Terminal(Box::new(
                    TerminalWithoutRouteEvidence,
                ));
            }
        };
        let normalized =
            normalize_driver_terminal(&terminal.fact(), terminal.retry_safe_after_refresh());
        TransactionEndPortCallPoll::Terminal(driver_evidence(normalized, move || {
            terminal.discard();
        }))
    }

    fn discard_after_driver_shutdown(self: Box<Self>) {
        self.call.discard_after_driver_shutdown();
    }
}

struct DriverTransactionEndEvidence {
    terminal: TransactionEndPortTerminal,
    discard: Option<Box<dyn FnOnce()>>,
}

impl TransactionEndPortTerminalEvidence for DriverTransactionEndEvidence {
    fn terminal(&self) -> TransactionEndPortTerminal {
        self.terminal
    }

    fn discard(mut self: Box<Self>) {
        if let Some(discard) = self.discard.take() {
            discard();
        }
    }
}

fn driver_evidence(
    terminal: TransactionEndPortTerminal,
    discard: impl FnOnce() + 'static,
) -> Box<dyn TransactionEndPortTerminalEvidence> {
    Box::new(DriverTransactionEndEvidence {
        terminal,
        discard: Some(Box::new(discard)),
    })
}

fn normalize_driver_terminal(
    fact: &TransactionEndTerminalFact,
    retry_safe_after_refresh: bool,
) -> TransactionEndPortTerminal {
    match fact {
        TransactionEndTerminalFact::Response(Ok(EndTxnOutcome::Succeeded { .. })) => {
            TransactionEndPortTerminal::Succeeded
        }
        _ if retry_safe_after_refresh => TransactionEndPortTerminal::RetryableCoordinatorLoss,
        TransactionEndTerminalFact::Response(Ok(EndTxnOutcome::Rejected { error, .. })) => {
            let _category = error.category();
            TransactionEndPortTerminal::Fatal
        }
        TransactionEndTerminalFact::Response(Err(_)) => TransactionEndPortTerminal::Fatal,
        TransactionEndTerminalFact::Failed { kind, delivery } => {
            let _ = (kind, delivery);
            TransactionEndPortTerminal::Fatal
        }
    }
}

struct TerminalWithoutRouteEvidence;

impl TransactionEndPortTerminalEvidence for TerminalWithoutRouteEvidence {
    fn terminal(&self) -> TransactionEndPortTerminal {
        TransactionEndPortTerminal::Fatal
    }

    fn discard(self: Box<Self>) {
        drop(self);
    }
}
