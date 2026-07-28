//! Non-blocking driver handoff and linear terminal-evidence settlement.

use kafka_client_core::{
    Moment, TransactionEndOutcome, TransactionLifecycleEffect, TransactionLifecycleInput,
};

use super::{
    host::{TransactionLifecycleHost, TransactionLifecycleHostError, TransactionLifecycleTurn},
    port::{
        TransactionEndPort, TransactionEndPortCallPoll, TransactionEndPortTerminal,
        TransactionEndRequest,
    },
};

impl TransactionLifecycleHost {
    #[cfg(test)]
    pub(super) fn turn_with(
        &mut self,
        port: &mut dyn TransactionEndPort,
    ) -> Result<TransactionLifecycleTurn, TransactionLifecycleHostError> {
        self.turn_with_at(Moment::from_tick(0), port)
    }

    pub(super) fn turn_with_at(
        &mut self,
        now: Moment,
        port: &mut dyn TransactionEndPort,
    ) -> Result<TransactionLifecycleTurn, TransactionLifecycleHostError> {
        if self.reclaim_one()? || self.publish_terminal()? {
            return Ok(TransactionLifecycleTurn::Progress);
        }
        let Some(pending) = self.pending_end.as_mut() else {
            return Ok(TransactionLifecycleTurn::Idle);
        };
        if pending.terminal.is_some() || !pending.ready {
            return Ok(TransactionLifecycleTurn::Idle);
        }
        if pending.call.is_none() {
            if pending.deadline.core().is_elapsed_at(now) {
                self.settle_end(TransactionEndOutcome::Fatal)?;
                return Ok(TransactionLifecycleTurn::Progress);
            }
            if pending
                .retry_not_before
                .is_some_and(|not_before| !not_before.is_elapsed_at(now))
            {
                return Ok(TransactionLifecycleTurn::Idle);
            }
            pending.retry_not_before = None;
            let owner = self
                .owner
                .as_ref()
                .ok_or(TransactionLifecycleHostError::UnexpectedEffect)?;
            let request = TransactionEndRequest {
                transactional_id: owner.transactional_id(),
                producer_id: owner.producer_id(),
                producer_epoch: owner.producer_epoch(),
                mode: pending.mode,
                deadline: pending.deadline.transport(),
            };
            match port.submit(request) {
                Ok(call) => pending.call = Some(call),
                Err(()) => self.settle_end(TransactionEndOutcome::Fatal)?,
            }
            return Ok(TransactionLifecycleTurn::Progress);
        }
        let (evidence, deadline_elapsed) = match pending
            .call
            .as_mut()
            .unwrap_or_else(|| unreachable!("checked accepted EndTxn call"))
            .poll(pending.deadline.core().is_elapsed_at(now))
        {
            TransactionEndPortCallPoll::Pending => return Ok(TransactionLifecycleTurn::Idle),
            TransactionEndPortCallPoll::DeadlineElapsed(evidence) => (evidence, true),
            TransactionEndPortCallPoll::Terminal(evidence) => (evidence, false),
        };
        let outcome = match (deadline_elapsed, evidence.terminal()) {
            (false, TransactionEndPortTerminal::Succeeded) => TransactionEndOutcome::Succeeded,
            (false, TransactionEndPortTerminal::RetryableCoordinatorLoss)
                if self.schedule_end_retry(now)? =>
            {
                evidence.discard();
                return Ok(TransactionLifecycleTurn::Progress);
            }
            (true, _)
            | (
                false,
                TransactionEndPortTerminal::RetryableCoordinatorLoss
                | TransactionEndPortTerminal::Fatal,
            ) => TransactionEndOutcome::Fatal,
        };
        let settlement = self.settle_end(outcome);
        evidence.discard();
        settlement?;
        Ok(TransactionLifecycleTurn::Progress)
    }

    fn schedule_end_retry(&mut self, now: Moment) -> Result<bool, TransactionLifecycleHostError> {
        let pending = self
            .pending_end
            .as_ref()
            .ok_or(TransactionLifecycleHostError::MissingEndOperation)?;
        if pending.retries_started >= self.end_retry_policy.max_retries()
            || pending.deadline.core().is_elapsed_at(now)
        {
            return Ok(false);
        }
        let Some(not_before) = now.checked_deadline_after(self.end_retry_policy.backoff_ticks())
        else {
            return Ok(false);
        };
        if not_before >= pending.deadline.core() {
            return Ok(false);
        }
        let Some(retries_started) = pending.retries_started.checked_add(1) else {
            return Ok(false);
        };
        let (epoch, mode, operation_id) = (pending.epoch, pending.mode, pending.operation_id);
        let transition = self.machine.apply(
            self.owner_id()?,
            TransactionLifecycleInput::EndRetryableBrokerRejected { epoch },
        )?;
        let effect = transition.into_effect();
        match effect {
            Some(TransactionLifecycleEffect::EndTransaction {
                epoch: effect_epoch,
                mode: effect_mode,
                operation_id: effect_operation,
                ..
            }) if effect_epoch == epoch
                && effect_mode == mode
                && effect_operation == operation_id => {}
            _ => return Err(TransactionLifecycleHostError::UnexpectedEffect),
        }
        self.interpret(effect, None)?;
        let pending = self
            .pending_end
            .as_mut()
            .ok_or(TransactionLifecycleHostError::MissingEndOperation)?;
        drop(pending.call.take());
        pending.retry_not_before = Some(not_before);
        pending.retries_started = retries_started;
        Ok(true)
    }

    pub(super) fn settle_end(
        &mut self,
        outcome: TransactionEndOutcome,
    ) -> Result<(), TransactionLifecycleHostError> {
        let epoch = self
            .pending_end
            .as_ref()
            .map(|pending| pending.epoch)
            .ok_or(TransactionLifecycleHostError::MissingEndOperation)?;
        let transition = self.machine.apply(
            self.owner_id()?,
            TransactionLifecycleInput::EndSettled { epoch, outcome },
        )?;
        self.interpret(transition.into_effect(), None)
    }
}
