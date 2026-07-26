//! Driver-neutral gate between core-authorized rediscovery and a later fresh Join.

/// Sole per-group mechanism state for one core-authorized coordinator rediscovery.
pub(super) struct ClassicCoordinatorRediscovery {
    rediscovery_state: ClassicCoordinatorRediscoveryState,
}

enum ClassicCoordinatorRediscoveryState {
    Open,
    AwaitingRouteTransfer,
    Invalidating,
}

/// Prevalidated installation paired with the exact core rejoin schedule.
#[must_use = "a prepared rediscovery gate must be committed with its rejoin schedule"]
pub(super) struct PreparedClassicCoordinatorRediscovery<'a> {
    owner: &'a mut ClassicCoordinatorRediscovery,
}

impl ClassicCoordinatorRediscovery {
    pub(super) const fn new() -> Self {
        Self {
            rediscovery_state: ClassicCoordinatorRediscoveryState::Open,
        }
    }

    pub(super) const fn blocks_join(&self) -> bool {
        !matches!(
            self.rediscovery_state,
            ClassicCoordinatorRediscoveryState::Open
        )
    }

    pub(super) const fn awaits_route_transfer(&self) -> bool {
        matches!(
            self.rediscovery_state,
            ClassicCoordinatorRediscoveryState::AwaitingRouteTransfer
        )
    }

    pub(super) fn prepare_rediscovery_install(
        &mut self,
    ) -> Result<PreparedClassicCoordinatorRediscovery<'_>, ClassicCoordinatorRediscoveryError> {
        if self.blocks_join() {
            return Err(ClassicCoordinatorRediscoveryError::Occupied);
        }
        Ok(PreparedClassicCoordinatorRediscovery { owner: self })
    }

    pub(super) fn confirm_rediscovery_transfer(
        &mut self,
    ) -> Result<(), ClassicCoordinatorRediscoveryError> {
        if !self.awaits_route_transfer() {
            return Err(ClassicCoordinatorRediscoveryError::TransferNotPending);
        }
        self.rediscovery_state = ClassicCoordinatorRediscoveryState::Invalidating;
        Ok(())
    }

    pub(super) fn permit_rejoin(&mut self) -> Result<(), ClassicCoordinatorRediscoveryError> {
        if !matches!(
            self.rediscovery_state,
            ClassicCoordinatorRediscoveryState::Invalidating
        ) {
            return Err(ClassicCoordinatorRediscoveryError::InvalidationNotPending);
        }
        self.rediscovery_state = ClassicCoordinatorRediscoveryState::Open;
        Ok(())
    }

    pub(super) fn clear_rediscovery_after_driver_shutdown(&mut self) {
        self.rediscovery_state = ClassicCoordinatorRediscoveryState::Open;
    }

    pub(super) const fn unsettled(&self) -> usize {
        if self.blocks_join() { 1 } else { 0 }
    }
}

impl PreparedClassicCoordinatorRediscovery<'_> {
    pub(super) fn commit(self) {
        self.owner.rediscovery_state = ClassicCoordinatorRediscoveryState::AwaitingRouteTransfer;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicCoordinatorRediscoveryError {
    Occupied,
    TransferNotPending,
    InvalidationNotPending,
}
