//! Scalar progress report for one bounded classic-group Fetch turn.

/// Concrete work retained or committed by one bounded group-Fetch turn.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "the report exposes each independently bounded execution stage"
)]
pub(in crate::consumer::group) struct ClassicGroupFetchTurn {
    pub(super) effect_interpreted: bool,
    pub(super) timer_input_applied: bool,
    pub(super) fetch_polled: bool,
    pub(super) fetch_submitted: bool,
    pub(super) blocked: bool,
    pub(super) fault_retained: bool,
}

impl ClassicGroupFetchTurn {
    /// Reports work that this exact bounded turn committed or consumed.
    pub(in crate::consumer::group) const fn progressed(self) -> bool {
        self.effect_interpreted
            || self.timer_input_applied
            || self.fetch_polled
            || self.fetch_submitted
    }

    /// Reports bounded pressure without treating it as terminal failure.
    pub(in crate::consumer::group) const fn blocked(self) -> bool {
        self.blocked
    }

    /// Reports that this turn froze the owner with an exact retained fault.
    pub(in crate::consumer::group) const fn fault_retained(self) -> bool {
        self.fault_retained
    }

    #[cfg(test)]
    pub(super) const fn effect_interpreted(self) -> bool {
        self.effect_interpreted
    }

    #[cfg(test)]
    pub(super) const fn timer_input_applied(self) -> bool {
        self.timer_input_applied
    }

    #[cfg(test)]
    pub(super) const fn fetch_polled(self) -> bool {
        self.fetch_polled
    }

    #[cfg(test)]
    pub(super) const fn fetch_submitted(self) -> bool {
        self.fetch_submitted
    }
}
