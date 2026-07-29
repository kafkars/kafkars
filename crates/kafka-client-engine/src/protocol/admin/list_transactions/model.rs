//! Generated-free borrowed request intent and normalized API-key 66 facts.

/// Borrowed filters used to build one bounded `ListTransactions` request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ListTransactionsRequestPlan<'a> {
    state_filters: &'a [String],
    producer_id_filters: &'a [i64],
    duration_filter_ms: Option<i64>,
    transactional_id_pattern: Option<&'a str>,
}

impl<'a> ListTransactionsRequestPlan<'a> {
    pub(crate) const fn new(
        state_filters: &'a [String],
        producer_id_filters: &'a [i64],
        duration_filter_ms: Option<i64>,
        transactional_id_pattern: Option<&'a str>,
    ) -> Self {
        Self {
            state_filters,
            producer_id_filters,
            duration_filter_ms,
            transactional_id_pattern,
        }
    }

    pub(super) const fn state_filters(self) -> &'a [String] {
        self.state_filters
    }

    pub(super) const fn producer_id_filters(self) -> &'a [i64] {
        self.producer_id_filters
    }

    pub(super) const fn duration_filter_ms(self) -> Option<i64> {
        self.duration_filter_ms
    }

    pub(super) const fn transactional_id_pattern(self) -> Option<&'a str> {
        self.transactional_id_pattern
    }
}

/// One current broker transaction with opaque state and signed producer ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListedTransaction {
    transactional_id: String,
    producer_id: i64,
    transaction_state: String,
}

impl ListedTransaction {
    pub(super) const fn new(
        transactional_id: String,
        producer_id: i64,
        transaction_state: String,
    ) -> Self {
        Self {
            transactional_id,
            producer_id,
            transaction_state,
        }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        transactional_id: String,
        producer_id: i64,
        transaction_state: String,
    ) -> Self {
        Self::new(transactional_id, producer_id, transaction_state)
    }

    pub(crate) fn into_parts(self) -> (String, i64, String) {
        (
            self.transactional_id,
            self.producer_id,
            self.transaction_state,
        )
    }

    pub(super) fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    #[cfg(test)]
    pub(super) fn transaction_state(&self) -> &str {
        &self.transaction_state
    }

    pub(super) fn retained_text_bytes(&self) -> Option<usize> {
        self.transactional_id
            .capacity()
            .checked_add(self.transaction_state.capacity())
    }

    #[cfg(test)]
    pub(crate) const fn producer_id(&self) -> i64 {
        self.producer_id
    }
}

/// Bounded, canonical facts from one API-key 66 broker response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListTransactionsResponseFacts {
    throttle_time_ms: u32,
    broker_error_code: Option<i16>,
    unknown_state_filters: Vec<String>,
    transactions: Vec<ListedTransaction>,
    retained_bytes: usize,
}

impl ListTransactionsResponseFacts {
    pub(super) const fn new(
        throttle_time_ms: u32,
        broker_error_code: Option<i16>,
        unknown_state_filters: Vec<String>,
        transactions: Vec<ListedTransaction>,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            broker_error_code,
            unknown_state_filters,
            transactions,
            retained_bytes,
        }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        throttle_time_ms: u32,
        broker_error_code: Option<i16>,
        unknown_state_filters: Vec<String>,
        transactions: Vec<ListedTransaction>,
        retained_bytes: usize,
    ) -> Self {
        Self::new(
            throttle_time_ms,
            broker_error_code,
            unknown_state_filters,
            transactions,
            retained_bytes,
        )
    }

    pub(crate) fn into_parts(
        self,
    ) -> (u32, Option<i16>, Vec<String>, Vec<ListedTransaction>, usize) {
        (
            self.throttle_time_ms,
            self.broker_error_code,
            self.unknown_state_filters,
            self.transactions,
            self.retained_bytes,
        )
    }

    #[cfg(test)]
    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    #[cfg(test)]
    pub(crate) const fn broker_error_code(&self) -> Option<i16> {
        self.broker_error_code
    }

    #[cfg(test)]
    pub(crate) fn unknown_state_filters(&self) -> &[String] {
        &self.unknown_state_filters
    }

    #[cfg(test)]
    pub(crate) fn transactions(&self) -> &[ListedTransaction] {
        &self.transactions
    }

    #[cfg(test)]
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}
