//! Public share close admission and terminal-observer type contract.

use super::{CloseShareConsumer, ShareConsumer, ShareConsumerCloseAdmissionError};

#[test]
#[expect(
    clippy::result_large_err,
    reason = "the API contract deliberately returns the exact unique share consumer"
)]
fn close_consumes_the_unique_handle_and_returns_one_terminal_observer() {
    fn close_contract(
        consumer: ShareConsumer,
    ) -> Result<CloseShareConsumer, ShareConsumerCloseAdmissionError> {
        consumer.try_close()
    }

    let _ = close_contract
        as fn(ShareConsumer) -> Result<CloseShareConsumer, ShareConsumerCloseAdmissionError>;
}
