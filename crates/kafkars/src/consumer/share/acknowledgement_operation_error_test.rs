//! Public share acknowledgement operation-error ownership contract.

use super::{
    ShareAcknowledgement, ShareAcknowledgementAdmissionError, ShareAcknowledgementBrokerError,
    ShareAcknowledgementError,
};

#[test]
fn operation_errors_expose_exact_recoverable_capabilities() {
    fn admission_contract(error: ShareAcknowledgementAdmissionError) {
        let _: &ShareAcknowledgement = error.acknowledgement();
        let _: &crate::KafkaError = error.error();
        let _: (ShareAcknowledgement, crate::KafkaError) = error.into_parts();
    }
    fn terminal_contract(error: ShareAcknowledgementError) {
        let _: Option<&ShareAcknowledgement> = error.acknowledgement();
        let _: &crate::KafkaError = error.error();
        let _: Option<&ShareAcknowledgementBrokerError> = error.broker();
        let _: (
            Option<ShareAcknowledgement>,
            crate::KafkaError,
            Option<ShareAcknowledgementBrokerError>,
        ) = error.into_parts();
    }
    fn broker_contract(error: &ShareAcknowledgementBrokerError) {
        let _: u32 = error.throttle_time_ms();
        let _: i16 = error.broker_code();
        let _: Option<&[u8]> = error.message();
    }

    let _ = admission_contract as fn(ShareAcknowledgementAdmissionError);
    let _ = terminal_contract as fn(ShareAcknowledgementError);
    let _ = broker_contract as fn(&ShareAcknowledgementBrokerError);
}
