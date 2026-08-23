//! Consumer-domain host diagnostics without collapsing concrete owner identity.

use std::fmt;

use super::host::EngineHostError;

pub(super) fn display(error: &EngineHostError, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match error {
        EngineHostError::GroupConsumer(error) => {
            write!(formatter, "group-consumer registry failed: {error}")
        }
        EngineHostError::GroupConsumerLockPoisoned => {
            formatter.write_str("group-consumer registry ownership lock is poisoned")
        }
        EngineHostError::GroupConsumerRecvNotifierUnavailable => {
            formatter.write_str("group-consumer receive notifier is unavailable")
        }
        EngineHostError::ShareConsumer(error) => {
            write!(formatter, "share-consumer registry failed: {error:?}")
        }
        EngineHostError::ShareConsumerLockPoisoned => {
            formatter.write_str("share-consumer registry ownership lock is poisoned")
        }
        EngineHostError::ShareConsumerUnsettled(count) => {
            write!(
                formatter,
                "{count} share-consumer obligations remain retained"
            )
        }
        _ => unreachable!("consumer formatter received a non-consumer host error"),
    }
}
