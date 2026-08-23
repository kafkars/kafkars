//! Public-boundary closure of every engine admission domain.

use std::time::Duration;

use super::EngineInner;

const SHARE_CONTROL_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

impl EngineInner {
    pub(super) fn close_admission(&self) {
        let _close_result = self.admission.close_admission();
        self.close_admin_admission();
        let _close_result = self.assigned_consumer_admission.close();
        self.group_consumer.close_admission();
        let _close_result = self
            .share_consumer
            .request_control_close(SHARE_CONTROL_CLOSE_TIMEOUT);
        self.transaction_initialization.close_admission();
    }
}
