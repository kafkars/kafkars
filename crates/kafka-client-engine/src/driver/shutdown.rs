//! Linear retention of the driver's shared explicit shutdown barrier.

use kafka_driver::{Call, CompletionError, Driver, SubmitError};

/// One retained subscription to the driver reactor's shared terminal barrier.
#[derive(Debug, Default)]
pub(super) struct DriverShutdown {
    call: Option<Call<()>>,
    settled: bool,
}

impl DriverShutdown {
    pub(super) fn begin(&mut self, driver: &Driver) -> Result<(), SubmitError> {
        if self.call.is_none() && !self.settled {
            self.call = Some(driver.shutdown()?);
        }
        Ok(())
    }

    pub(super) fn observe(&mut self) -> Result<(), CompletionError> {
        let Some(call) = self.call.as_ref() else {
            return Ok(());
        };
        let Some(result) = call.try_result() else {
            return Ok(());
        };
        result?;
        self.call = None;
        self.settled = true;
        Ok(())
    }

    pub(super) const fn is_started(&self) -> bool {
        self.call.is_some() || self.settled
    }

    pub(super) const fn is_settled(&self) -> bool {
        self.settled
    }
}
