//! Narrow qualification-owned broker disruption control boundary.

use std::{env, io, process::Command};

use crate::real_broker_support::TestError;

pub(super) struct BrokerGuard {
    broker_id: i32,
    restore_action: &'static str,
    restored: bool,
}

impl BrokerGuard {
    pub(super) fn stop(broker_id: i32) -> Result<Self, TestError> {
        invoke("stop", &broker_id.to_string())?;
        Ok(Self {
            broker_id,
            restore_action: "start",
            restored: false,
        })
    }

    pub(super) fn pause(broker_id: i32) -> Result<Self, TestError> {
        invoke("pause", &broker_id.to_string())?;
        Ok(Self {
            broker_id,
            restore_action: "unpause",
            restored: false,
        })
    }

    pub(super) fn restore(mut self) -> Result<(), TestError> {
        invoke(self.restore_action, &self.broker_id.to_string())?;
        self.restored = true;
        Ok(())
    }
}

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        if !self.restored {
            let _restore = invoke(self.restore_action, &self.broker_id.to_string());
        }
    }
}

pub(super) fn restart(broker_id: i32) -> Result<(), TestError> {
    invoke("restart", &broker_id.to_string()).map(|_| ())
}

pub(super) fn restart_coordinator(group_id: &str) -> Result<i32, TestError> {
    let output = invoke("restart-coordinator", group_id)?;
    output.trim().parse::<i32>().map_err(|error| {
        io::Error::other(format!(
            "qualification control returned invalid broker ID: {error}"
        ))
        .into()
    })
}

fn invoke(action: &str, target: &str) -> Result<String, TestError> {
    let command = env::var("KAFKARS_QUALIFICATION_CONTROL").map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "KAFKARS_QUALIFICATION_CONTROL is required for nightly disruptions",
        )
    })?;
    let output = Command::new(command).args([action, target]).output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "qualification control {action} {target} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
        .into());
    }
    String::from_utf8(output.stdout)
        .map_err(|error| io::Error::other(format!("control output was not UTF-8: {error}")).into())
}
