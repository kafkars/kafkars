//! Per-scenario duration evidence written without retaining cluster diagnostics.

use std::{env, fs::OpenOptions, io::Write, time::Instant};

use crate::real_broker_support::TestError;

pub(super) fn measure<T>(
    scenario: &'static str,
    operation: impl FnOnce() -> Result<T, TestError>,
) -> Result<T, TestError> {
    let started = Instant::now();
    let result = operation();
    let status = if result.is_ok() { "passed" } else { "failed" };
    append(scenario, status, started.elapsed().as_millis())?;
    result
}

fn append(scenario: &str, status: &str, duration_ms: u128) -> Result<(), TestError> {
    let Ok(path) = env::var("KAFKARS_QUALIFICATION_EVENTS") else {
        return Ok(());
    };
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{scenario}\t{status}\t{duration_ms}")?;
    Ok(())
}
