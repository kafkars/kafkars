//! Panic-safe host cleanup and retained lifecycle publication.

use std::panic::{AssertUnwindSafe, catch_unwind};

use super::{
    EngineHostError, EngineHostExit, EngineHostResources, EngineLifecycle, recovery::recover, run,
};

pub(super) fn finish_host(mut resources: EngineHostResources, lifecycle: &EngineLifecycle) {
    publish_caught(lifecycle, move || {
        let outcome = catch_unwind(AssertUnwindSafe(|| run(&mut resources)));
        let exit = match outcome {
            Ok(Ok(exit)) => exit,
            Ok(Err(error)) => recover(&mut resources, error),
            Err(_panic) => recover(&mut resources, EngineHostError::HostPanicked),
        };
        let failure = finalize_exit(exit);
        drop(resources);
        failure
    });
}

pub(super) fn publish_caught(
    lifecycle: &EngineLifecycle,
    finalize: impl FnOnce() -> Option<EngineHostError>,
) {
    let outcome = catch_unwind(AssertUnwindSafe(finalize));
    match outcome {
        Ok(failure) => lifecycle.publish(failure.as_ref()),
        Err(_panic) => lifecycle.publish(Some(&EngineHostError::HostPanicked)),
    }
}

pub(super) fn finalize_exit(mut exit: EngineHostExit) -> Option<EngineHostError> {
    let mut failure = exit.failure.take();
    if let Err(cleanup) = exit.notifier.join_off_notifier() {
        failure = Some(attach_cleanup(failure, EngineHostError::Notifier(cleanup)));
    }
    failure
}

fn attach_cleanup(primary: Option<EngineHostError>, cleanup: EngineHostError) -> EngineHostError {
    match primary {
        Some(primary) => primary.with_cleanup(cleanup),
        None => cleanup,
    }
}
