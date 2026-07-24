//! Linearity and deadline ordering for handle-bound position controls.

use std::time::Duration;

use super::{
    AssignedConsumerControlErrorKind, AssignedConsumerHandle, AssignedConsumerResumeCapture,
    AssignedConsumerSeekCapture,
};

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Implemented;
            trait AmbiguousIfImplemented<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
            impl<T: ?Sized + $trait> AmbiguousIfImplemented<Implemented> for T {}
            let _ = <$type as AmbiguousIfImplemented<_>>::check;
        };
    };
}

#[test]
fn captures_are_linear_handle_bound_deadlines() {
    fn require_resume(
        _capture: for<'handle> fn(
            &'handle mut AssignedConsumerHandle,
            Duration,
        ) -> Result<
            AssignedConsumerResumeCapture<'handle>,
            super::AssignedConsumerControlError,
        >,
    ) {
    }
    fn require_seek(
        _capture: for<'handle> fn(
            &'handle mut AssignedConsumerHandle,
            Duration,
        ) -> Result<
            AssignedConsumerSeekCapture<'handle>,
            super::AssignedConsumerControlError,
        >,
    ) {
    }

    require_resume(AssignedConsumerHandle::capture_resume);
    require_seek(AssignedConsumerHandle::capture_seek);
    assert_not_impl!(AssignedConsumerResumeCapture<'static>: Clone);
    assert_not_impl!(AssignedConsumerResumeCapture<'static>: Copy);
    assert_not_impl!(AssignedConsumerSeekCapture<'static>: Clone);
    assert_not_impl!(AssignedConsumerSeekCapture<'static>: Copy);
}

#[test]
fn overflowing_capture_rejects_before_any_control_input_exists() {
    let (_owner, port, _wake) = super::shard_test::setup();
    let (slot, _closer) = super::AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: std::sync::Arc<dyn Send + Sync> = std::sync::Arc::new(());
    let mut handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    let resume = handle
        .capture_resume(Duration::MAX)
        .err()
        .unwrap_or_else(|| panic!("resume capture must reject"));
    let seek = handle
        .capture_seek(Duration::MAX)
        .err()
        .unwrap_or_else(|| panic!("seek capture must reject"));

    assert_eq!(
        resume.kind(),
        AssignedConsumerControlErrorKind::DeadlineOverflow
    );
    assert_eq!(
        seek.kind(),
        AssignedConsumerControlErrorKind::DeadlineOverflow
    );
}
