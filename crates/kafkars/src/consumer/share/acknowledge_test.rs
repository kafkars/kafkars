//! Public share acknowledgement operation contract.

use std::{future::Future, time::Duration};

use super::{
    AcknowledgeShareConsumer, ShareAcknowledgement, ShareAcknowledgementAdmissionError,
    ShareAcknowledgementError, ShareAcknowledgementResponse, ShareConsumer,
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
fn acknowledgement_is_one_named_runtime_neutral_operation() {
    fn require_send<T: Send>() {}
    fn require_future<
        T: Future<Output = Result<ShareAcknowledgementResponse, ShareAcknowledgementError>>,
    >() {
    }
    let _: fn(
        &mut ShareConsumer,
        ShareAcknowledgement,
        Duration,
    ) -> Result<AcknowledgeShareConsumer, ShareAcknowledgementAdmissionError> =
        ShareConsumer::try_acknowledge;

    require_send::<AcknowledgeShareConsumer>();
    require_future::<AcknowledgeShareConsumer>();
    assert_not_impl!(AcknowledgeShareConsumer: Clone);
    assert_not_impl!(AcknowledgeShareConsumer: Copy);
}
