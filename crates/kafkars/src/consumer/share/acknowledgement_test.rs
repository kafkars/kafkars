//! Public share acknowledgement capability contract.

use super::{ShareAcknowledgement, ShareDisposition, ShareRecordDecision};

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
fn dispositions_are_exact_and_acknowledgement_ownership_is_linear() {
    fn require_send<T: Send>() {}
    fn decision_contract(decision: ShareRecordDecision) {
        let _: i64 = decision.offset();
        let _: ShareDisposition = decision.disposition();
    }
    fn acknowledgement_contract(acknowledgement: &ShareAcknowledgement) {
        let _: usize = acknowledgement.acquisition_count();
        let _: usize = acknowledgement.range_count();
    }

    assert_eq!(ShareDisposition::Accept, ShareDisposition::Accept);
    assert_eq!(ShareDisposition::Release, ShareDisposition::Release);
    assert_eq!(ShareDisposition::Reject, ShareDisposition::Reject);
    require_send::<ShareAcknowledgement>();
    assert_not_impl!(ShareAcknowledgement: Clone);
    assert_not_impl!(ShareAcknowledgement: Copy);
    let _ = decision_contract as fn(ShareRecordDecision);
    let _ = acknowledgement_contract as fn(&ShareAcknowledgement);
}
