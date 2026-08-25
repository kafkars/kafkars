//! Public immutable direct-consumer Fetch evidence shape contract.

use crate::TopicUuid;

use super::ConsumerFetchEvidence;

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
fn evidence_is_lease_borrowed_and_exposes_exact_correlated_facts() {
    fn contract(evidence: &ConsumerFetchEvidence) {
        let _: &str = evidence.topic();
        let _: TopicUuid = evidence.topic_uuid();
        let _: i32 = evidence.partition();
        let _: i64 = evidence.requested_offset();
        let _: i64 = evidence.next_offset();
        let _: Option<i64> = evidence.log_start_offset();
        let _: Option<i64> = evidence.last_stable_offset();
        let _: Option<i64> = evidence.high_watermark();
        let _: usize = evidence.retained_bytes();
    }

    assert_not_impl!(ConsumerFetchEvidence: Clone);
    assert_not_impl!(ConsumerFetchEvidence: Copy);
    let _ = contract as fn(&ConsumerFetchEvidence);
}
