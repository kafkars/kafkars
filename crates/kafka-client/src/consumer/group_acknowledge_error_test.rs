//! Public acknowledgment-rejection ownership contract.

use super::{Checkpoint, ConsumerAcknowledgeError};

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
fn rejection_exposes_and_returns_the_exact_checkpoint() {
    fn contract(error: ConsumerAcknowledgeError) {
        let _: &crate::KafkaError = error.error();
        let _: &Checkpoint = error.checkpoint();
        let _: (Checkpoint, crate::KafkaError) = error.into_parts();
    }

    assert_not_impl!(ConsumerAcknowledgeError: Clone);
    assert_not_impl!(ConsumerAcknowledgeError: Copy);
    let _ = contract as fn(ConsumerAcknowledgeError);
}
