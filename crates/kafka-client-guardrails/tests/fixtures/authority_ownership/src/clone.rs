//! Invalid duplication traits on an authority token.

#[derive(Clone, Copy)]
pub(crate) struct NotifierPendingDispatchOwner {
    _seal: (),
}
