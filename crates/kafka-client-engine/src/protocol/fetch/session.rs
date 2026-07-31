//! Exact request metadata for one broker-owned incremental Fetch session.

const LEGACY_SESSION_ID: i32 = 0;
const LEGACY_SESSION_EPOCH: i32 = -1;
const INITIAL_SESSION_EPOCH: i32 = 0;
const FIRST_INCREMENTAL_SESSION_EPOCH: i32 = 1;
const FINAL_SESSION_EPOCH: i32 = -1;

/// Fetch-session request state retained by the concrete execution owner.
///
/// The legacy value preserves compatibility below Fetch v7. The initial value
/// asks a v7+ broker to establish a session, while an incremental value names
/// the exact broker session and request epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchSessionRequest {
    session_id: i32,
    session_epoch: i32,
}

impl FetchSessionRequest {
    pub(crate) const LEGACY: Self = Self {
        session_id: LEGACY_SESSION_ID,
        session_epoch: LEGACY_SESSION_EPOCH,
    };

    pub(crate) const INITIAL: Self = Self {
        session_id: LEGACY_SESSION_ID,
        session_epoch: INITIAL_SESSION_EPOCH,
    };

    pub(crate) const fn incremental(session_id: i32, session_epoch: i32) -> Option<Self> {
        if session_id > 0 && session_epoch > 0 {
            Some(Self {
                session_id,
                session_epoch,
            })
        } else {
            None
        }
    }

    /// Converts live incremental metadata into Kafka's final session request.
    pub(crate) const fn close(self) -> Option<Self> {
        if self.session_id > 0 && self.session_epoch > 0 {
            Some(Self {
                session_id: self.session_id,
                session_epoch: FINAL_SESSION_EPOCH,
            })
        } else {
            None
        }
    }

    pub(crate) const fn session_id(self) -> i32 {
        self.session_id
    }

    pub(crate) const fn session_epoch(self) -> i32 {
        self.session_epoch
    }

    pub(crate) const fn is_legacy(self) -> bool {
        self.session_id == LEGACY_SESSION_ID && self.session_epoch == LEGACY_SESSION_EPOCH
    }

    pub(crate) const fn is_initial(self) -> bool {
        self.session_id == LEGACY_SESSION_ID && self.session_epoch == INITIAL_SESSION_EPOCH
    }

    pub(crate) const fn is_incremental(self) -> bool {
        self.session_id > 0 && self.session_epoch > 0
    }

    pub(crate) const fn is_close(self) -> bool {
        self.session_id > 0 && self.session_epoch == FINAL_SESSION_EPOCH
    }

    pub(crate) const fn next_incremental_epoch(self) -> Option<i32> {
        if self.session_id <= 0 || self.session_epoch <= 0 {
            return None;
        }
        Some(if self.session_epoch == i32::MAX {
            FIRST_INCREMENTAL_SESSION_EPOCH
        } else {
            self.session_epoch + 1
        })
    }
}
