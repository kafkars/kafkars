//! Fair response transfer and exact return across broker-local share sessions.

use kafka_client_core::Moment;

use super::super::{
    fetch_delivery::{
        ShareFetchDelivery, ShareFetchDeliveryReclaimError, ShareFetchDeliveryTransferError,
    },
    fetch_session_set::ShareFetchSessionSet,
};

impl ShareFetchSessionSet {
    pub(in crate::consumer::share) fn prepare_session(
        &mut self,
        index: usize,
        capture: crate::clock::DeadlineCapture,
    ) -> Result<(), super::super::fetch_session::ShareFetchSessionOwnerError> {
        self.sessions
            .get_mut(index)
            .ok_or(super::super::fetch_session::ShareFetchSessionOwnerError::Occupied)?
            .prepare_next(capture)
    }

    pub(in crate::consumer::share) fn take_delivery(
        &mut self,
        now: Moment,
    ) -> Result<Option<ShareFetchDelivery>, ShareFetchSessionSetDeliveryError> {
        let count = self.sessions.len();
        for offset in 0..count {
            let index = self
                .delivery_cursor
                .checked_add(offset)
                .map(|index| index % count)
                .ok_or(ShareFetchSessionSetDeliveryError::Cursor)?;
            match self.sessions[index].take_delivery(now) {
                Ok(Some(delivery)) => {
                    self.delivery_cursor = (index + 1) % count;
                    return Ok(Some(delivery));
                }
                Ok(None) => {}
                Err(error) => return Err(ShareFetchSessionSetDeliveryError::Session(error)),
            }
        }
        Ok(None)
    }

    pub(in crate::consumer::share) fn reclaim_delivery(
        &mut self,
        delivery: ShareFetchDelivery,
    ) -> Result<(), ShareFetchSessionSetReclaimError> {
        let fence = delivery.fence();
        let Some(session) = self
            .sessions
            .iter_mut()
            .find(|session| session.owns_delivery_fence(fence))
        else {
            return Err(ShareFetchSessionSetReclaimError::Unknown(delivery));
        };
        session
            .reclaim_delivery(delivery)
            .map_err(ShareFetchSessionSetReclaimError::Session)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareFetchSessionSetDeliveryError {
    Cursor,
    Session(ShareFetchDeliveryTransferError),
}

#[must_use = "a rejected share delivery remains owned by its exact caller"]
pub(in crate::consumer::share) enum ShareFetchSessionSetReclaimError {
    Unknown(ShareFetchDelivery),
    Session(ShareFetchDeliveryReclaimError),
}

impl ShareFetchSessionSetReclaimError {
    pub(in crate::consumer::share) fn into_delivery(self) -> ShareFetchDelivery {
        match self {
            Self::Unknown(delivery) => delivery,
            Self::Session(error) => error.into_delivery(),
        }
    }
}
