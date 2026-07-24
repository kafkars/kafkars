//! One native compression worker loop with no scheduling or policy authority.

use std::sync::{
    Mutex,
    mpsc::{Receiver, SyncSender},
};

use super::{CompressionCompletion, CompressionJob};
use crate::producer::ingress::ProducerShardWake;

pub(super) fn run<W>(
    jobs: &Mutex<Receiver<CompressionJob>>,
    results: &SyncSender<CompressionCompletion>,
    wake: &W,
) where
    W: ProducerShardWake,
{
    loop {
        let Ok(Ok(job)) = jobs.lock().map(|receiver| receiver.recv()) else {
            break;
        };
        if results.send(job.run()).is_err() {
            break;
        }
        let _wake_result = wake.wake();
    }
}
