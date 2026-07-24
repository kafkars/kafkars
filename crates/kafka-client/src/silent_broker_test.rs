//! Negotiated loopback broker ownership for deterministic public deadlines.

use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel},
    thread::{self, JoinHandle},
    time::Duration,
};

const API_VERSIONS_KEY: i16 = 18;
const INIT_PRODUCER_ID_KEY: i16 = 22;

pub(crate) struct SilentBroker {
    endpoint: String,
    stop: SyncSender<()>,
    worker: Option<JoinHandle<()>>,
}

impl SilentBroker {
    pub(crate) fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|error| panic!("silent broker should bind: {error}"));
        listener.set_nonblocking(true).unwrap_or_else(|error| {
            panic!("silent broker listener should be nonblocking: {error}")
        });
        let endpoint = listener
            .local_addr()
            .unwrap_or_else(|error| panic!("silent broker should have an address: {error}"))
            .to_string();
        let (stop, stopped) = sync_channel(1);
        let worker = thread::spawn(move || serve(&listener, &stopped));
        Self {
            endpoint,
            stop,
            worker: Some(worker),
        }
    }

    pub(crate) fn endpoint(&self) -> String {
        self.endpoint.clone()
    }
}

impl Drop for SilentBroker {
    fn drop(&mut self) {
        let _sent = self.stop.try_send(());
        let Some(worker) = self.worker.take() else {
            return;
        };
        let joined = worker.join();
        if !thread::panicking() {
            assert!(
                joined.is_ok(),
                "silent broker worker should finish without panic"
            );
        }
    }
}

fn serve(listener: &TcpListener, stopped: &Receiver<()>) {
    let mut peer = accept_until_stopped(listener, stopped);
    peer.set_nonblocking(false)
        .unwrap_or_else(|error| panic!("silent broker peer should be blocking: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound silent broker reads: {error}"));
    let negotiation = read_frame(&mut peer);
    assert_eq!(api_key(&negotiation), API_VERSIONS_KEY);
    peer.write_all(&negotiation_response(correlation_id(&negotiation)))
        .unwrap_or_else(|error| panic!("write negotiation response: {error}"));
    // Keep the negotiated connection alive without acknowledging whichever
    // producer prerequisite the client submits first. The test concerns the
    // public deadline, not one incidental prerequisite order.
    let _stopped = stopped.recv_timeout(Duration::from_secs(2));
}

fn accept_until_stopped(listener: &TcpListener, stopped: &Receiver<()>) -> TcpStream {
    loop {
        match listener.accept() {
            Ok((peer, _address)) => return peer,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("accept silent broker connection: {error}"),
        }
        match stopped.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => {
                panic!("silent broker stopped before the client connected")
            }
            Err(TryRecvError::Empty) => thread::yield_now(),
        }
    }
}

fn read_frame(peer: &mut TcpStream) -> Vec<u8> {
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read request frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("request frame length must be nonnegative: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read request frame body: {error}"));
    body
}

fn api_key(frame: &[u8]) -> i16 {
    let bytes = frame
        .get(0..2)
        .and_then(|value| value.try_into().ok())
        .unwrap_or_else(|| panic!("request frame must carry an API key"));
    i16::from_be_bytes(bytes)
}

fn correlation_id(frame: &[u8]) -> i32 {
    let bytes = frame
        .get(4..8)
        .and_then(|value| value.try_into().ok())
        .unwrap_or_else(|| panic!("request frame must carry a correlation ID"));
    i32::from_be_bytes(bytes)
}

fn negotiation_response(correlation: i32) -> Vec<u8> {
    let mut frame = Vec::with_capacity(26);
    frame.extend_from_slice(&22_i32.to_be_bytes());
    frame.extend_from_slice(&correlation.to_be_bytes());
    frame.extend_from_slice(&0_i16.to_be_bytes());
    frame.extend_from_slice(&2_i32.to_be_bytes());
    for (api_key, maximum) in [(API_VERSIONS_KEY, 0_i16), (INIT_PRODUCER_ID_KEY, 5_i16)] {
        frame.extend_from_slice(&api_key.to_be_bytes());
        frame.extend_from_slice(&0_i16.to_be_bytes());
        frame.extend_from_slice(&maximum.to_be_bytes());
    }
    frame
}
