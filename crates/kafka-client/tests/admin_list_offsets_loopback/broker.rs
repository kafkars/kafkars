//! Two-node routing and exact single-attempt API-key 2 observations.

use std::{
    io::{self, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use kafka_wire::ListOffsetsRequest;

use super::{frame::RequestFrame, responses};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Workflow {
    Kafka43,
    NoEarliestPendingUpload,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Node {
    Bootstrap,
    Leader,
}

#[derive(Debug, Eq, PartialEq)]
struct ListOffsetsObservation {
    node: Node,
    version: i16,
    isolation_level: i8,
    timeout_ms: i32,
    topic: String,
    partition: i32,
    current_leader_epoch: i32,
    timestamp: i64,
}

pub(crate) struct ListOffsetsBroker {
    bootstrap_endpoint: String,
    state: Arc<BrokerState>,
    workers: Vec<JoinHandle<()>>,
}

impl ListOffsetsBroker {
    pub(crate) fn start(workflow: Workflow) -> Self {
        let bootstrap = listener("bootstrap");
        let leader = listener("leader");
        let bootstrap_endpoint = endpoint(&bootstrap, "bootstrap");
        let state = Arc::new(BrokerState {
            stop: AtomicBool::new(false),
            keys: Mutex::new(Vec::new()),
            observations: Mutex::new(Vec::new()),
            workflow,
            bootstrap_port: port(&bootstrap, "bootstrap"),
            leader_port: port(&leader, "leader"),
        });
        let workers = vec![
            spawn_listener(bootstrap, Node::Bootstrap, Arc::clone(&state)),
            spawn_listener(leader, Node::Leader, Arc::clone(&state)),
        ];
        Self {
            bootstrap_endpoint,
            state,
            workers,
        }
    }

    pub(crate) fn endpoint(&self) -> String {
        self.bootstrap_endpoint.clone()
    }

    pub(crate) fn assert_complete(mut self) {
        self.stop_and_join();
        let keys = self
            .state
            .keys
            .lock()
            .unwrap_or_else(|error| panic!("lock ListOffsets keys: {error}"));
        assert!(
            keys.iter().any(|(_node, key)| *key == responses::METADATA),
            "leader routing must consult Metadata: {keys:?}"
        );
        let calls = keys
            .iter()
            .filter(|(_node, key)| *key == responses::LIST_OFFSETS)
            .count();
        drop(keys);

        let observations = self
            .state
            .observations
            .lock()
            .unwrap_or_else(|error| panic!("lock ListOffsets observations: {error}"));
        match self.state.workflow {
            Workflow::Kafka43 => {
                assert_eq!(calls, 4, "each selected partition gets one physical call");
                assert_eq!(
                    observations
                        .iter()
                        .map(|observation| (
                            observation.node,
                            observation.version,
                            observation.isolation_level,
                            observation.topic.as_str(),
                            observation.partition,
                            observation.current_leader_epoch,
                            observation.timestamp,
                        ))
                        .collect::<Vec<_>>(),
                    [
                        (Node::Leader, 11, 1, "orders", 0, 41, -3),
                        (Node::Leader, 11, 1, "orders", 1, -1, -4),
                        (Node::Leader, 11, 1, "orders", 2, -1, -5),
                        (Node::Leader, 11, 1, "orders", 3, -1, -6),
                    ]
                );
                assert!(
                    observations[0].version >= 4,
                    "fenced ListOffsets requires a version that represents CurrentLeaderEpoch"
                );
                assert!(
                    observations
                        .iter()
                        .all(|observation| observation.timeout_ms > 0),
                    "v11 must carry the remaining public timeout"
                );
            }
            Workflow::NoEarliestPendingUpload => {
                assert_eq!(calls, 0, "v11-only intent must fail before transport");
                assert!(observations.is_empty());
            }
        }
    }

    fn stop_and_join(&mut self) {
        self.state.stop.store(true, Ordering::Release);
        for worker in self.workers.drain(..) {
            let joined = worker.join();
            if !thread::panicking() {
                assert!(joined.is_ok(), "ListOffsets broker must finish cleanly");
            }
        }
    }
}

impl Drop for ListOffsetsBroker {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

struct BrokerState {
    stop: AtomicBool,
    keys: Mutex<Vec<(Node, i16)>>,
    observations: Mutex<Vec<ListOffsetsObservation>>,
    workflow: Workflow,
    bootstrap_port: u16,
    leader_port: u16,
}

fn listener(name: &str) -> TcpListener {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind ListOffsets {name} listener: {error}"));
    listener
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make ListOffsets {name} listener nonblocking: {error}"));
    listener
}

fn endpoint(listener: &TcpListener, name: &str) -> String {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read ListOffsets {name} endpoint: {error}"))
        .to_string()
}

fn port(listener: &TcpListener, name: &str) -> u16 {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read ListOffsets {name} port: {error}"))
        .port()
}

fn spawn_listener(listener: TcpListener, node: Node, state: Arc<BrokerState>) -> JoinHandle<()> {
    thread::spawn(move || serve(&listener, node, &state))
}

fn serve(listener: &TcpListener, node: Node, state: &Arc<BrokerState>) {
    let mut peers = Vec::new();
    while !state.stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((peer, _address)) => {
                let peer_state = Arc::clone(state);
                peers.push(thread::spawn(move || serve_peer(peer, node, &peer_state)));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("accept ListOffsets {node:?} connection: {error}"),
        }
    }
    for peer in peers {
        peer.join()
            .unwrap_or_else(|_error| panic!("ListOffsets {node:?} peer must finish cleanly"));
    }
}

fn serve_peer(mut peer: TcpStream, node: Node, state: &BrokerState) {
    peer.set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap_or_else(|error| panic!("bound ListOffsets peer read: {error}"));
    while !state.stop.load(Ordering::Acquire) {
        let request = match RequestFrame::read(&mut peer) {
            Ok(request) => request,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::BrokenPipe
                ) =>
            {
                return;
            }
            Err(error) => panic!("read ListOffsets {node:?} request: {error}"),
        };
        observe_request(&request, node, state);
        let response = responses::for_request(
            &request,
            state.workflow,
            state.bootstrap_port,
            state.leader_port,
        );
        if let Err(error) = peer.write_all(&response) {
            if matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
            ) {
                return;
            }
            panic!("write ListOffsets {node:?} response: {error}");
        }
    }
}

fn observe_request(request: &RequestFrame, node: Node, state: &BrokerState) {
    state
        .keys
        .lock()
        .unwrap_or_else(|error| panic!("record ListOffsets key: {error}"))
        .push((node, request.api_key));
    if request.api_key != responses::LIST_OFFSETS {
        return;
    }

    let decoded: ListOffsetsRequest = request.decode();
    let [topic] = decoded.topics.as_slice() else {
        panic!("ListOffsets call must contain one topic")
    };
    let [partition] = topic.partitions.as_slice() else {
        panic!("ListOffsets call must contain one partition")
    };
    state
        .observations
        .lock()
        .unwrap_or_else(|error| panic!("record ListOffsets request: {error}"))
        .push(ListOffsetsObservation {
            node,
            version: request.api_version.value(),
            isolation_level: decoded.isolation_level,
            timeout_ms: decoded.timeout_ms,
            topic: topic.name.as_str().to_owned(),
            partition: partition.partition_index,
            current_leader_epoch: partition.current_leader_epoch,
            timestamp: partition.timestamp,
        });
}
