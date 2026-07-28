//! Two-node topology ownership and exact caller-owned API 46 observations.

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

use kafka_wire::ListPartitionReassignmentsRequest;

use super::{
    frame::RequestFrame,
    observation::{ListObservation, Workflow},
    responses,
};

pub(crate) struct ListPartitionReassignmentsBroker {
    bootstrap_endpoint: String,
    state: Arc<BrokerState>,
    workers: Vec<JoinHandle<()>>,
}

impl ListPartitionReassignmentsBroker {
    pub(crate) fn start(workflow: Workflow) -> Self {
        let broker_2 = listener(2);
        let broker_7 = listener(7);
        let broker_2_port = port(&broker_2, 2);
        let bootstrap_endpoint = format!("127.0.0.1:{broker_2_port}");
        let state = Arc::new(BrokerState {
            stop: AtomicBool::new(false),
            keys: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
            workflow,
            broker_2_port,
            broker_7_port: port(&broker_7, 7),
        });
        let workers = vec![
            spawn_listener(broker_2, 2, Arc::clone(&state)),
            spawn_listener(broker_7, 7, Arc::clone(&state)),
        ];
        Self {
            bootstrap_endpoint,
            state,
            workers,
        }
    }

    pub(crate) fn bootstrap_endpoint(&self) -> String {
        self.bootstrap_endpoint.clone()
    }

    pub(crate) fn observation_summary(&self) -> String {
        format!(
            "keys={:?}, requests={:?}",
            self.state
                .keys
                .lock()
                .unwrap_or_else(|error| panic!("lock API 46 keys: {error}")),
            self.state
                .requests
                .lock()
                .unwrap_or_else(|error| panic!("lock API 46 requests: {error}")),
        )
    }

    pub(crate) fn assert_refreshed_without_replay(&self) {
        assert_eq!(self.state.workflow, Workflow::ControllerRecovery);
        let keys = self
            .state
            .keys
            .lock()
            .unwrap_or_else(|error| panic!("lock API 46 recovery keys: {error}"));
        assert_controller_recovery(&keys, 1);
    }

    pub(crate) fn assert_complete(mut self) {
        self.stop_and_join();
        let keys = self
            .state
            .keys
            .lock()
            .unwrap_or_else(|error| panic!("lock API 46 keys: {error}"));
        assert!(
            keys.iter().any(|(_, key)| *key == responses::METADATA),
            "controller API 46 routing must discover topology: {keys:?}"
        );
        let calls = keys
            .iter()
            .filter(|(_node_id, key)| *key == responses::LIST_PARTITION_REASSIGNMENTS)
            .copied()
            .collect::<Vec<_>>();
        let expected_count = if self.state.workflow == Workflow::ControllerRecovery {
            assert_controller_recovery(&keys, 2);
            2
        } else {
            1
        };
        assert_eq!(
            calls,
            vec![(7, responses::LIST_PARTITION_REASSIGNMENTS); expected_count],
            "API 46 calls must remain caller-owned and route only to controller 7"
        );
        drop(keys);
        let requests = self
            .state
            .requests
            .lock()
            .unwrap_or_else(|error| panic!("lock API 46 requests: {error}"));
        assert_eq!(
            requests.as_slice(),
            vec![ListObservation::expected(self.state.workflow); expected_count],
            "API 46 must select v0 and retain explicit selected-vs-all semantics"
        );
    }

    fn stop_and_join(&mut self) {
        self.state.stop.store(true, Ordering::Release);
        for worker in self.workers.drain(..) {
            let joined = worker.join();
            if !thread::panicking() {
                assert!(joined.is_ok(), "API 46 broker must finish cleanly");
            }
        }
    }
}

impl Drop for ListPartitionReassignmentsBroker {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

struct BrokerState {
    stop: AtomicBool,
    keys: Mutex<Vec<(i32, i16)>>,
    requests: Mutex<Vec<ListObservation>>,
    workflow: Workflow,
    broker_2_port: u16,
    broker_7_port: u16,
}

fn listener(node_id: i32) -> TcpListener {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind API 46 broker {node_id}: {error}"));
    listener
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make API 46 broker {node_id} nonblocking: {error}"));
    listener
}

fn port(listener: &TcpListener, node_id: i32) -> u16 {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read API 46 broker {node_id} port: {error}"))
        .port()
}

fn spawn_listener(listener: TcpListener, node_id: i32, state: Arc<BrokerState>) -> JoinHandle<()> {
    thread::spawn(move || serve(&listener, node_id, &state))
}

fn serve(listener: &TcpListener, node_id: i32, state: &Arc<BrokerState>) {
    let mut peers = Vec::new();
    while !state.stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((peer, _address)) => {
                let peer_state = Arc::clone(state);
                peers.push(thread::spawn(move || {
                    serve_peer(peer, node_id, &peer_state);
                }));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("accept API 46 broker {node_id}: {error}"),
        }
    }
    for peer in peers {
        peer.join()
            .unwrap_or_else(|_error| panic!("API 46 broker {node_id} peer must finish cleanly"));
    }
}

fn serve_peer(mut peer: TcpStream, node_id: i32, state: &BrokerState) {
    peer.set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap_or_else(|error| panic!("bound API 46 broker {node_id} read: {error}"));
    while !state.stop.load(Ordering::Acquire) {
        let request = match RequestFrame::read(&mut peer) {
            Ok(request) => request,
            Err(error) if transient(&error) => continue,
            Err(error) if disconnected(&error) => return,
            Err(error) => panic!("read API 46 broker {node_id}: {error}"),
        };
        observe_request(&request, node_id, state);
        let controller_refreshed = controller_refreshed(state);
        let response = responses::for_request(
            &request,
            node_id,
            state.broker_2_port,
            state.broker_7_port,
            state.workflow,
            controller_refreshed,
        );
        if let Err(error) = peer.write_all(&response) {
            if disconnected(&error) {
                return;
            }
            panic!("write API 46 broker {node_id}: {error}");
        }
    }
}

fn observe_request(request: &RequestFrame, node_id: i32, state: &BrokerState) {
    state
        .keys
        .lock()
        .unwrap_or_else(|error| panic!("record API 46 key: {error}"))
        .push((node_id, request.api_key));
    if request.api_key != responses::LIST_PARTITION_REASSIGNMENTS {
        return;
    }
    let decoded: ListPartitionReassignmentsRequest = request.decode();
    state
        .requests
        .lock()
        .unwrap_or_else(|error| panic!("record API 46 request: {error}"))
        .push(ListObservation::from_request(
            node_id,
            request.api_version.value(),
            decoded,
        ));
}

fn controller_refreshed(state: &BrokerState) -> bool {
    let keys = state
        .keys
        .lock()
        .unwrap_or_else(|error| panic!("inspect API 46 controller refresh: {error}"));
    let Some(first_call) = keys
        .iter()
        .position(|(_node_id, key)| *key == responses::LIST_PARTITION_REASSIGNMENTS)
    else {
        return false;
    };
    keys[first_call + 1..]
        .iter()
        .any(|(_node_id, key)| *key == responses::METADATA)
}

fn assert_controller_recovery(keys: &[(i32, i16)], expected_calls: usize) {
    let calls = keys
        .iter()
        .enumerate()
        .filter_map(|(index, (_node_id, key))| {
            (*key == responses::LIST_PARTITION_REASSIGNMENTS).then_some(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        calls.len(),
        expected_calls,
        "API 46 must not replay without a caller submission: {keys:?}"
    );
    let refresh = keys
        .iter()
        .enumerate()
        .find_map(|(index, (_node_id, key))| {
            (index > calls[0] && *key == responses::METADATA).then_some(index)
        })
        .unwrap_or_else(|| panic!("code 41 must cause a causal metadata refresh: {keys:?}"));
    if let Some(second_call) = calls.get(1) {
        assert!(
            refresh < *second_call,
            "caller retry must follow the controller refresh: {keys:?}"
        );
    }
}

fn transient(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

fn disconnected(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
    )
}
