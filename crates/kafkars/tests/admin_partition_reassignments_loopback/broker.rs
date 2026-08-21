//! Two-node topology ownership and exact caller-owned API 45 observations.
#![allow(
    clippy::needless_pass_by_value,
    reason = "the broker thread transfers exact listener and shared-state ownership"
)]

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

use super::{
    frame::RequestFrame,
    observation::{RequestObservation, Workflow},
    responses,
};

pub(crate) struct PartitionReassignmentsBroker {
    endpoint: String,
    state: Arc<BrokerState>,
    workers: Vec<JoinHandle<()>>,
}

impl PartitionReassignmentsBroker {
    pub(crate) fn start(workflow: Workflow) -> Self {
        let bootstrap = listener(2);
        let controller = listener(7);
        let ports = responses::BrokerPorts {
            bootstrap: port(&bootstrap, 2),
            controller: port(&controller, 7),
        };
        let state = Arc::new(BrokerState {
            stop: AtomicBool::new(false),
            keys: Mutex::new(Vec::new()),
            alterations: Mutex::new(Vec::new()),
            ports,
            workflow,
        });
        let workers = [(2, bootstrap), (7, controller)]
            .into_iter()
            .map(|(node_id, listener)| {
                let worker_state = Arc::clone(&state);
                thread::spawn(move || serve(listener, node_id, worker_state))
            })
            .collect();
        Self {
            endpoint: format!("127.0.0.1:{}", ports.bootstrap),
            state,
            workers,
        }
    }

    pub(crate) fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    pub(crate) fn observation_summary(&self) -> String {
        format!(
            "keys={:?}, alterations={:?}",
            self.state
                .keys
                .lock()
                .unwrap_or_else(|error| panic!("lock API 45 keys: {error}")),
            self.state
                .alterations
                .lock()
                .unwrap_or_else(|error| panic!("lock API 45 alterations: {error}")),
        )
    }

    pub(crate) fn assert_refreshed_without_replay(&self) {
        assert_eq!(self.state.workflow, Workflow::ControllerRecovery);
        let keys = self
            .state
            .keys
            .lock()
            .unwrap_or_else(|error| panic!("lock API 45 recovery keys: {error}"));
        assert_controller_recovery(&keys, 1);
    }

    pub(crate) fn assert_complete(mut self) {
        self.stop_and_join();
        let keys = self
            .state
            .keys
            .lock()
            .unwrap_or_else(|error| panic!("lock API 45 keys: {error}"));
        assert!(
            keys.iter()
                .any(|(node_id, key)| *node_id == 2 && *key == responses::METADATA),
            "controller routing must discover topology from bootstrap broker 2: {keys:?}"
        );
        let alterations = keys
            .iter()
            .filter(|(_node_id, key)| *key == responses::ALTER_PARTITION_REASSIGNMENTS)
            .copied()
            .collect::<Vec<_>>();
        let expected_count = match self.state.workflow {
            Workflow::Standard => 1,
            Workflow::ControllerRecovery => {
                assert_controller_recovery(&keys, 2);
                2
            }
        };
        assert_eq!(
            alterations,
            vec![(7, responses::ALTER_PARTITION_REASSIGNMENTS); expected_count],
            "destructive API 45 calls must remain caller-owned and route only to controller 7"
        );
        drop(keys);

        let alterations = self
            .state
            .alterations
            .lock()
            .unwrap_or_else(|error| panic!("lock API 45 alterations: {error}"));
        assert_eq!(alterations.len(), expected_count);
        for request in alterations.iter() {
            request.assert_exact();
        }
    }

    fn stop_and_join(&mut self) {
        self.state.stop.store(true, Ordering::Release);
        for worker in self.workers.drain(..) {
            let joined = worker.join();
            if !thread::panicking() {
                assert!(joined.is_ok(), "API 45 broker must finish cleanly");
            }
        }
    }
}

impl Drop for PartitionReassignmentsBroker {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

struct BrokerState {
    stop: AtomicBool,
    keys: Mutex<Vec<(i32, i16)>>,
    alterations: Mutex<Vec<RequestObservation>>,
    ports: responses::BrokerPorts,
    workflow: Workflow,
}

fn listener(node_id: i32) -> TcpListener {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind API 45 broker {node_id}: {error}"));
    listener
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make API 45 broker {node_id} nonblocking: {error}"));
    listener
}

fn port(listener: &TcpListener, node_id: i32) -> u16 {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read API 45 broker {node_id} endpoint: {error}"))
        .port()
}

fn serve(listener: TcpListener, node_id: i32, state: Arc<BrokerState>) {
    let mut peers = Vec::new();
    while !state.stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((peer, _address)) => {
                let peer_state = Arc::clone(&state);
                peers.push(thread::spawn(move || {
                    serve_peer(peer, node_id, &peer_state);
                }));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("accept API 45 broker {node_id}: {error}"),
        }
    }
    for peer in peers {
        peer.join()
            .unwrap_or_else(|_error| panic!("API 45 broker {node_id} peer must finish cleanly"));
    }
}

fn serve_peer(mut peer: TcpStream, node_id: i32, state: &BrokerState) {
    peer.set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap_or_else(|error| panic!("bound API 45 broker {node_id} read: {error}"));
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
            Err(error) => panic!("read API 45 broker {node_id}: {error}"),
        };
        observe(&request, node_id, state);
        let controller_refreshed = controller_refreshed(state);
        let response = responses::for_request(
            &request,
            node_id,
            state.ports,
            state.workflow,
            controller_refreshed,
        );
        if let Err(error) = peer.write_all(&response) {
            if matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
            ) {
                return;
            }
            panic!("write API 45 broker {node_id}: {error}");
        }
    }
}

fn observe(request: &RequestFrame, node_id: i32, state: &BrokerState) {
    state
        .keys
        .lock()
        .unwrap_or_else(|error| panic!("record API 45 key: {error}"))
        .push((node_id, request.api_key));
    if request.api_key == responses::ALTER_PARTITION_REASSIGNMENTS {
        state
            .alterations
            .lock()
            .unwrap_or_else(|error| panic!("record API 45 request: {error}"))
            .push(RequestObservation::decode(request, node_id));
    }
}

fn controller_refreshed(state: &BrokerState) -> bool {
    let keys = state
        .keys
        .lock()
        .unwrap_or_else(|error| panic!("inspect API 45 controller refresh: {error}"));
    let Some(first_call) = keys
        .iter()
        .position(|(_node_id, key)| *key == responses::ALTER_PARTITION_REASSIGNMENTS)
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
            (*key == responses::ALTER_PARTITION_REASSIGNMENTS).then_some(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        calls.len(),
        expected_calls,
        "API 45 must not replay without a caller submission: {keys:?}"
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
