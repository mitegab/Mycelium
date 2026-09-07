//! The Mycelium node engine: one independent mesh participant.
//!
//! A `Node` owns its identity, its direct-neighbor table (constrained
//! visibility), its routing table, the timers that drive discovery pings and
//! OGMs, and the counters that describe its behavior. It communicates with
//! the outside world through:
//!
//! * [`RawDatagram`]s from the transport reader thread,
//! * [`Command`]s from local applications (the "Channel API"), and
//! * [`Event`]s delivered to local applications.
//!
//! The engine itself is a plain state machine with a single event loop; no
//! async runtime is required. The transport seam in [`crate::io`] is where a
//! tokio-based runtime can later be introduced.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::io::{spawn_reader, RawDatagram};
use crate::linkq::is_newer;
use crate::neighbors::NeighborTable;
use crate::routing::RouteTable;
use crate::types::{NodeId, Rng, COST_INF, DATA_TTL, MAX_PAYLOAD, OGM_TTL};
use crate::wire::{sanitize_payload, Advert, Body, Packet};

/// How long duplicate `Data` frames are remembered for loop suppression.
const DATA_SEEN_TTL: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// One configured direct neighbor ("what this node can see").
#[derive(Debug, Clone, Copy)]
pub struct PeerConfig {
    pub addr: SocketAddr,
    /// Simulated inbound loss from this peer in `[0,1]` (0 = none). This knob
    /// exists so simulations can exercise PDR-aware rerouting without a real
    /// lossy medium.
    pub sim_loss_in: f64,
}

impl PeerConfig {
    pub fn new(addr: SocketAddr) -> PeerConfig {
        PeerConfig { addr, sim_loss_in: 0.0 }
    }
}

/// Full configuration of a node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub name: String,
    pub node_id: NodeId,
    /// Local bind address (IPv4). Port 0 asks the OS for an ephemeral port.
    pub bind: SocketAddr,
    /// Directly visible peers; datagrams from any other address are dropped.
    pub peers: Vec<PeerConfig>,
    pub ogm_interval: Duration,
    pub ping_interval: Duration,
    pub neighbor_timeout: Duration,
    pub route_timeout: Duration,
    /// Deterministic RNG seed (0 = entropy). Used for tests and simulations.
    pub seed: u64,
}

impl Default for NodeConfig {
    fn default() -> NodeConfig {
        NodeConfig {
            name: "node".into(),
            node_id: NodeId::ZERO,
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            peers: Vec::new(),
            ogm_interval: Duration::from_millis(1000),
            ping_interval: Duration::from_millis(1000),
            neighbor_timeout: Duration::from_secs(4),
            route_timeout: Duration::from_secs(4),
            seed: 0,
        }
    }
}

impl NodeConfig {
    /// Timeouts must comfortably exceed the OGM interval or routes/neighbors
    /// would expire before a refresh arrives.
    pub fn validate(&self) -> Result<(), String> {
        if self.ogm_interval.is_zero() || self.ping_interval.is_zero() {
            return Err("intervals must be non-zero".into());
        }
        if self.neighbor_timeout < self.ogm_interval * 2 {
            return Err("neighbor_timeout must be >= 2x ogm_interval".into());
        }
        if self.route_timeout < self.ogm_interval * 2 {
            return Err("route_timeout must be >= 2x ogm_interval".into());
        }
        for p in &self.peers {
            if p.addr == self.bind {
                return Err(format!("peer {} is this node's own address", p.addr));
            }
            if !(0.0..=1.0).contains(&p.sim_loss_in) {
                return Err(format!("sim_loss_in for {} out of range", p.addr));
            }
        }
        Ok(())
    }

    /// Convenience builder for tests: quick timers on an ephemeral port.
    pub fn test_node(name: &str, node_id: NodeId, seed: u64) -> NodeConfig {
        NodeConfig {
            name: name.into(),
            node_id,
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            peers: Vec::new(),
            ogm_interval: Duration::from_millis(120),
            ping_interval: Duration::from_millis(120),
            neighbor_timeout: Duration::from_millis(600),
            route_timeout: Duration::from_millis(600),
            seed,
        }
    }
}

// ---------------------------------------------------------------------------
// Counters, snapshots, events, commands
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counters {
    pub tx_packets: u64,
    pub rx_packets: u64,
    pub rx_unknown_sender: u64,
    pub rx_sim_loss_dropped: u64,
    pub rx_parse_errors: u64,
    pub rx_stale_nonce: u64,
    pub ogms_sent: u64,
    pub pings_sent: u64,
    pub data_forwarded: u64,
    pub data_delivered: u64,
    pub data_unroutable: u64,
    pub data_duplicate: u64,
}

/// A point-in-time view of one direct neighbor.
#[derive(Debug, Clone)]
pub struct NeighborInfo {
    pub node_id: Option<NodeId>,
    pub addr: SocketAddr,
    /// Inbound PDR estimate for this neighbor's OGM stream, 0..1.
    pub link_quality: f64,
    pub sim_loss_in: f64,
    pub rx_frames: u64,
    pub tx_frames: u64,
    pub last_seen_ago: Duration,
}

/// A point-in-time view of one route.
#[derive(Debug, Clone)]
pub struct RouteInfo {
    pub dest: NodeId,
    pub cost: u16,
    pub next_hop: NodeId,
    pub via_ogm_hops: Option<u8>,
    pub age: Duration,
}

/// A consistent point-in-time view of a node's state.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub name: String,
    pub node_id: NodeId,
    pub bind: SocketAddr,
    pub neighbors: Vec<NeighborInfo>,
    pub routes: Vec<RouteInfo>,
    pub counters: Counters,
    pub routes_expired_total: u64,
}

/// Local application → node requests ("Channel API", MVP subset).
#[derive(Debug)]
pub enum Command {
    /// Send a payload toward `dst` (best-effort; no end-to-end ack in MVP).
    Send { dst: NodeId, payload: Vec<u8> },
    /// Adjust the simulated inbound loss of a direct peer by address.
    SetLoss { peer: SocketAddr, rate: f64 },
    /// Request a state snapshot (replied over the oneshot channel).
    Snapshot { reply: Sender<Snapshot> },
    /// Stop the engine.
    Shutdown,
}

/// Node → local application events.
#[derive(Debug)]
pub enum Event {
    /// A diagnostic line for logs / the daemon stdout.
    Log(String),
    /// A `Data` payload addressed to this node arrived.
    App { from: NodeId, payload: Vec<u8> },
    /// A locally issued `Send` was handed to the first hop.
    Sent { dst: NodeId, msg_id: u64 },
    /// A locally issued `Send` could not be routed (no fresh route).
    Unroutable { dst: NodeId, msg_id: u64, payload_len: usize },
}

// ---------------------------------------------------------------------------
// Node controller (handle to a running engine thread)
// ---------------------------------------------------------------------------

/// Handle for talking to a running node engine (running on its own thread).
///
/// Dropping a controller without calling [`NodeController::shutdown`] leaves
/// the engine running until its command channel disconnects and traffic goes
/// quiet (see `Node::run`); `shutdown` is the deterministic path.
pub struct NodeController {
    pub commands: Sender<Command>,
    /// Engine → app events. Held behind a mutex so a printer thread can share
    /// the receiver with the owner of the controller.
    pub events: Arc<Mutex<Receiver<Event>>>,
    engine: Option<std::thread::JoinHandle<()>>,
}

impl NodeController {
    /// Ask the engine for a consistent state snapshot.
    pub fn snapshot(&self) -> Snapshot {
        let (tx, rx) = channel();
        let _ = self.commands.send(Command::Snapshot { reply: tx });
        rx.recv_timeout(Duration::from_secs(5))
            .expect("engine did not answer snapshot")
    }

    /// Send an application payload toward `dst` (fire-and-forget).
    pub fn send(&self, dst: NodeId, payload: Vec<u8>) -> Result<(), String> {
        if payload.len() > MAX_PAYLOAD {
            return Err(format!(
                "payload of {} bytes exceeds MAX_PAYLOAD ({MAX_PAYLOAD})",
                payload.len()
            ));
        }
        self.commands
            .send(Command::Send { dst, payload })
            .map_err(|_| "engine stopped".to_string())
    }

    /// Adjust the simulated inbound loss of one direct peer.
    pub fn set_loss(&self, peer: SocketAddr, rate: f64) -> Result<(), String> {
        self.commands
            .send(Command::SetLoss { peer, rate })
            .map_err(|_| "engine stopped".to_string())
    }

    /// Block up to `timeout` for the next engine event.
    pub fn next_event(&self, timeout: Duration) -> Result<Event, std::sync::mpsc::RecvTimeoutError> {
        let rx = self.events.lock().unwrap();
        rx.recv_timeout(timeout)
    }

    /// Ask the engine to stop and join its thread.
    pub fn shutdown(mut self) {
        let _ = self.commands.send(Command::Shutdown);
        if let Some(h) = self.engine.take() {
            let _ = h.join();
        }
    }
}

// ---------------------------------------------------------------------------
// The engine
// ---------------------------------------------------------------------------

struct Node {
    cfg: NodeConfig,
    /// The socket as bound (authoritative when cfg.bind used port 0).
    bind: SocketAddr,
    socket: Arc<std::net::UdpSocket>,
    raw_rx: Receiver<RawDatagram>,
    cmd_rx: Receiver<Command>,
    events: Sender<Event>,
    stop: Arc<AtomicBool>,
    reader: Option<std::thread::JoinHandle<()>>,

    peers: HashSet<SocketAddr>,
    neighbors: NeighborTable,
    routes: RouteTable,
    /// Highest OGM sequence already processed per origin (dedupe gate for
    /// vector relaxation and forwarding).
    ogm_seen: HashMap<NodeId, u32>,
    /// (src, dst, msg_id) -> time first seen, breaks forwarding loops.
    data_seen: HashMap<(NodeId, NodeId, u64), Instant>,
    /// Highest `Packet.nonce` observed per sending node (replay seam).
    last_nonce: HashMap<NodeId, u64>,

    ogm_seq: u32,
    nonce: u64,
    rng: Rng,

    counters: Counters,
    routes_expired_total: u64,

    next_ping_at: Instant,
    next_ogm_at: Instant,
    next_sweep_at: Instant,
}

impl Node {
    fn log(&self, msg: String) {
        let _ = self.events.send(Event::Log(msg));
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    /// Encode and send one packet to `addr`; bumps counters.
    fn send_datagram(&mut self, addr: SocketAddr, packet: &Packet) {
        match packet.to_datagram() {
            Ok(dg) => match self.socket.send_to(&dg, addr) {
                Ok(_) => {
                    self.counters.tx_packets += 1;
                    if let Some(n) = self.neighbors.get_mut(&addr) {
                        n.tx_frames += 1;
                    }
                }
                Err(e) => self.log(format!("send to {addr} failed: {e}")),
            },
            Err(e) => self.log(format!("encode failed: {e}")),
        }
    }

    fn next_nonce(&mut self) -> u64 {
        self.nonce = self.nonce.wrapping_add(1);
        self.nonce
    }

    fn wrap(&mut self, body: Body) -> Packet {
        Packet { from: self.cfg.node_id, nonce: self.next_nonce(), body }
    }

    // -- periodic duties -----------------------------------------------------

    fn ping_neighbors(&mut self) {
        let peers: Vec<SocketAddr> = self.peers.iter().copied().collect();
        for addr in peers {
            let p = self.wrap(Body::Ping);
            self.send_datagram(addr, &p);
        }
        self.counters.pings_sent += 1;
    }

    fn send_self_ogm(&mut self) {
        self.ogm_seq = self.ogm_seq.wrapping_add(1);
        let adverts = self.routes.build_adverts(crate::types::MAX_ADVERTS);
        let peers: Vec<SocketAddr> = self.peers.iter().copied().collect();
        let sent = peers.len() as u64;
        for addr in peers {
            let body = Body::Ogm {
                origin: self.cfg.node_id,
                seq: self.ogm_seq,
                ttl: OGM_TTL,
                hops: 0,
                adverts: adverts.clone(),
            };
            let p = self.wrap(body);
            self.send_datagram(addr, &p);
        }
        self.counters.ogms_sent += sent;
    }

    fn sweep(&mut self) {
        let now = self.now();
        // 1. Forget neighbors that went silent.
        let gone = self.neighbors.flush_expired(now, self.cfg.neighbor_timeout);
        if !gone.is_empty() {
            self.log(format!(
                "neighbor(s) gone: {}",
                gone.iter().map(|(a, _)| a.to_string()).collect::<Vec<_>>().join(", ")
            ));
            // 2. Drop routes whose next hop just disappeared, and reset the
            // replay/sequence watermarks for the departed neighbor: if the
            // same node comes back (e.g. after a restart) its sequence space
            // starts over, and treating its first frames as stale would
            // blackhole it for as long as it was previously up.
            let gone_ids: Vec<NodeId> =
                gone.iter().filter_map(|(_, id)| (*id != NodeId::ZERO).then_some(*id)).collect();
            for id in gone_ids {
                let doomed: Vec<NodeId> = self
                    .routes
                    .sorted()
                    .into_iter()
                    .filter(|r| r.next_hop == id)
                    .map(|r| r.dest)
                    .collect();
                for d in doomed {
                    self.routes.remove(&d);
                }
                self.last_nonce.remove(&id);
                self.ogm_seen.remove(&id);
            }
        }
        // 3. Expire stale routes.
        self.routes_expired_total += self.routes.prune_expired(now) as u64;
        // 4. Forget stale duplicate-suppression entries.
        self.data_seen.retain(|_, seen_at| now.duration_since(*seen_at) < DATA_SEEN_TTL);
    }

    // -- ingress handling -----------------------------------------------------

    fn on_datagram(&mut self, raw: RawDatagram) {
        if !self.peers.contains(&raw.addr) {
            self.counters.rx_unknown_sender += 1;
            return;
        }
        let packet = match Packet::from_datagram(&raw.data) {
            Ok(p) => p,
            Err(e) => {
                self.counters.rx_parse_errors += 1;
                self.log(format!("parse error from {}: {e}", raw.addr));
                return;
            }
        };
        // Simulated inbound link loss (after decode, before any state update)
        // so a lossy link is observable in both directions' estimators.
        if let Some(n) = self.neighbors.get(&raw.addr) {
            if self.rng.chance(n.sim_loss) {
                self.counters.rx_sim_loss_dropped += 1;
                return;
            }
        }
        self.counters.rx_packets += 1;

        // Replay/duplicate guard: per-sender nonces must increase.
        if let Some(&last) = self.last_nonce.get(&packet.from) {
            if packet.nonce <= last {
                self.counters.rx_stale_nonce += 1;
                return;
            }
        }
        self.last_nonce.insert(packet.from, packet.nonce);

        // Record liveness; bind addr <-> node id on first sight.
        self.neighbors.note_frame(raw.addr, packet.from);

        match &packet.body {
            Body::Ping => {
                let reply = self.wrap(Body::Pong { nonce: packet.nonce });
                self.send_datagram(raw.addr, &reply);
            }
            Body::Pong { .. } => {
                // Liveness already recorded by note_frame.
            }
            Body::Ogm { origin, seq, ttl, hops, adverts } => {
                self.on_ogm(raw.addr, packet.from, *origin, *seq, *ttl, *hops, adverts);
            }
            Body::Data { src, dst, msg_id, ttl, payload } => {
                self.on_data(packet.from, *src, *dst, *msg_id, *ttl, payload);
            }
        }
    }

    /// Process one OGM. `sender` is the immediate (direct) neighbor.
    fn on_ogm(
        &mut self,
        sender_addr: SocketAddr,
        sender: NodeId,
        origin: NodeId,
        seq: u32,
        ttl: u8,
        hops: u8,
        adverts: &[Advert],
    ) {
        // Ignore our own announcements bouncing back to us.
        if origin == self.cfg.node_id {
            return;
        }

        // Dedupe gate: process each (origin, seq) at most once.
        let fresh = match self.ogm_seen.get(&origin) {
            None => true,
            Some(&last) => is_newer(seq, last),
        };
        if !fresh {
            return;
        }
        self.ogm_seen.insert(origin, seq);

        // Direct-neighbor self-OGMs feed the PDR estimator for that link.
        let lq = if origin == sender {
            let n = self.neighbors.get_mut(&sender_addr).expect("known peer");
            n.ogm_pdr.observe(seq);
            n.link_quality()
        } else {
            self.neighbors.get(&sender_addr).map(|n| n.link_quality()).unwrap_or(0.0)
        };

        let now = self.now();
        if origin == sender {
            // Direct neighbor: reachable at one link cost; its distance vector
            // relaxes our table.
            self.routes.relax_dv(origin, 0, sender, lq, now);
            for adv in adverts {
                if adv.dst == self.cfg.node_id || adv.dst == origin {
                    continue;
                }
                if adv.cost >= COST_INF {
                    continue;
                }
                self.routes.relax_dv(adv.dst, adv.cost, sender, lq, now);
            }
        } else {
            // Forwarded OGM: tentative multi-hop reachability for the origin.
            self.routes.relax_ogm(origin, sender, hops, lq, now);
        }

        // Rebroadcast to everyone except the sender, bounded by TTL, so nodes
        // beyond direct reach still learn the origin exists.
        if ttl > 0 {
            let body = Body::Ogm {
                origin,
                seq,
                ttl: ttl - 1,
                hops: hops + 1,
                // Distance vectors are only trusted from the origin itself.
                adverts: Vec::new(),
            };
            let peers: Vec<SocketAddr> =
                self.peers.iter().copied().filter(|a| *a != sender_addr).collect();
            for addr in peers {
                let p = self.wrap(body.clone());
                self.send_datagram(addr, &p);
            }
        }
    }

    fn on_data(
        &mut self,
        sender: NodeId,
        src: NodeId,
        dst: NodeId,
        msg_id: u64,
        ttl: u8,
        payload: &[u8],
    ) {
        // Loop / duplicate suppression.
        let key = (src, dst, msg_id);
        if self.data_seen.contains_key(&key) {
            self.counters.data_duplicate += 1;
            return;
        }
        self.data_seen.insert(key, self.now());

        if dst == self.cfg.node_id {
            self.counters.data_delivered += 1;
            self.emit(Event::App { from: src, payload: payload.to_vec() });
            return;
        }
        if ttl == 0 {
            self.log(format!("dropping data {msg_id}: ttl exhausted (src={})", src.short()));
            return;
        }

        let next = self.routes.next_hop(&dst, self.now());
        let Some(next) = next else {
            self.counters.data_unroutable += 1;
            self.log(format!(
                "no route to {} (data {msg_id} from {})",
                dst.short(),
                src.short()
            ));
            return;
        };
        let Some(addr) = self.neighbors.addr_of(&next) else {
            self.counters.data_unroutable += 1;
            return;
        };
        let body = Body::Data {
            src,
            dst,
            msg_id,
            ttl: ttl - 1,
            payload: payload.to_vec(),
        };
        self.counters.data_forwarded += 1;
        let _ = sender;
        let p = self.wrap(body);
        self.send_datagram(addr, &p);
    }

    // -- local sends ----------------------------------------------------------

    fn send_app(&mut self, dst: NodeId, payload: Vec<u8>) {
        let mut payload = payload;
        sanitize_payload(&mut payload);
        let msg_id = self.rng.next_u64();
        let next = self.routes.next_hop(&dst, self.now());
        let Some(next) = next else {
            self.counters.data_unroutable += 1;
            self.emit(Event::Unroutable { dst, msg_id, payload_len: payload.len() });
            return;
        };
        let Some(addr) = self.neighbors.addr_of(&next) else {
            self.counters.data_unroutable += 1;
            self.emit(Event::Unroutable { dst, msg_id, payload_len: payload.len() });
            return;
        };
        self.data_seen.insert((self.cfg.node_id, dst, msg_id), self.now());
        let body = Body::Data { src: self.cfg.node_id, dst, msg_id, ttl: DATA_TTL, payload };
        let p = self.wrap(body);
        self.send_datagram(addr, &p);
        self.emit(Event::Sent { dst, msg_id });
    }

    // -- snapshots --------------------------------------------------------------

    fn snapshot(&self) -> Snapshot {
        let now = self.now();
        let mut neighbors: Vec<NeighborInfo> = self
            .neighbors
            .iter()
            .map(|n| NeighborInfo {
                node_id: n.node_id,
                addr: n.addr,
                link_quality: n.link_quality(),
                sim_loss_in: n.sim_loss,
                rx_frames: n.rx_frames,
                tx_frames: n.tx_frames,
                last_seen_ago: now.duration_since(n.last_seen),
            })
            .collect();
        neighbors.sort_by_key(|n| n.addr);
        let routes: Vec<RouteInfo> = self
            .routes
            .sorted()
            .into_iter()
            .map(|r| RouteInfo {
                dest: r.dest,
                cost: r.cost,
                next_hop: r.next_hop,
                via_ogm_hops: r.via_ogm_hops,
                age: now.duration_since(r.last_update),
            })
            .collect();
        Snapshot {
            name: self.cfg.name.clone(),
            node_id: self.cfg.node_id,
            bind: self.bind,
            neighbors,
            routes,
            counters: self.counters,
            routes_expired_total: self.routes_expired_total,
        }
    }

    fn emit(&self, ev: Event) {
        let _ = self.events.send(ev);
    }

    /// Main event loop. Runs until a shutdown command arrives or both control
    /// channels disconnect.
    fn run(&mut self) {
        loop {
            let now = Instant::now();
            if now >= self.next_ping_at {
                self.ping_neighbors();
                self.next_ping_at = now + self.cfg.ping_interval;
            }
            if now >= self.next_ogm_at {
                self.send_self_ogm();
                self.next_ogm_at = now + self.cfg.ogm_interval;
            }
            if now >= self.next_sweep_at {
                self.sweep();
                self.next_sweep_at = now + self.cfg.ogm_interval;
            }

            let due = [self.next_ping_at, self.next_ogm_at, self.next_sweep_at]
                .into_iter()
                .min()
                .unwrap_or_else(|| now + Duration::from_secs(60));
            let wait = due.saturating_duration_since(Instant::now());

            let mut shutdown = false;
            match self.raw_rx.recv_timeout(wait) {
                Ok(raw) => self.on_datagram(raw),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => shutdown = true,
            }
            loop {
                match self.cmd_rx.try_recv() {
                    Ok(Command::Shutdown) => {
                        shutdown = true;
                        break;
                    }
                    Ok(Command::Send { dst, payload }) => self.send_app(dst, payload),
                    Ok(Command::SetLoss { peer, rate }) => {
                        let applied = match self.neighbors.get_mut(&peer) {
                            Some(n) => {
                                n.sim_loss = rate.clamp(0.0, 1.0);
                                n.sim_loss
                            }
                            None => {
                                self.log(format!("set_loss: {peer} is not a direct peer"));
                                continue;
                            }
                        };
                        self.log(format!("sim loss for {peer} set to {applied}"));
                    }
                    Ok(Command::Snapshot { reply }) => {
                        let _ = reply.send(self.snapshot());
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        shutdown = true;
                        break;
                    }
                }
            }

            if shutdown {
                break;
            }
        }
        // Tear down the reader thread.
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.reader.take() {
            let _ = h.join();
        }
        self.log("node stopped".into());
    }
}

// ---------------------------------------------------------------------------
// Public constructor
// ---------------------------------------------------------------------------

/// Start a node engine on its own thread and return a controller.
///
/// This is the seam used by the `mycelium` daemon binary (which adds
/// stdin/stdout adapters) and by the in-process simulator / tests (which talk
/// to the controller directly).
pub fn start(cfg: NodeConfig) -> Result<NodeController, String> {
    cfg.validate()?;
    let socket = Arc::new(
        std::net::UdpSocket::bind(cfg.bind).map_err(|e| format!("bind {}: {e}", cfg.bind))?,
    );
    let bind = socket.local_addr().map_err(|e| e.to_string())?;

    let peers: HashSet<SocketAddr> = cfg.peers.iter().map(|p| p.addr).collect();
    let mut rng = Rng::new(if cfg.seed != 0 {
        cfg.seed
    } else {
        Rng::from_entropy().next_u64()
    });

    let (events_tx, events_rx) = channel::<Event>();
    let (cmd_tx, cmd_rx) = channel::<Command>();
    let (raw_tx, raw_rx) = channel::<RawDatagram>();
    let stop = Arc::new(AtomicBool::new(false));
    let reader = spawn_reader(socket.clone(), stop.clone(), raw_tx);

    let mut engine = Node {
        cfg: cfg.clone(),
        bind,
        socket,
        raw_rx,
        cmd_rx,
        events: events_tx,
        stop,
        reader: Some(reader),
        peers,
        neighbors: NeighborTable::new(),
        routes: RouteTable::new(cfg.route_timeout),
        ogm_seen: HashMap::new(),
        data_seen: HashMap::new(),
        last_nonce: HashMap::new(),
        ogm_seq: rng.next_u32(),
        nonce: rng.next_u64(),
        rng,
        counters: Counters::default(),
        routes_expired_total: 0,
        next_ping_at: Instant::now() + Duration::from_millis(150),
        next_ogm_at: Instant::now() + Duration::from_millis(50),
        next_sweep_at: Instant::now() + Duration::from_millis(250),
    };
    // Rebuild the routing table with the correct (owned) timeout.
    engine.routes = RouteTable::new(engine.cfg.route_timeout);

    let handle = std::thread::Builder::new()
        .name(format!("mycelium-{}", cfg.name))
        .spawn(move || engine.run())
        .map_err(|e| e.to_string())?;

    Ok(NodeController {
        commands: cmd_tx,
        events: Arc::new(Mutex::new(events_rx)),
        engine: Some(handle),
    })
}

// ---------------------------------------------------------------------------
// Convenience helpers
// ---------------------------------------------------------------------------

/// Parse `"ip:port"` (IPv4).
pub fn parse_socket_addr(s: &str) -> Result<SocketAddr, String> {
    s.parse::<SocketAddr>().map_err(|e| format!("bad address {s:?}: {e}"))
}
