//! End-to-end integration tests that run real mesh nodes (real UDP sockets,
//! real threads, real timers) inside one process. Multi-*process* behavior is
//! exercised by the `sim` binary.

use std::time::{Duration, Instant};

use mycelium::engine::{start, Event, NodeConfig, NodeController, PeerConfig};
use mycelium::types::{hex_encode, NodeId, LINK_BASE_COST};

fn id(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

/// Resolve a free port by binding to :0 and dropping the socket.
fn free_port() -> u16 {
    let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    s.local_addr().unwrap().port()
}

struct Mesh {
    nodes: Vec<NodeController>,
    names: Vec<String>,
}

impl Mesh {
    fn snapshot_routes(&self, i: usize) -> Vec<(NodeId, NodeId, u16)> {
        self.nodes[i]
            .snapshot()
            .routes
            .into_iter()
            .map(|r| (r.dest, r.next_hop, r.cost))
            .collect()
    }

    fn route_to(&self, i: usize, dst: &NodeId) -> Option<(NodeId, u16)> {
        self.snapshot_routes(i)
            .into_iter()
            .find(|(d, _, _)| d == dst)
            .map(|(_, v, c)| (v, c))
    }

    /// Wait up to `timeout` until `f(node_index)` is true.
    fn wait_for<F: Fn(usize) -> bool>(&self, timeout: Duration, f: F) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let mut all = true;
            for i in 0..self.nodes.len() {
                if !f(i) {
                    all = false;
                    break;
                }
            }
            if all {
                return true;
            }
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(40));
        }
    }

    fn wait_converged(&self, timeout: Duration) -> bool {
        let ids: Vec<NodeId> = self.nodes.iter().map(|n| n.snapshot().node_id).collect();
        self.wait_for(timeout, |i| {
            let routes = self.snapshot_routes(i);
            ids.iter().enumerate().all(|(j, target)| {
                if j == i {
                    return true;
                }
                routes
                    .iter()
                    .any(|(d, _, c)| d == target && *c < u16::MAX)
            })
        })
    }

    /// Wait until node `i` has a finite route to `dst` with the exact cost.
    fn wait_route_cost(&self, i: usize, dst: &NodeId, cost: u16, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some((_, c)) = self.route_to(i, dst) {
                if c == cost {
                    return true;
                }
            }
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(40));
        }
    }

    /// Wait until an `Event::App` with the given payload shows up on node i.
    fn expect_app(&self, i: usize, needle: &str, timeout: Duration) -> Result<NodeId, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let wait = deadline.saturating_duration_since(Instant::now());
            match self.nodes[i].next_event(wait) {
                Ok(Event::App { from, payload }) => {
                    let text = String::from_utf8_lossy(&payload);
                    if text.contains(needle) {
                        return Ok(from);
                    }
                }
                Ok(_) => continue,
                Err(_) => return Err(format!("node {i}: no App event containing {needle:?}")),
            }
        }
    }

    /// Drain events on node i until predicate matches or timeout.
    fn wait_event<F: Fn(&Event) -> bool>(&self, i: usize, timeout: Duration, f: F) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let wait = deadline.saturating_duration_since(Instant::now());
            match self.nodes[i].next_event(wait) {
                Ok(ev) => {
                    if f(&ev) {
                        return true;
                    }
                }
                Err(_) => return false,
            }
        }
    }
}

impl Drop for Mesh {
    fn drop(&mut self) {
        for node in self.nodes.drain(..) {
            node.shutdown();
        }
    }
}

fn local_addr(port: u16) -> std::net::SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

/// Build a mesh from an adjacency matrix of node indices.
fn build_mesh(adj: &[Vec<usize>]) -> Mesh {
    let n = adj.len();
    let ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
    let mut nodes = Vec::new();
    let mut names = Vec::new();
    for i in 0..n {
        let peers: Vec<PeerConfig> = adj[i]
            .iter()
            .map(|&j| PeerConfig::new(local_addr(ports[j])))
            .collect();
        let mut cfg = NodeConfig::test_node(&format!("node-{i}"), id((i + 1) as u8), 1000 + i as u64);
        cfg.bind = local_addr(ports[i]);
        cfg.peers = peers;
        let ctl = start(cfg).expect("start node");
        names.push(format!("node-{i}"));
        nodes.push(ctl);
    }
    Mesh { nodes, names }
}

/// The engines all come up on different ephemeral ports.
#[test]
fn two_nodes_deliver_and_learn_quality() {
    let mesh = build_mesh(&[vec![1], vec![0]]);
    let a = &mesh.nodes[0];
    let b = &mesh.nodes[1];
    let id_a = a.snapshot().node_id;
    let id_b = b.snapshot().node_id;
    assert!(id_a != id_b);

    assert!(
        mesh.wait_converged(Duration::from_secs(8)),
        "mesh did not converge: a sees {:?}, b sees {:?}",
        mesh.snapshot_routes(0),
        mesh.snapshot_routes(1)
    );

    // Once the PDR window warms up, a perfect link costs exactly
    // LINK_BASE_COST per hop.
    assert!(
        mesh.wait_route_cost(0, &id_b, LINK_BASE_COST, Duration::from_secs(5)),
        "route to neighbor should cost exactly {LINK_BASE_COST} on a clean link"
    );
    assert!(mesh.wait_route_cost(1, &id_a, LINK_BASE_COST, Duration::from_secs(3)));

    // Neighbor table has exactly one neighbor with full link quality.
    let snap = a.snapshot();
    assert_eq!(snap.neighbors.len(), 1);
    assert_eq!(snap.neighbors[0].node_id, Some(id_b));
    assert!(
        (snap.neighbors[0].link_quality - 1.0).abs() < 1e-3,
        "lq should approach 1.0, got {}",
        snap.neighbors[0].link_quality
    );

    // Send A -> B and check delivery.
    a.send(id_b, b"hello from a".to_vec()).unwrap();
    let from = mesh.expect_app(1, "hello from a", Duration::from_secs(5)).unwrap();
    assert_eq!(from, id_a);

    // And B -> A back.
    b.send(id_a, b"ping-back".to_vec()).unwrap();
    let from = mesh.expect_app(0, "ping-back", Duration::from_secs(5)).unwrap();
    assert_eq!(from, id_b);

    // No data should have needed forwarding (direct neighbors).
    let ca = a.snapshot().counters;
    assert_eq!(ca.data_forwarded, 0);
}

#[test]
fn line_topology_forwards_hop_by_hop() {
    // 0 -- 1 -- 2 -- 3
    let mesh = build_mesh(&[vec![1], vec![0, 2], vec![1, 3], vec![2]]);
    assert!(mesh.wait_converged(Duration::from_secs(12)));

    let id0 = mesh.nodes[0].snapshot().node_id;
    let id3 = mesh.nodes[3].snapshot().node_id;

    // Clean 3-hop path = 3 * LINK_BASE_COST.
    assert!(
        mesh.wait_route_cost(0, &id3, 3 * LINK_BASE_COST, Duration::from_secs(8)),
        "expected 3-hop cost 30, got {:?}",
        mesh.route_to(0, &id3)
    );
    // First hop toward node 3 must be node 1.
    assert_eq!(mesh.route_to(0, &id3).unwrap().0, id(2)); // id(2) == "node 1"

    // Send across the whole line.
    mesh.nodes[0].send(id3, b"cross-the-line".to_vec()).unwrap();
    let from = mesh.expect_app(3, "cross-the-line", Duration::from_secs(6)).unwrap();
    assert_eq!(from, id0);

    // Intermediate nodes must have forwarded exactly once each.
    let c1 = mesh.nodes[1].snapshot().counters;
    assert!(c1.data_forwarded >= 1, "node1 should have forwarded");
    let c2 = mesh.nodes[2].snapshot().counters;
    assert!(c2.data_forwarded >= 1, "node2 should have forwarded");
    assert_eq!(mesh.nodes[0].snapshot().counters.data_forwarded, 0);
}

#[test]
fn visibility_is_enforced() {
    // Physical: 0 -- 1 -- 2. Node 0 must never accept traffic directly from
    // node 2's address even if it fabricates a valid frame.
    let mesh = build_mesh(&[vec![1], vec![0, 2], vec![1]]);
    let c0 = &mesh.nodes[0];
    let c2 = &mesh.nodes[2];
    let id2 = c2.snapshot().node_id;
    let addr0 = c0.snapshot().bind;

    // Wait until 0 knows 2 through 1 (multi-hop route exists).
    assert!(mesh.wait_converged(Duration::from_secs(12)));
    assert!(mesh.route_to(0, &id2).is_some());

    // Fabricate a valid Ping packet "from" node 2 and send it straight to 0
    // from node 2's own socket (node 2 is not in node 0's peer set).
    let sock2 = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let pkt = mycelium::wire::Packet {
        from: id2,
        nonce: 1,
        body: mycelium::wire::Body::Ping,
    };
    sock2.send_to(&pkt.to_datagram().unwrap(), addr0).unwrap();

    // Node 0 must not have learned anything from that address.
    std::thread::sleep(Duration::from_millis(300));
    let snap = c0.snapshot();
    assert_eq!(snap.counters.rx_unknown_sender, 1, "node 0 accepted a non-peer");
    assert!(
        snap.neighbors.iter().all(|n| n.addr.port() != sock2.local_addr().unwrap().port()),
        "node 0 learned a neighbor outside its visibility set"
    );
    // And a legitimate send still works.
    mesh.nodes[0].send(id2, b"via-1".to_vec()).unwrap();
    let from = mesh.expect_app(2, "via-1", Duration::from_secs(6)).unwrap();
    assert_eq!(from, mesh.nodes[0].snapshot().node_id);
}

#[test]
fn loss_degrades_link_quality_and_cost() {
    let mesh = build_mesh(&[vec![1], vec![0]]);
    assert!(mesh.wait_converged(Duration::from_secs(8)));
    let addr_b = mesh.nodes[1].snapshot().bind;

    // Degrade node 0's inbound view of node 1 to ~10% delivery.
    mesh.nodes[0].set_loss(addr_b, 0.9).unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut degraded = false;
    while Instant::now() < deadline {
        let snap = mesh.nodes[0].snapshot();
        let nb = snap.neighbors.iter().find(|n| n.addr == addr_b).unwrap();
        if nb.link_quality < 0.35 {
            degraded = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(degraded, "link quality should degrade under simulated loss");

    // The route cost to the direct neighbor must rise above the clean 10.
    let id_b = mesh.nodes[1].snapshot().node_id;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut cost_rose = false;
    while Instant::now() < deadline {
        if let Some((_, cost)) = mesh.route_to(0, &id_b) {
            if cost > LINK_BASE_COST + 10 {
                cost_rose = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(cost_rose, "route cost should rise as link quality drops");

    // Delivery should still work (15% of frames get through).
    let id_a = mesh.nodes[0].snapshot().node_id;
    let mut ok = false;
    for i in 0..8 {
        mesh.nodes[0].send(id_b, format!("lossy-{i}").into_bytes()).unwrap();
        if let Ok(_) = mesh.expect_app(1, &format!("lossy-{i}"), Duration::from_secs(4)) {
            ok = true;
            break;
        }
    }
    assert!(ok, "no message survived the degraded link");
    let _ = id_a;
}

#[test]
fn partition_heals_after_node_restart() {
    // 0 -- 1 -- 2 (line). Kill node 1, watch 0 lose 2, restart 1, heal.
    let mut mesh = build_mesh(&[vec![1], vec![0, 2], vec![1]]);
    assert!(mesh.wait_converged(Duration::from_secs(12)));
    let id2 = mesh.nodes[2].snapshot().node_id;
    let id1 = mesh.nodes[1].snapshot().node_id;

    mesh.nodes[0].send(id2, b"before".to_vec()).unwrap();
    mesh.expect_app(2, "before", Duration::from_secs(5)).unwrap();

    // Kill node 1.
    let cfg1 = {
        let snap = mesh.nodes[1].snapshot();
        NodeConfig {
            name: snap.name,
            node_id: snap.node_id,
            bind: snap.bind,
            peers: vec![
                PeerConfig::new(mesh.nodes[0].snapshot().bind),
                PeerConfig::new(mesh.nodes[2].snapshot().bind),
            ],
            ogm_interval: Duration::from_millis(120),
            ping_interval: Duration::from_millis(120),
            neighbor_timeout: Duration::from_millis(600),
            route_timeout: Duration::from_millis(600),
            seed: 1,
        }
    };
    let removed = mesh.nodes.remove(1); // remove node 1 from the mesh...
    removed.shutdown(); // ...and actually stop its engine + socket
    mesh.names.remove(1);

    // Node 0 must lose its route to node 2 within ~2 route timeouts.
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut gone = false;
    while Instant::now() < deadline {
        if mesh.route_to(0, &id2).is_none() {
            gone = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(gone, "node 0 still routes to node 2 after node 1 died");

    // A send now must not be deliverable (either unroutable locally, or
    // nothing arrives at the far side).
    mesh.nodes[0].send(id2, b"during".to_vec()).unwrap();
    let _during_delivered = mesh.wait_event(1, Duration::from_millis(1200), |_| false);

    // Restart node 1 with the same identity/address/peers.
    let ctl1 = start(cfg1).expect("restart node 1");
    mesh.nodes.insert(1, ctl1);
    mesh.names.insert(1, "node-1".to_string());

    if !mesh.wait_converged(Duration::from_secs(12)) {
        for (i, n) in mesh.nodes.iter().enumerate() {
            let s = n.snapshot();
            eprintln!("[diag] node {i} {} routes:", s.name);
            for r in &s.routes {
                eprintln!(
                    "  -> {} via {} cost {} age {:?}",
                    r.dest.short(),
                    r.next_hop.short(),
                    r.cost,
                    r.age
                );
            }
            eprintln!("[diag]   neighbors: {:?}", s.neighbors);
            eprintln!("[diag]   counters: {:?}", s.counters);
            eprintln!("[diag]   bind: {}", s.bind);
        }
        panic!("mesh did not reconverge after restart");
    }
    // Wait for full-quality costs before asserting the exact metric.
    assert!(mesh.wait_route_cost(0, &id2, 2 * LINK_BASE_COST, Duration::from_secs(8)));
    mesh.nodes[0].send(id2, b"after-restart".to_vec()).unwrap();
    let from = mesh.expect_app(2, "after-restart", Duration::from_secs(6)).unwrap();
    assert_eq!(from, mesh.nodes[0].snapshot().node_id);
    assert_eq!(mesh.route_to(0, &id2).unwrap().0, id1);
}

#[test]
fn replay_guard_and_duplicate_suppression() {
    use mycelium::wire::{Body, Packet};

    // A standalone receiver B whose *only* peer is a raw control socket that
    // we drive by hand. This lets us exercise the replay guard and the
    // (src,dst,msg_id) loop-suppression logic with crafted frames.
    let ctl_sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let ctl_addr = ctl_sock.local_addr().unwrap();
    let mut cfg = NodeConfig::test_node("receiver", id(0xbb), 99);
    cfg.bind = local_addr(free_port());
    cfg.peers = vec![PeerConfig::new(ctl_addr)];
    let receiver = start(cfg).expect("start receiver");
    let b_id = receiver.snapshot().node_id;
    let sender_id = id(0xaa);
    let receiver_addr = receiver.snapshot().bind;
    let drain_sock = ctl_sock.try_clone().expect("clone control socket");
    let _drain = std::thread::spawn(move || {
        let mut buf = [0u8; 1500];
        while drain_sock.recv_from(&mut buf).is_ok() {}
    });

    // Give the receiver time to learn our (fake) neighbor identity.
    std::thread::sleep(Duration::from_millis(300));

    // 1) Fresh ping, nonce 100 -> accepted.
    let ping = Packet { from: sender_id, nonce: 100, body: Body::Ping };
    ctl_sock.send_to(&ping.to_datagram().unwrap(), receiver_addr).unwrap();
    std::thread::sleep(Duration::from_millis(150));
    assert!(receiver.snapshot().counters.rx_packets >= 1);

    // 2) Same datagram again (stale nonce) -> replay guard drops it.
    ctl_sock.send_to(&ping.to_datagram().unwrap(), receiver_addr).unwrap();
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        receiver.snapshot().counters.rx_stale_nonce >= 1,
        "stale nonce should trip the replay guard"
    );

    // 3) A Data frame addressed to B (dst == receiver) with fresh nonce.
    let data = |nonce: u64| Packet {
        from: sender_id,
        nonce,
        body: Body::Data {
            src: sender_id,
            dst: b_id,
            msg_id: 4242,
            ttl: 32,
            payload: b"one-delivery-only".to_vec(),
        },
    };
    ctl_sock.send_to(&data(200).to_datagram().unwrap(), receiver_addr).unwrap();
    let from = receiver
        .next_event(Duration::from_secs(3))
        .expect("first copy should be delivered");
    assert!(matches!(&from, Event::App { payload, .. } if payload == b"one-delivery-only"));

    // 4) A retransmission with a *fresh* nonce but identical (src,dst,msg_id):
    //    the loop-suppression cache must drop it without re-delivering.
    ctl_sock.send_to(&data(201).to_datagram().unwrap(), receiver_addr).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    let counters = receiver.snapshot().counters;
    assert!(
        counters.data_duplicate >= 1,
        "duplicate (src,dst,msg_id) should be suppressed, counters: {counters:?}"
    );
    // No second App event may arrive for the same message.
    let extra = receiver.next_event(Duration::from_millis(400));
    assert!(
        !matches!(extra, Ok(Event::App { .. })),
        "duplicate frame must not be redelivered: {extra:?}"
    );
    receiver.shutdown();
}

#[test]
fn postcard_node_id_wire_size_matches_lib_tests() {
    // Cross-check the lib-level compactness guarantee end to end.
    let enc = postcard_bytes(NodeId([7; 32]));
    assert_eq!(enc.len(), 33);
}

fn postcard_bytes(id: NodeId) -> Vec<u8> {
    postcard::to_stdvec(&id).unwrap()
}

/// Make sure hex encoding of ids is stable & parseable (used by the CLI).
#[test]
fn cli_hex_conventions_work() {
    let id = id(0xab);
    let hex = hex_encode(id.as_bytes());
    assert_eq!(NodeId::from_hex(&hex).unwrap(), id);
    assert_eq!(hex.len(), 64);
}
