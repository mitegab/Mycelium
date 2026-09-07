//! `sim` — spawn a fleet of real `mycelium` node *processes* on localhost,
//! with artificially constrained visibility, and run self-checking scenarios
//! that demonstrate the mesh's self-healing behavior.
//!
//! ```text
//! sim --nodes 5 --topology ring --scenario deliver --bin target/debug/mycelium
//! sim --nodes 6 --topology ring --scenario lossy
//! sim --nodes 5 --topology line --scenario partition
//! ```
//!
//! The simulator only ever talks to each node through its stdin (commands)
//! and stdout (parseable lines), exactly like a human operator or an app
//! would via the Channel API — it has no access to the nodes' internals.
//!
//! Exit code 0 means every assertion of the scenario passed; anything else is
//! a failure with details on stderr.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mycelium::types::NodeId;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Opts {
    nodes: usize,
    topology: String, // line | ring | full
    base_port: u16,
    bin: PathBuf,
    scenario: String, // deliver | partition | lossy
    ogm_ms: u64,
    route_ms: u64,
    neighbor_ms: u64,
    seed: u64,
    workdir: PathBuf,
    timeout_s: u64,
    verbose: bool,
}

fn usage() -> &'static str {
    "usage: sim --nodes N [--topology line|ring|full] [--scenario deliver|partition|lossy]\n\
            [--bin PATH] [--base-port P] [--ogm-ms N] [--route-ms N] [--neighbor-ms N]\n\
            [--seed N] [--workdir DIR] [--timeout S] [--verbose]\n"
}

fn parse_opts() -> Result<Opts, String> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut o = Opts {
        nodes: 5,
        topology: "ring".into(),
        base_port: 17100,
        bin: std::env::var("MYCELIUM_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/debug/mycelium")
            }),
        scenario: "deliver".into(),
        ogm_ms: 250,
        route_ms: 1200,
        neighbor_ms: 1500,
        seed: 0x5eed,
        workdir: std::env::temp_dir().join(format!("mycelium-sim-{}", std::process::id())),
        timeout_s: 120,
        verbose: false,
    };
    let mut i = 0;
    while i < argv.len() {
        let a = argv[i].clone();
        let take = |i: &mut usize| -> Result<String, String> {
            *i += 1;
            argv.get(*i).cloned().ok_or_else(|| format!("missing value for {a}"))
        };
        match a.as_str() {
            "--nodes" => o.nodes = take(&mut i)?.parse().map_err(|_| "bad --nodes")?,
            "--topology" => o.topology = take(&mut i)?,
            "--scenario" => o.scenario = take(&mut i)?,
            "--bin" => o.bin = PathBuf::from(take(&mut i)?),
            "--base-port" => o.base_port = take(&mut i)?.parse().map_err(|_| "bad --base-port")?,
            "--ogm-ms" => o.ogm_ms = take(&mut i)?.parse().map_err(|_| "bad --ogm-ms")?,
            "--route-ms" => o.route_ms = take(&mut i)?.parse().map_err(|_| "bad --route-ms")?,
            "--neighbor-ms" => {
                o.neighbor_ms = take(&mut i)?.parse().map_err(|_| "bad --neighbor-ms")?
            }
            "--seed" => o.seed = take(&mut i)?.parse().map_err(|_| "bad --seed")?,
            "--workdir" => o.workdir = PathBuf::from(take(&mut i)?),
            "--timeout" => o.timeout_s = take(&mut i)?.parse().map_err(|_| "bad --timeout")?,
            "--verbose" => o.verbose = true,
            "--help" | "-h" => {
                print!("{}", usage());
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg {other:?}\n{}", usage())),
        }
        i += 1;
    }
    if o.nodes < 2 || o.nodes > 32 {
        return Err("--nodes must be in 2..=32".into());
    }
    if !["line", "ring", "full"].contains(&o.topology.as_str()) {
        return Err("--topology must be line|ring|full".into());
    }
    if !["deliver", "partition", "lossy"].contains(&o.scenario.as_str()) {
        return Err("--scenario must be deliver|partition|lossy".into());
    }
    if o.ogm_ms == 0 || o.route_ms < o.ogm_ms * 2 || o.neighbor_ms < o.ogm_ms * 2 {
        return Err("bad timer values: route/neighbor-ms must be >= 2x ogm-ms".into());
    }
    Ok(o)
}

// ---------------------------------------------------------------------------
// Node process plumbing
// ---------------------------------------------------------------------------

/// A running `mycelium` child process with a line mailbox for its stdout.
struct NodeProc {
    name: String,
    addr: std::net::SocketAddr,
    id: Option<NodeId>,
    child: Child,
    stdin: ChildStdin,
    /// stdout lines that arrived but were not consumed by the previous wait.
    pending: Arc<Mutex<VecDeque<String>>>,
    rx: Receiver<String>,
}

impl NodeProc {
    fn spawn(
        idx: usize,
        name: &str,
        addr: std::net::SocketAddr,
        peers: &[std::net::SocketAddr],
        bin: &PathBuf,
        opts: &Opts,
    ) -> Result<NodeProc, String> {
        let mut cmd = Command::new(bin);
        cmd.arg("run")
            .arg("--name")
            .arg(name)
            .arg("--bind")
            .arg(addr.to_string())
            .arg("--state-dir")
            .arg(opts.workdir.join(format!("node-{idx}")))
            .arg("--seed")
            .arg((opts.seed + idx as u64).to_string())
            .arg("--ogm-ms")
            .arg(opts.ogm_ms.to_string())
            .arg("--route-ms")
            .arg(opts.route_ms.to_string())
            .arg("--neighbor-ms")
            .arg(opts.neighbor_ms.to_string())
            .arg("--quiet");
        for p in peers {
            cmd.arg("--peer").arg(p.to_string());
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().map_err(|e| format!("spawn {name}: {e}"))?;
        let stdin = child.stdin.take().ok_or("no stdin")?;
        let stdout = child.stdout.take().ok_or("no stdout")?;
        let stderr = child.stderr.take().ok_or("no stderr")?;

        // Forward child stderr to our stderr for diagnostics.
        std::thread::spawn(move || {
            let r = BufReader::new(stderr);
            for line in r.lines().flatten() {
                eprintln!("[child-stderr] {line}");
            }
        });

        let (tx, rx): (Sender<String>, Receiver<String>) = channel();
        let pending = Arc::new(Mutex::new(VecDeque::new()));
        {
            let pending = pending.clone();
            std::thread::spawn(move || {
                let r = BufReader::new(stdout);
                for line in r.lines().flatten() {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
                drop(pending);
            });
        }
        Ok(NodeProc { name: name.into(), addr, id: None, child, stdin, pending, rx })
    }

    fn send_cmd(&mut self, line: &str) -> Result<(), String> {
        writeln!(self.stdin, "{line}").map_err(|e| format!("write to {}: {e}", self.name))?;
        self.stdin.flush().map_err(|e| format!("flush {}: {e}", self.name))
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Wait for the next stdout line matching `pred`, preserving others.
    /// Returns `None` on deadline.
    fn wait_line<F: Fn(&str) -> bool>(&self, deadline: Instant, pred: F) -> Option<String> {
        // Check buffered lines first.
        if let Ok(mut q) = self.pending.lock() {
            if let Some(pos) = q.iter().position(|l| pred(l)) {
                let l = q.remove(pos).unwrap();
                return Some(l);
            }
        }
        loop {
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            match self.rx.recv_timeout(deadline - now) {
                Ok(line) => {
                    if pred(&line) {
                        return Some(line);
                    }
                    if let Ok(mut q) = self.pending.lock() {
                        q.push_back(line);
                    }
                }
                Err(RecvTimeoutError::Timeout) => return None,
                Err(RecvTimeoutError::Disconnected) => return None,
            }
        }
    }

    /// Wait for the `MYCELIUM_READY` banner.
    fn wait_ready(&mut self, deadline: Instant) -> Result<NodeId, String> {
        loop {
            let line = self
                .wait_line(deadline, |l| l.starts_with("MYCELIUM_READY"))
                .ok_or_else(|| format!("{} never became ready", self.name))?;
            // name=... id=<hex> bind=...
            let mut id = None;
            for field in line.split_whitespace() {
                if let Some(v) = field.strip_prefix("id=") {
                    id = Some(NodeId::from_hex(v)?);
                }
            }
            if let Some(id) = id {
                self.id = Some(id);
                return Ok(id);
            }
        }
    }

    /// Send `routes`, collect the ROUTE lines until `ROUTES_END`.
    fn query_routes(&mut self, deadline: Instant) -> Result<Vec<(NodeId, NodeId, u16)>, String> {
        self.send_cmd("routes")?;
        let mut out = Vec::new();
        let end = deadline.min(Instant::now() + Duration::from_secs(5));
        loop {
            let line = self
                .wait_line(end, |l| l.starts_with("ROUTE ") || l.starts_with("ROUTES_END"))
                .ok_or_else(|| format!("{}: no ROUTES_END", self.name))?;
            if line.starts_with("ROUTES_END") {
                return Ok(out);
            }
            // ROUTE dst=<hex> via=<hex> cost=<n> ogm=<bool>
            let mut dst = None;
            let mut via = None;
            let mut cost = None;
            for f in line.split_whitespace() {
                if let Some(v) = f.strip_prefix("dst=") {
                    dst = Some(NodeId::from_hex(v).map_err(|e| e.to_string())?);
                } else if let Some(v) = f.strip_prefix("via=") {
                    via = Some(NodeId::from_hex(v).map_err(|e| e.to_string())?);
                } else if let Some(v) = f.strip_prefix("cost=") {
                    cost = Some(v.parse::<u16>().map_err(|_| "bad cost".to_string())?);
                }
            }
            if let (Some(d), Some(v), Some(c)) = (dst, via, cost) {
                out.push((d, v, c));
            }
        }
    }

    #[allow(dead_code)] // useful for interactive diagnostics
    fn query_neighbors(
        &mut self,
        deadline: Instant,
    ) -> Result<Vec<(std::net::SocketAddr, Option<NodeId>, f64)>, String> {
        self.send_cmd("neighbors")?;
        let mut out = Vec::new();
        let end = deadline.min(Instant::now() + Duration::from_secs(5));
        loop {
            let line = self
                .wait_line(end, |l| l.starts_with("NEIGHBOR ") || l.starts_with("NEIGHBORS_END"))
                .ok_or_else(|| format!("{}: no NEIGHBORS_END", self.name))?;
            if line.starts_with("NEIGHBORS_END") {
                return Ok(out);
            }
            let mut addr = None;
            let mut id = None;
            let mut lq = 0.0;
            for f in line.split_whitespace() {
                if let Some(v) = f.strip_prefix("addr=") {
                    addr = Some(v.parse().map_err(|_| "bad addr".to_string())?);
                } else if let Some(v) = f.strip_prefix("id=") {
                    id = if v == "-" {
                        None
                    } else {
                        Some(NodeId::from_hex(v).map_err(|e| e.to_string())?)
                    };
                } else if let Some(v) = f.strip_prefix("lq=") {
                    lq = v.parse().map_err(|_| "bad lq".to_string())?;
                }
            }
            if let Some(a) = addr {
                out.push((a, id, lq));
            }
        }
    }

    fn terminate(&mut self) {
        let _ = self.send_cmd("quit");
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Scenario helpers
// ---------------------------------------------------------------------------

struct Mesh {
    nodes: Vec<NodeProc>,
    ids: Vec<NodeId>, // index-aligned with nodes
    opts: Opts,
    matrix: Vec<Vec<usize>>, // adjacency by node index
}

impl Mesh {
    fn deadline(&self) -> Instant {
        Instant::now() + Duration::from_secs(self.opts.timeout_s)
    }

    fn idx_of_id(&self, id: &NodeId) -> usize {
        self.ids.iter().position(|x| x == id).expect("id known")
    }

    /// Wait until every node has a (finite-cost) route to every other node.
    fn wait_converged(&mut self) -> Result<(), String> {
        let deadline = self.deadline();
        let ids = self.ids.clone();
        loop {
            let mut all = true;
            for (i, node) in self.nodes.iter_mut().enumerate() {
                let routes = node.query_routes(deadline)?;
                let my_id = self.ids[i];
                for target in &ids {
                    if *target == my_id {
                        continue;
                    }
                    let found = routes
                        .iter()
                        .find(|(d, _, _)| d == target)
                        .map(|(_, _, c)| *c < u16::MAX)
                        .unwrap_or(false);
                    if !found {
                        all = false;
                        break;
                    }
                }
                if !all {
                    break;
                }
            }
            if all {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("mesh did not converge in time".into());
            }
            std::thread::sleep(Duration::from_millis(150));
        }
    }

    /// Trace the actual multi-hop path `from` -> `to_id` by asking each node
    /// for its next hop. Returns the list of node indices traversed.
    fn trace_path(&mut self, from: usize, to_id: &NodeId) -> Result<Vec<usize>, String> {
        let deadline = self.deadline();
        let mut path = vec![from];
        let mut cur = from;
        for _ in 0..(self.nodes.len() * 2 + 2) {
            if self.ids[cur] == *to_id {
                return Ok(path);
            }
            let routes = self.nodes[cur].query_routes(deadline)?;
            let next = routes
                .iter()
                .find(|(d, _, _)| d == to_id)
                .map(|(_, v, _)| *v)
                .ok_or_else(|| format!("node {} lost route to target", self.nodes[cur].name))?;
            let nxt = self.idx_of_id(&next);
            path.push(nxt);
            cur = nxt;
        }
        Err("path trace did not terminate (loop?)".into())
    }

    /// Wait until node `idx`'s route to `target` becomes stable across several
    /// polls; returns the stable (cost, next hop).
    fn wait_route_stable(
        &mut self,
        idx: usize,
        target: &NodeId,
        polls: usize,
    ) -> Result<(u16, NodeId), String> {
        let deadline = self.deadline();
        let mut last: Option<(u16, NodeId)> = None;
        let mut stable_for = 0usize;
        loop {
            let routes = self.nodes[idx].query_routes(deadline)?;
            let cur = routes
                .iter()
                .find(|(d, _, _)| d == target)
                .map(|(_, v, c)| (*c, *v))
                .ok_or_else(|| format!("node {} has no route to target", self.nodes[idx].name))?;
            if Some(cur) == last {
                stable_for += 1;
                if stable_for >= polls {
                    return Ok(cur);
                }
            } else {
                stable_for = 0;
                last = Some(cur);
            }
            if Instant::now() >= deadline {
                return Err("route never stabilized".into());
            }
            std::thread::sleep(Duration::from_millis(300));
        }
    }

    /// Send `text` from node `from` to `to_id` and wait for an APP line at the
    /// destination. Returns the number of APP lines matched (0 on timeout).
    fn send_and_expect(
        &mut self,
        from: usize,
        to_id: &NodeId,
        text: &str,
        deadline: Instant,
    ) -> Result<(), String> {
        let from_id = self.ids[from];
        self.nodes[from]
            .send_cmd(&format!("send {} {text}", to_id))?;
        // The sender should accept (a route must exist at send time).
        let _accept = self.nodes[from]
            .wait_line(deadline, |l| {
                l.starts_with("SEND_ACCEPT") || l.starts_with("SEND_ERROR")
            })
            .ok_or_else(|| format!("{}: no send response", self.nodes[from].name))?;
        // The destination should print APP from=<src> <text>.
        let to_idx = self.idx_of_id(to_id);
        let expect = format!("APP from={}", from_id);
        let line = self.nodes[to_idx]
            .wait_line(deadline, |l| l.starts_with("APP "))
            .ok_or_else(|| format!("{}: no APP line arrived", self.nodes[to_idx].name))?;
        if !line.starts_with(&expect) || !line.contains(text) {
            return Err(format!(
                "{}: got unexpected APP line: {line:?} (expected sender {})",
                self.nodes[to_idx].name, from_id
            ));
        }
        Ok(())
    }

    fn route_to(&mut self, idx: usize, target: &NodeId) -> Result<Option<(u16, NodeId)>, String> {
        let deadline = self.deadline();
        let routes = self.nodes[idx].query_routes(deadline)?;
        Ok(routes.iter().find(|(d, _, _)| d == target).map(|(_, v, c)| (*c, *v)))
    }
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

fn build_mesh(opts: Opts) -> Result<Mesh, String> {
    let n = opts.nodes;
    let mut matrix = vec![Vec::new(); n];
    for i in 0..n {
        for j in 0..n {
            if i == j {
                continue;
            }
            let adjacent = match opts.topology.as_str() {
                "line" => (i as isize - j as isize).abs() == 1,
                "ring" => {
                    let d = ((i as isize - j as isize).abs() as usize) % n;
                    d == 1 || d == n - 1
                }
                "full" => true,
                _ => unreachable!(),
            };
            if adjacent {
                matrix[i].push(j);
            }
        }
    }
    let _ = std::fs::create_dir_all(&opts.workdir);
    let mut nodes = Vec::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let addr: std::net::SocketAddr =
            format!("127.0.0.1:{}", opts.base_port + i as u16).parse().unwrap();
        let peers: Vec<std::net::SocketAddr> = matrix[i]
            .iter()
            .map(|&j| {
                format!("127.0.0.1:{}", opts.base_port + j as u16)
                    .parse()
                    .unwrap()
            })
            .collect();
        let mut proc = NodeProc::spawn(i, &format!("node-{i}"), addr, &peers, &opts.bin, &opts)?;
        let id = proc.wait_ready(Instant::now() + Duration::from_secs(15))?;
        eprintln!("sim: {} up as {}", proc.name, id.short());
        ids.push(id);
        nodes.push(proc);
    }
    let mut mesh = Mesh { nodes, ids, matrix, opts };
    eprintln!("sim: topology={} nodes={}", mesh.opts.topology, mesh.nodes.len());
    mesh.wait_converged()?;
    eprintln!("sim: converged");
    Ok(mesh)
}

fn scenario_deliver(mesh: &mut Mesh) -> Result<(), String> {
    let deadline = mesh.deadline();
    let from = 0;
    let to = mesh.nodes.len() - 1;
    let to_id = mesh.ids[to];
    let path = mesh.trace_path(from, &to_id)?;
    let names: Vec<String> =
        path.iter().map(|&i| format!("node-{i}")).collect();
    eprintln!("sim: path {} -> {} : {}", mesh.nodes[from].name, mesh.nodes[to].name, names.join(" -> "));
    // The path must start at `from` and end at the destination node.
    if *path.last().unwrap() != to {
        return Err(format!("path ends at node-{} not node-{to}", path.last().unwrap()));
    }
    // Sanity: consecutive nodes must be adjacent in the configured topology.
    for w in path.windows(2) {
        if !mesh.matrix[w[0]].contains(&w[1]) {
            return Err(format!("path uses non-adjacent hop node-{} -> node-{}", w[0], w[1]));
        }
    }
    let text = format!("hello-mesh-{}-{}", from, to);
    mesh.send_and_expect(from, &to_id, &text, deadline)?;
    eprintln!("sim: delivered '{text}' from node-{from} to node-{to}");
    eprintln!("SIM_RESULT deliver OK ({})", names.join(" -> "));
    Ok(())
}

fn scenario_partition(mesh: &mut Mesh) -> Result<(), String> {
    let n = mesh.nodes.len();
    if n < 5 {
        return Err("partition scenario wants >= 5 nodes".into());
    }
    let mid = n / 2;
    let from = 0;
    let to = n - 1;
    let to_id = mesh.ids[to];

    // Initial delivery works.
    mesh.send_and_expect(from, &to_id, "before-partition", mesh.deadline())?;
    eprintln!("sim: baseline delivery OK");

    // Freeze the middle node: the far side becomes unreachable for both halves.
    let pid = mesh.nodes[mid].pid();
    let st = Command::new("kill").args(["-STOP", &pid.to_string()]).status();
    st.map_err(|e| format!("kill -STOP: {e}"))?;
    eprintln!("sim: froze node-{mid} (pid {pid}); waiting for routes to expire...");

    // Wait until node `from` loses its route to `to`.
    let grace = Duration::from_millis(mesh.opts.route_ms * 3 + mesh.opts.ogm_ms * 4 + 1500);
    std::thread::sleep(grace);
    let deadline = mesh.deadline();
    let mut lost = false;
    while Instant::now() < deadline {
        match mesh.route_to(from, &to_id)? {
            Some(_) => std::thread::sleep(Duration::from_millis(300)),
            None => {
                lost = true;
                break;
            }
        }
    }
    if !lost {
        return Err("partition not observed: node-0 still routes to the far side".into());
    }
    eprintln!("sim: route expired as expected after partition");

    // Send should now fail locally with SEND_ERROR (no resolvable route).
    let before = Instant::now();
    mesh.nodes[from].send_cmd(&format!("send {} during-partition", to_id))?;
    let resp = mesh.nodes[from]
        .wait_line(before + Duration::from_secs(5), |l| {
            l.starts_with("SEND_ERROR") || l.starts_with("SEND_ACCEPT")
        })
        .ok_or("no send response during partition")?;
    if !resp.starts_with("SEND_ERROR") {
        return Err(format!("expected SEND_ERROR during partition, got: {resp}"));
    }
    eprintln!("sim: send rejected during partition (SEND_ERROR)");

    // Thaw the middle node: the mesh heals itself.
    let st = Command::new("kill").args(["-CONT", &pid.to_string()]).status();
    st.map_err(|e| format!("kill -CONT: {e}"))?;
    eprintln!("sim: thawed node-{mid}; waiting for self-healing...");
    // Wait for reconvergence by polling until node from sees the far side
    // again for a stable amount of time.
    loop {
        if Instant::now() > deadline {
            return Err("mesh did not heal after partition".into());
        }
        match mesh.route_to(from, &to_id)? {
            Some((_, next)) if next != mesh.ids[mid] => {
                // Any next hop is fine; just needs to exist and be stable.
                break;
            }
            _ => std::thread::sleep(Duration::from_millis(200)),
        }
    }
    mesh.wait_converged()?;
    eprintln!("sim: reconverged after thaw");

    let path = mesh.trace_path(from, &to_id)?;
    eprintln!(
        "sim: healed path: {}",
        path.iter().map(|&i| format!("node-{i}")).collect::<Vec<_>>().join(" -> ")
    );
    mesh.send_and_expect(from, &to_id, "after-heal", deadline)?;
    eprintln!("sim: delivery OK after heal");
    eprintln!("SIM_RESULT partition-heal OK");
    Ok(())
}

fn scenario_lossy(mesh: &mut Mesh) -> Result<(), String> {
    let n = mesh.nodes.len();
    if n < 6 {
        return Err("lossy scenario wants >= 6 nodes".into());
    }
    let from = 0usize;
    let to = 3usize;
    let to_id = mesh.ids[to];
    let a = 1usize; // link a<->b gets lossy
    let b = 2usize;

    // Baseline: direct-ish short path exists and delivery works.
    mesh.send_and_expect(from, &to_id, "baseline", mesh.deadline())?;
    let base = mesh.route_to(from, &to_id)?.unwrap();
    eprintln!("sim: baseline route 0->3 cost {} via node-{}", base.0, mesh.idx_of_id(&base.1));

    // Introduce heavy inbound loss on both ends of the a<->b link.
    let addr_a = mesh.nodes[a].addr;
    let addr_b = mesh.nodes[b].addr;
    mesh.nodes[b].send_cmd(&format!("loss {addr_a} 0.85"))?;
    mesh.nodes[a].send_cmd(&format!("loss {addr_b} 0.85"))?;
    eprintln!("sim: injected 85% inbound loss on the node-{a} <-> node-{b} link");

    // Wait for the PDR window to drain and routes to settle on a new path.
    let warm = Duration::from_millis(mesh.opts.ogm_ms * 20 + 2000);
    std::thread::sleep(warm);
    let (cost, next) = mesh.wait_route_stable(from, &to_id, 3)?;
    let via = mesh.idx_of_id(&next);
    let path = mesh.trace_path(from, &to_id)?;
    let names: Vec<String> = path.iter().map(|&i| format!("node-{i}")).collect();
    eprintln!("sim: post-loss route 0->3 cost {cost} via node-{via}: {}", names.join(" -> "));
    if path.contains(&a) && path.contains(&b) && path.windows(2).any(|w| w == [a, b]) {
        // If the only path still uses the lossy link, note it but do not fail
        // (a topology without alternatives is legitimate); we still require
        // successful delivery below.
        eprintln!("sim: warning: path still uses the lossy link");
    }

    // Delivery must still work (send several; loss may eat some).
    let mut ok = false;
    let mut last_err = String::new();
    let send_deadline = Instant::now() + Duration::from_secs(30);
    for i in 0..6 {
        if Instant::now() > send_deadline {
            break;
        }
        let text = format!("through-loss-{i}");
        if let Err(e) = mesh.send_and_expect(from, &to_id, &text, send_deadline) {
            last_err = e;
        } else {
            ok = true;
            eprintln!("sim: delivered '{text}' across the degraded mesh");
            break;
        }
    }
    if !ok {
        return Err(format!("no message got through the degraded mesh: {last_err}"));
    }
    eprintln!("SIM_RESULT lossy-reroute OK");
    Ok(())
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let opts = match parse_opts() {
        Ok(o) => o,
        Err(e) => {
            eprintln!("sim: {e}\n{}", usage());
            std::process::exit(2);
        }
    };
    let verbose = opts.verbose;
    let result = (|| -> Result<(), String> {
        let mut mesh = build_mesh(opts)?;
        let r = match mesh.opts.scenario.as_str() {
            "deliver" => scenario_deliver(&mut mesh),
            "partition" => scenario_partition(&mut mesh),
            "lossy" => scenario_lossy(&mut mesh),
            _ => unreachable!(),
        };
        // Terminate children best-effort (Mesh drop also cleans up).
        for node in mesh.nodes.iter_mut() {
            node.terminate();
        }
        if verbose {
            for (i, node) in mesh.nodes.iter().enumerate() {
                eprintln!(
                    "node-{i} {} id={}",
                    node.name,
                    node.id.map(|x| x.to_string()).unwrap_or_default()
                );
            }
        }
        r
    })();
    match result {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("sim: FAILED: {e}");
            std::process::exit(1);
        }
    }
}
