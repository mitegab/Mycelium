//! The `mycelium` daemon binary: runs one mesh node as an independent process.
//!
//! ```text
//! mycelium run --name alpha --bind 127.0.0.1:17001 \
//!              --peer 127.0.0.1:17002 --peer 127.0.0.1:17003 \
//!              --state-dir .mycelium/alpha
//! ```
//!
//! Once running, the node reads commands from stdin:
//!
//! ```text
//! send <node-id-hex-prefix> <text...>   # best-effort mesh message
//! routes [prefix]                       # print routing table
//! neighbors                             # print direct neighbors + link quality
//! loss <peer-addr> <0..1>               # simulated inbound loss (testing)
//! help | quit
//! ```
//!
//! Every line the node prints to stdout is either a *log* line or a
//! *parseable* line (see [`docs/PROTOCOL.md`](../../docs/PROTOCOL.md)); the
//! simulator and tests rely on the parseable ones.

use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use mycelium::engine::{parse_socket_addr, Event, NodeConfig, PeerConfig};
use mycelium::identity::Identity;
use mycelium::types::NodeId;

const USAGE: &str = "\
mycelium — run one mesh node as an independent OS process

USAGE:
    mycelium run [OPTIONS]

OPTIONS:
    --name <name>               short human name for this node [default: node]
    --bind <ip:port>            local UDP address to bind [default: 127.0.0.1:0]
    --peer <ip:port[=loss]>     direct neighbor; may be repeated. `=loss`
                                (0..1) simulates inbound packet loss for that
                                peer (testing/simulation knob)
    --state-dir <dir>           directory holding identity.key; created on
                                first run [default: .mycelium/<name>]
    --seed <u64>                deterministic RNG seed (simulation)
    --ogm-ms <n>                OGM broadcast interval in ms [default: 1000]
    --ping-ms <n>               discovery ping interval in ms [default: 1000]
    --neighbor-ms <n>           neighbor expiry timeout in ms [default: 4000]
    --route-ms <n>              route expiry timeout in ms [default: 4000]
    --quiet                     suppress informational logs (parseable output
                                and APP messages are still printed)

COMMANDS (stdin):
    send <hex-prefix> <text...>   send a message across the mesh
    routes [hex-prefix]           print the routing table
    neighbors                     print direct neighbors and link quality
    loss <peer-addr> <0..1>       set simulated inbound loss for a peer
    help                          this help
    quit                          stop the node
";

struct Args {
    name: String,
    bind: String,
    peers: Vec<PeerConfig>,
    state_dir: PathBuf,
    seed: u64,
    ogm_ms: u64,
    ping_ms: u64,
    neighbor_ms: u64,
    route_ms: u64,
    quiet: bool,
}

fn parse_peer(spec: &str) -> Result<PeerConfig, String> {
    let (addr_s, loss_s) = match spec.split_once('=') {
        Some((a, l)) => (a, Some(l)),
        None => (spec, None),
    };
    let addr = parse_socket_addr(addr_s)?;
    let sim_loss_in = match loss_s {
        None => 0.0,
        Some(l) => l
            .parse::<f64>()
            .map_err(|_| format!("bad loss value in peer {spec:?}"))?,
    };
    if !(0.0..=1.0).contains(&sim_loss_in) {
        return Err(format!("peer loss must be in 0..1, got {sim_loss_in}"));
    }
    Ok(PeerConfig { addr, sim_loss_in })
}

fn parse_args(argv: &[String]) -> Result<Args, String> {
    // Accept an optional leading subcommand name (`mycelium run ...`).
    let argv: &[String] = match argv.first().map(|s| s.as_str()) {
        Some("run") => &argv[1..],
        Some(other) if other.starts_with('-') => argv,
        Some(other) => return Err(format!("unknown subcommand {other:?}\n{USAGE}")),
        None => return Err("missing arguments\n{USAGE}".into()),
    };
    let mut name = "node".to_string();
    let mut bind = "127.0.0.1:0".to_string();
    let mut peers: Vec<PeerConfig> = Vec::new();
    let mut state_dir: Option<PathBuf> = None;
    let mut seed = 0u64;
    let mut ogm_ms = 1000u64;
    let mut ping_ms = 1000u64;
    let mut neighbor_ms = 4000u64;
    let mut route_ms = 4000u64;
    let mut quiet = false;

    let mut i = 0;
    while i < argv.len() {
        let a = &argv[i];
        let take = |i: &mut usize| -> Result<String, String> {
            *i += 1;
            argv.get(*i).cloned().ok_or_else(|| format!("missing value for {a}"))
        };
        match a.as_str() {
            "--name" => name = take(&mut i)?,
            "--bind" => bind = take(&mut i)?,
            "--peer" => peers.push(parse_peer(&take(&mut i)?)?),
            "--state-dir" => state_dir = Some(PathBuf::from(take(&mut i)?)),
            "--seed" => {
                seed = take(&mut i)?
                    .parse()
                    .map_err(|_| "bad --seed (expect u64)".to_string())?
            }
            "--ogm-ms" => ogm_ms = take(&mut i)?.parse().map_err(|_| "bad --ogm-ms")?,
            "--ping-ms" => ping_ms = take(&mut i)?.parse().map_err(|_| "bad --ping-ms")?,
            "--neighbor-ms" => {
                neighbor_ms = take(&mut i)?.parse().map_err(|_| "bad --neighbor-ms")?
            }
            "--route-ms" => route_ms = take(&mut i)?.parse().map_err(|_| "bad --route-ms")?,
            "--quiet" => quiet = true,
            other => return Err(format!("unknown argument {other:?}\n{USAGE}")),
        }
        i += 1;
    }
    if ogm_ms == 0 || ping_ms == 0 {
        return Err("--ogm-ms/--ping-ms must be > 0".into());
    }
    let state_dir = state_dir.unwrap_or_else(|| PathBuf::from(format!(".mycelium/{name}")));
    Ok(Args {
        name,
        bind,
        peers,
        state_dir,
        seed,
        ogm_ms,
        ping_ms,
        neighbor_ms,
        route_ms,
        quiet,
    })
}

fn build_config(args: &Args, identity: &Identity) -> Result<NodeConfig, String> {
    let bind = parse_socket_addr(&args.bind)?;
    Ok(NodeConfig {
        name: args.name.clone(),
        node_id: identity.node_id,
        bind,
        peers: args.peers.clone(),
        ogm_interval: Duration::from_millis(args.ogm_ms),
        ping_interval: Duration::from_millis(args.ping_ms),
        neighbor_timeout: Duration::from_millis(args.neighbor_ms),
        route_timeout: Duration::from_millis(args.route_ms),
        seed: args.seed,
    })
}

fn identity_path(state_dir: &Path) -> PathBuf {
    state_dir.join("identity.key")
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.iter().any(|a| a == "--help" || a == "-h") || argv.is_empty() {
        print!("{USAGE}");
        std::process::exit(if argv.is_empty() { 1 } else { 0 });
    }
    match run(argv) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("mycelium: {e}");
            std::process::exit(1);
        }
    }
}

fn run(argv: Vec<String>) -> Result<(), String> {
    let args = parse_args(&argv)?;
    let id_path = identity_path(&args.state_dir);
    let identity = Identity::load_or_create(&id_path)
        .map_err(|e| format!("identity {}: {e}", id_path.display()))?;
    let cfg = build_config(&args, &identity)?;
    let controller = mycelium::start(cfg.clone()).map_err(|e| format!("start: {e}"))?;

    // Banner the simulator / outer tooling parses.
    println!(
        "MYCELIUM_READY name={} id={} bind={}",
        args.name,
        identity.node_id,
        controller.snapshot().bind
    );
    let _ = io::stdout().flush();

    // The engine's event stream feeds a printer thread; the command channel
    // drives the stdin loop below.
    let events = controller.events.clone();
    let printer_name = args.name.clone();
    let printer = std::thread::spawn(move || print_events(printer_name, events, args.quiet));

    eprintln!(
        "{} ready ({}). type 'help' for commands; 'quit' to stop.",
        args.name,
        identity.node_id.short()
    );

    let commands = controller.commands.clone();
    let res = run_stdin_commands(&args, commands, identity.node_id);
    eprintln!("{}: stdin closed, shutting down", args.name);
    controller.shutdown();
    let _ = printer.join();
    res
}

/// Small facade over the raw command sender, enough for the CLI's snapshot
/// and request/response needs without holding a full `NodeController`.
struct Ctl(std::sync::mpsc::Sender<mycelium::Command>);

impl Ctl {
    fn snapshot(&self) -> mycelium::Snapshot {
        let (tx, rx) = std::sync::mpsc::channel();
        let _ = self.0.send(mycelium::Command::Snapshot { reply: tx });
        rx.recv_timeout(Duration::from_secs(5))
            .expect("engine did not answer snapshot")
    }

    fn send_payload(&self, dst: NodeId, payload: Vec<u8>) -> Result<(), String> {
        self.0
            .send(mycelium::Command::Send { dst, payload })
            .map_err(|_| "engine stopped".to_string())
    }

    fn set_loss(&self, peer: std::net::SocketAddr, rate: f64) -> Result<(), String> {
        self.0
            .send(mycelium::Command::SetLoss { peer, rate })
            .map_err(|_| "engine stopped".to_string())
    }
}

fn run_stdin_commands(
    _args: &Args,
    commands: std::sync::mpsc::Sender<mycelium::Command>,
    _my_id: NodeId,
) -> Result<(), String> {
    let ctl = Ctl(commands);
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.splitn(3, ' ').collect();
        match parts[0] {
            "quit" | "exit" => break,
            "help" => print!("{USAGE}"),
            "neighbors" => print_neighbors(&ctl),
            "routes" => print_routes(&ctl, parts.get(1).copied().unwrap_or("")),
            "send" => {
                let (dst_p, text) = match (parts.get(1), parts.get(2)) {
                    (Some(d), Some(t)) => (d, t),
                    _ => {
                        println!("SEND_ERROR usage: send <hex-prefix> <text>");
                        continue;
                    }
                };
                let dst = resolve_dest(&ctl, dst_p)?;
                match ctl.send_payload(dst, text.as_bytes().to_vec()) {
                    Ok(()) => println!("SEND_ACCEPT dst={}", dst),
                    Err(e) => println!("SEND_ERROR {e}"),
                }
            }
            "loss" => {
                let (addr_s, rate_s) = match (parts.get(1), parts.get(2)) {
                    (Some(a), Some(r)) => (a, r),
                    _ => {
                        println!("LOSS_ERROR usage: loss <peer-ip:port> <0..1>");
                        continue;
                    }
                };
                match (parse_socket_addr(addr_s), rate_s.parse::<f64>()) {
                    (Ok(addr), Ok(rate)) => {
                        ctl.set_loss(addr, rate)?;
                        println!("LOSS_SET {addr} {rate}");
                    }
                    _ => println!("LOSS_ERROR bad arguments"),
                }
            }
            other => println!("unknown command {other:?} ('help' for usage)"),
        }
        let _ = io::stdout().flush();
    }
    Ok(())
}

fn print_events(
    name: String,
    events: std::sync::Arc<std::sync::Mutex<std::sync::mpsc::Receiver<Event>>>,
    quiet: bool,
) {
    loop {
        let msg: Option<Event> = {
            let rx = events.lock().unwrap();
            rx.recv().ok()
        };
        let ev = match msg {
            Some(ev) => ev,
            None => break, // engine exited and closed the channel
        };
        match ev {
            Event::Log(msg) => {
                if !quiet {
                    println!("[{name}] {msg}");
                }
            }
            Event::App { from, payload } => {
                let text = String::from_utf8_lossy(&payload);
                println!("APP from={} {}", from, text);
            }
            Event::Sent { dst, msg_id } => {
                println!("SENT dst={} msg={}", dst.short(), msg_id);
            }
            Event::Unroutable { dst, msg_id, payload_len } => {
                println!("UNROUTABLE dst={} msg={msg_id} len={payload_len}", dst.short());
            }
        }
        let _ = io::stdout().flush();
    }
}

/// Resolve a hex prefix against everything this node can see (routes +
/// neighbors). Prefixes may be abbreviated.
fn resolve_dest(ctl: &Ctl, prefix: &str) -> Result<NodeId, String> {
    let snap = ctl.snapshot();
    let mut candidates: Vec<NodeId> = snap
        .routes
        .iter()
        .map(|r| r.dest)
        .chain(snap.neighbors.iter().filter_map(|n| n.node_id))
        .collect();
    candidates.sort();
    candidates.dedup();
    NodeId::resolve_prefix(prefix, &candidates)
        .map_err(|e| format!("cannot resolve destination: {e}"))
}

fn print_routes(ctl: &Ctl, prefix: &str) {
    let snap = ctl.snapshot();
    let count = snap.routes.len();
    for r in &snap.routes {
        if r.dest.to_hex().starts_with(prefix) {
            println!(
                "ROUTE dst={} via={} cost={} ogm={}",
                r.dest,
                r.next_hop,
                r.cost,
                r.via_ogm_hops.is_some()
            );
        }
    }
    println!("ROUTES_END count={count}");
}

fn print_neighbors(ctl: &Ctl) {
    let snap = ctl.snapshot();
    let count = snap.neighbors.len();
    for n in &snap.neighbors {
        let id = n.node_id.map(|i| i.to_string()).unwrap_or_else(|| "-".into());
        println!(
            "NEIGHBOR addr={} id={} lq={:.3} loss={:.3}",
            n.addr, id, n.link_quality, n.sim_loss_in
        );
    }
    println!("NEIGHBORS_END count={count}");
}
