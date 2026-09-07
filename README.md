# Mycelium

## Resilient, Authenticated, Off-Grid Mesh Networking Engine

`mycelium` is a peer-to-peer, self-healing mesh networking daemon written in Rust. Inspired by protocol designs from [B.A.T.M.A.N.](https://www.open-mesh.org/projects/open-mesh/wiki) and [Reticulum](https://reticulum.network/), Mycelium is designed to provide authenticated, end-to-end encrypted, multi-hop communication across contested, bandwidth-constrained, or infrastructure-denied environments.

Like its biological namesake, a Mycelium network has no central brain: every node acts as an autonomous relay. If nodes drop, links are jammed, or physical topologies shift, the network can dynamically route traffic around partitions without human intervention.

> Mycelium is intended for networks that must continue working when the usual network assumptions do not.

## Design goals

- **Decentralized:** No central controller, broker, or coordination service is required for the mesh to operate.
- **Self-healing:** Nodes adapt as peers become unavailable and links appear, disappear, or degrade.
- **Authenticated:** Nodes establish trust using cryptographic identities rather than network location alone.
- **End-to-end encrypted:** Relays forward traffic without needing access to the contents of messages.
- **Multi-hop:** Data can cross a changing sequence of intermediate nodes to reach its destination.
- **Transport-agnostic:** The design should work across a mix of available links, including low-bandwidth or intermittently connected transports.
- **Resource-conscious:** Protocol overhead should remain practical for constrained devices and unreliable links.

## Conceptual architecture

A Mycelium node is both an endpoint and a relay:

1. A node uses a cryptographic identity to represent itself on the network.
2. It discovers and maintains relationships with reachable peers.
3. Authenticated sessions protect traffic between endpoints and neighboring nodes.
4. The routing layer selects viable paths through the current topology.
5. Intermediate nodes relay opaque packets and can forward traffic for peers they do not directly reach.
6. When the topology changes, nodes recalculate paths and continue delivery where an alternate route exists.

This separation keeps routing resilient while ensuring that forwarding a packet does not require a relay to be trusted with the application payload.

## Where Mycelium fits

Mycelium is aimed at deployments such as:

- field and expedition networks;
- disaster-response and emergency communications;
- temporary or rapidly changing infrastructure;
- rural and remote-area connectivity;
- networks operating under censorship, interference, or partial outages; and
- embedded systems that need local communication without dependable internet access.

Mycelium is not intended to replace a conventional low-latency LAN or a centrally managed cloud network where those options are available. Its focus is graceful degradation: preserving useful connectivity when infrastructure is limited or unreliable.

## Project status

Mycelium is in early development. The protocol, wire format, public APIs, and transport integrations are expected to evolve as the implementation and threat model mature. Until those interfaces are documented and stabilized, deployments should be treated as experimental.

## Roadmap

The project is expected to grow in the following areas:

- a small, portable Rust daemon and configuration model;
- cryptographic identity and authenticated session establishment;
- resilient peer discovery and topology-aware routing;
- support for multiple interchangeable transports;
- explicit handling of intermittent links, congestion, and partitions;
- fault-injection, interoperability, and property-based testing; and
- operational documentation for deploying and monitoring a mesh.

## Contributing

Contributions are welcome as the implementation takes shape. For substantial changes, open an issue first to discuss the protocol or API impact. In particular, changes to cryptographic flows, packet formats, routing behavior, or trust assumptions should include tests and a short explanation of the security and compatibility considerations.

## Name

Mycelium is named after the underground network of fungal threads that connects and sustains a forest. The name reflects the project's goal: many independent nodes cooperating to keep a larger network alive without a single point of control.
