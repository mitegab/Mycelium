//! Underlay transport: a raw UDP datagram socket with a background reader.
//!
//! This is the *only* module that touches a concrete transport today. The
//! protocol logic in [`crate::engine`] never sees sockets, only decoded
//! frames, so a later phase can swap in `tokio::net::UdpSocket` tasks, a BLE
//! peripheral, a LoRa serial device or raw Ethernet sockets behind the same
//! seam without touching the mesh engine.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::types::MAX_DATAGRAM;

/// One raw datagram as delivered by the reader thread.
#[derive(Debug)]
pub struct RawDatagram {
    pub addr: SocketAddr,
    pub data: Vec<u8>,
}

/// Spawn a background reader that forwards every received datagram on `tx`.
///
/// The socket is set non-blocking and polled in 10 ms slices so the thread
/// can observe `stop` promptly; this is the std-only stand-in for an async
/// `recv` loop (tokio adapter later).
pub fn spawn_reader(
    socket: Arc<std::net::UdpSocket>,
    stop: Arc<AtomicBool>,
    tx: Sender<RawDatagram>,
) -> JoinHandle<()> {
    let _ = socket.set_nonblocking(true);
    thread::Builder::new()
        .name("mycelium-reader".into())
        .spawn(move || {
            let mut buf = vec![0u8; MAX_DATAGRAM];
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                match socket.recv_from(&mut buf) {
                    Ok((n, addr)) => {
                        let data = buf[..n].to_vec();
                        if tx.send(RawDatagram { addr, data }).is_err() {
                            break; // engine gone
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(_) => {
                        // Transient/ignorable error: keep reading (a dead
                        // socket is caught by the stop flag at shutdown).
                        thread::sleep(std::time::Duration::from_millis(10));
                    }
                }
            }
        })
        .expect("spawn reader thread")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_forwards_datagrams() {
        let a = Arc::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
        let b = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let a_addr = a.local_addr().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn_reader(a, stop.clone(), tx);

        b.send_to(b"hello", a_addr).unwrap();
        let raw = rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        assert_eq!(raw.data, b"hello");
        assert_eq!(raw.addr, b.local_addr().unwrap());

        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
