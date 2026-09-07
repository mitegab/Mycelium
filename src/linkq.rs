//! Link-quality measurement (packet delivery ratio over a sliding window).
//!
//! Every node broadcasts one self-OGM per OGM interval with a monotonically
//! increasing sequence number. A receiver can therefore treat each direct
//! neighbor as a periodic transmitter and estimate the *inbound* packet
//! delivery ratio (PDR) of that link by counting how many of the last `W`
//! OGM slots actually arrived. This is the quantity that makes the routing
//! metric quality-aware: lossy links cost more than clean ones, and routes
//! re-converge as soon as measured quality changes (see `docs/PROTOCOL.md`).
//!
//! The estimator is deliberately independent of wall-clock timing: it counts
//! sequence slots, so it is immune to jitter and to the receiver's own timer
//! granularity.

use std::collections::VecDeque;

/// Return `true` if `candidate` is a newer sequence number than `last`,
/// handling u32 wraparound (the sequence space is far larger than any window
/// we ever inspect, so a simple half-space comparison is safe).
pub fn is_newer(candidate: u32, last: u32) -> bool {
    candidate != last && candidate.wrapping_sub(last) < (1u32 << 31)
}

/// Sliding-window estimator over a periodic sequence-numbered stream.
///
/// Implementation: the last `window` transmission slots are represented as a
/// bitset in which bit `k` records whether the slot `last_seq - k` arrived.
/// Advancing the high-water mark by a gap of `g` shifts the bitset left by
/// `g` slots (older slots fall off the window) and marks the newest slot.
/// Because all arithmetic is modular over the u32 sequence space, wraparound
/// needs no special casing.
#[derive(Debug, Clone)]
pub struct DeliveryEstimator {
    /// Window size in slots (expected transmissions). Must be <= 64.
    window: u32,
    /// Bit `k` set => slot `last - k` was received (k in 0..window).
    bits: u64,
    /// Highest sequence number observed so far.
    last: Option<u32>,
    /// Ring used only for diagnostics/debug output; not part of the math.
    _recent: VecDeque<u32>,
}

impl DeliveryEstimator {
    pub fn new(window: u32) -> DeliveryEstimator {
        assert!((1..=64).contains(&window), "window must be in 1..=64");
        DeliveryEstimator { window, bits: 0, last: None, _recent: VecDeque::new() }
    }

    /// The most recently observed sequence number, if any.
    pub fn last_seq(&self) -> Option<u32> {
        self.last
    }

    /// Should this sequence be processed as new (dedup for periodic frames)?
    pub fn is_new(&self, seq: u32) -> bool {
        match self.last {
            None => true,
            Some(last) => is_newer(seq, last),
        }
    }

    /// Record the arrival of sequence `seq` and return the updated delivery
    /// ratio. Stale/duplicate sequences are ignored.
    pub fn observe(&mut self, seq: u32) -> f64 {
        match self.last {
            None => {
                self.bits = 1;
            }
            Some(last) => {
                if !is_newer(seq, last) {
                    return self.ratio();
                }
                let gap = seq.wrapping_sub(last); // 1..=2^31-1
                if gap >= self.window {
                    // The link was silent for a whole window: reset.
                    self.bits = 1;
                } else {
                    self.bits = (self.bits << gap) | 1;
                    let mask = if self.window == 64 {
                        u64::MAX
                    } else {
                        (1u64 << self.window) - 1
                    };
                    self.bits &= mask;
                }
            }
        }
        self.last = Some(seq);
        if self._recent.len() >= self.window as usize {
            self._recent.pop_front();
        }
        self._recent.push_back(seq);
        self.ratio()
    }

    /// Current delivery ratio in `[0, 1]` (`1.0` = perfect, `0.0` = nothing
    /// delivered inside the window).
    pub fn ratio(&self) -> f64 {
        if self.last.is_none() {
            return 0.0;
        }
        self.bits.count_ones() as f64 / f64::from(self.window)
    }

    /// Reset the estimator (used when a neighbor is flushed).
    pub fn reset(&mut self) {
        self.bits = 0;
        self.last = None;
        self._recent.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perfect_link_is_1_0() {
        let mut e = DeliveryEstimator::new(10);
        for seq in 0..25 {
            assert!(e.is_new(seq));
            e.observe(seq);
        }
        assert!((e.ratio() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn dead_link_goes_to_zero() {
        let mut e = DeliveryEstimator::new(10);
        for seq in 0..10 {
            e.observe(seq);
        }
        assert!((e.ratio() - 1.0).abs() < 1e-9);
        // ... and then the transmitter goes quiet for more than a window.
        e.reset();
        assert_eq!(e.ratio(), 0.0);
    }

    #[test]
    fn half_loss_is_half() {
        let mut e = DeliveryEstimator::new(10);
        for seq in 0..30 {
            if seq % 2 == 0 {
                e.observe(seq);
            }
        }
        let r = e.ratio();
        assert!(r > 0.45 && r < 0.55, "expected ~0.5, got {r}");
    }

    #[test]
    fn stale_duplicates_are_ignored() {
        let mut e = DeliveryEstimator::new(10);
        e.observe(5);
        e.observe(5);
        e.observe(4); // older than the high-water mark
        assert_eq!(e.last_seq(), Some(5));
        assert_eq!(e.ratio(), 0.1);
    }

    #[test]
    fn wrap_around_is_handled() {
        let mut e = DeliveryEstimator::new(10);
        // Stream across the u32 boundary: u32::MAX-1, MAX, 0, 1, 2, ...
        e.observe(u32::MAX - 3);
        e.observe(u32::MAX - 2);
        assert!(e.is_new(u32::MAX - 1));
        e.observe(u32::MAX - 1);
        assert!(e.is_new(0)); // wrapped
        e.observe(0);
        assert!(e.is_new(1));
        e.observe(1);
        for seq in 2..20 {
            e.observe(seq);
        }
        assert!(!e.is_new(u32::MAX - 2)); // old again
        assert_eq!(e.last_seq(), Some(19));
        // After a healthy run the window is full again.
        assert!((e.ratio() - 1.0).abs() < 1e-9, "ratio {}", e.ratio());
    }

    #[test]
    fn long_gap_resets_window() {
        let mut e = DeliveryEstimator::new(5);
        e.observe(0);
        e.observe(1);
        e.observe(2);
        // Transmitter silent for 6 slots, then returns: old slot info gone.
        e.observe(9);
        assert!((e.ratio() - 0.2).abs() < 1e-9, "ratio {}", e.ratio());
    }

    #[test]
    fn uneven_gaps_count_only_distinct_slots() {
        let mut e = DeliveryEstimator::new(4);
        e.observe(10);
        e.observe(11); // gap 1
        e.observe(13); // gap 2: slot 12 missed
        // Received slots among {10,11,12,13}: 10,11,13 -> 3 of 4.
        let r = e.ratio();
        assert!((r - 0.75).abs() < 1e-9, "ratio {r}");
        // Slot 10 has now fallen out of the window (last=13, window=4).
        assert_eq!(e.observe(14), 0.75); // slots 11,13,14 of {11,12,13,14}
    }
}
