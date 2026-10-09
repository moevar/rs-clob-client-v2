//! Raw WebSocket event observation.

use std::time::{Instant, SystemTime};

use super::connection::{ConnectionDiagnostic, ConnectionGeneration};

/// Wire protocol used by an observed frame.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawFrameProtocol {
    /// Text frame bytes before parser handling.
    Text,
    /// Binary frame bytes before parser handling.
    Binary,
    /// Close frame payload as on the wire: the two-byte status code (big-endian) followed by the
    /// UTF-8 reason. Empty when the frame carried no payload.
    Close,
}

/// When a frame was observed: right after the socket read returned it (inbound), or right after
/// its write completed (outbound). Both clocks are read together: the monotonic one for local
/// stage times, the wall clock for comparison with venue timestamps.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameTime {
    /// Monotonic clock.
    pub monotonic: Instant,
    /// Wall clock.
    pub wall: SystemTime,
}

impl FrameTime {
    /// Read both clocks now.
    #[must_use]
    pub fn now() -> Self {
        Self {
            monotonic: Instant::now(),
            wall: SystemTime::now(),
        }
    }
}

/// Byte-exact frame observed on a concrete connection generation.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    /// Immutable connection generation associated with the frame.
    pub generation: ConnectionGeneration,
    /// Wire protocol used by the frame.
    pub protocol: RawFrameProtocol,
    /// Byte-exact payload.
    pub bytes: Vec<u8>,
    /// When the frame was observed.
    pub observed_at: FrameTime,
}

impl RawFrame {
    /// Create a raw frame envelope.
    #[must_use]
    pub fn new(
        generation: ConnectionGeneration,
        protocol: RawFrameProtocol,
        bytes: Vec<u8>,
        observed_at: FrameTime,
    ) -> Self {
        Self {
            generation,
            protocol,
            bytes,
            observed_at,
        }
    }
}

/// Raw connection event stream.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawConnectionEvent {
    /// Inbound frame captured before parser handling.
    Inbound(RawFrame),
    /// Outbound frame captured after the socket write succeeds.
    Outbound(RawFrame),
    /// Connection lifecycle, parser, or write diagnostic.
    Diagnostic(ConnectionDiagnostic),
}
