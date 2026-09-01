//! Raw WebSocket event observation.

use super::connection::{ConnectionDiagnostic, ConnectionGeneration};

/// Wire protocol used by an observed frame.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawFrameProtocol {
    /// Text frame bytes before parser handling.
    Text,
    /// Binary frame bytes before parser handling.
    Binary,
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
}

impl RawFrame {
    /// Create a raw frame envelope.
    #[must_use]
    pub fn new(
        generation: ConnectionGeneration,
        protocol: RawFrameProtocol,
        bytes: Vec<u8>,
    ) -> Self {
        Self {
            generation,
            protocol,
            bytes,
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
