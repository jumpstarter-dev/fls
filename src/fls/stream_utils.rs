/// Stream utilities for async/sync bridging and download handling
///
/// Provides reusable components for streaming data between async HTTP
/// downloads and sync processing (like tar extraction or decompression).
use bytes::Bytes;
use std::io::Read;
use tokio::sync::mpsc;

use crate::fls::byte_channel::ByteBoundedReceiver;

/// Abstraction over plain and byte-bounded receivers
enum ReceiverVariant {
    Plain(mpsc::Receiver<Bytes>),
    ByteBounded(ByteBoundedReceiver<Bytes>),
}

impl ReceiverVariant {
    fn blocking_recv(&mut self) -> Option<Bytes> {
        match self {
            ReceiverVariant::Plain(rx) => rx.blocking_recv(),
            ReceiverVariant::ByteBounded(rx) => rx.blocking_recv(),
        }
    }
}

/// Reader that pulls bytes from a tokio mpsc channel
///
/// This bridges async HTTP streaming with synchronous readers
/// like tar::Archive or flate2::GzDecoder.
pub struct ChannelReader {
    rx: ReceiverVariant,
    current: Option<Bytes>,
    offset: usize,
}

impl ChannelReader {
    /// Create a new ChannelReader from a plain mpsc receiver
    pub fn new(rx: mpsc::Receiver<Bytes>) -> Self {
        Self {
            rx: ReceiverVariant::Plain(rx),
            current: None,
            offset: 0,
        }
    }

    /// Create a new ChannelReader from a byte-bounded receiver
    pub fn new_byte_bounded(rx: ByteBoundedReceiver<Bytes>) -> Self {
        Self {
            rx: ReceiverVariant::ByteBounded(rx),
            current: None,
            offset: 0,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            // If we have current data, use it
            if let Some(ref data) = self.current {
                let remaining = &data[self.offset..];
                if !remaining.is_empty() {
                    let to_copy = remaining.len().min(buf.len());
                    buf[..to_copy].copy_from_slice(&remaining[..to_copy]);
                    self.offset += to_copy;
                    return Ok(to_copy);
                }
            }

            // Need more data - blocking receive
            match self.rx.blocking_recv() {
                Some(data) => {
                    self.current = Some(data);
                    self.offset = 0;
                }
                None => {
                    // Channel closed - EOF
                    return Ok(0);
                }
            }
        }
    }
}
