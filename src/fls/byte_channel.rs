/// Byte-bounded channel wrapper for memory-safe streaming
///
/// Wraps `mpsc::channel` with a `tokio::sync::Semaphore` to bound total
/// buffered bytes rather than item count. This prevents OOM when chunk
/// sizes vary (e.g., reqwest delivering 64-256KB chunks on fast networks).
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};

/// Trait for items that know their byte size.
pub trait SizedItem {
    fn byte_size(&self) -> usize;
}

impl SizedItem for bytes::Bytes {
    fn byte_size(&self) -> usize {
        self.len()
    }
}

/// Sender half of a byte-bounded channel.
///
/// Acquires semaphore permits equal to `chunk.byte_size()` before sending,
/// ensuring total buffered bytes never exceeds `max_bytes`.
pub struct ByteBoundedSender<T: SizedItem> {
    inner: mpsc::Sender<T>,
    semaphore: Arc<Semaphore>,
    max_bytes: usize,
}

impl<T: SizedItem> ByteBoundedSender<T> {
    /// Send an item, blocking (async) until enough byte budget is available.
    ///
    /// Acquires `min(item.byte_size(), max_bytes)` permits so a single
    /// oversized chunk can still pass through without deadlocking.
    pub async fn send(&self, item: T) -> Result<(), mpsc::error::SendError<T>> {
        let permits_needed = item.byte_size().min(self.max_bytes);

        let permits_needed_u32 = permits_needed as u32;

        // acquire_many_owned returns OwnedSemaphorePermit which we intentionally
        // forget — the receiver side adds permits back after consuming the item.
        let permit = self
            .semaphore
            .acquire_many(permits_needed_u32)
            .await
            .expect("semaphore closed unexpectedly");
        permit.forget();

        self.inner.send(item).await
    }
}

/// Receiver half of a byte-bounded channel.
///
/// Returns semaphore permits after receiving each item, freeing byte budget
/// for the sender.
pub struct ByteBoundedReceiver<T: SizedItem> {
    inner: mpsc::Receiver<T>,
    semaphore: Arc<Semaphore>,
    max_bytes: usize,
}

impl<T: SizedItem> ByteBoundedReceiver<T> {
    /// Receive an item asynchronously, releasing byte budget on success.
    pub async fn recv(&mut self) -> Option<T> {
        let item = self.inner.recv().await?;
        let to_release = item.byte_size().min(self.max_bytes);
        self.semaphore.add_permits(to_release);
        Some(item)
    }

    /// Receive an item synchronously (for use in `spawn_blocking`),
    /// releasing byte budget on success.
    pub fn blocking_recv(&mut self) -> Option<T> {
        let item = self.inner.blocking_recv()?;
        let to_release = item.byte_size().min(self.max_bytes);
        self.semaphore.add_permits(to_release);
        Some(item)
    }
}

/// Create a byte-bounded channel.
///
/// - `max_bytes`: maximum total bytes buffered at any time (must be ≤ u32::MAX)
/// - `max_items`: underlying mpsc channel item capacity (prevents unbounded item queuing)
///
/// # Panics
///
/// Panics if `max_bytes` exceeds `u32::MAX` (4,294,967,295 bytes ≈ 4GB).
/// This limit exists because the underlying semaphore uses u32 permit counts.
pub fn byte_bounded_channel<T: SizedItem>(
    max_bytes: usize,
    max_items: usize,
) -> (ByteBoundedSender<T>, ByteBoundedReceiver<T>) {
    // Guard against overflow in send() method's permits_needed as u32 cast
    if max_bytes > u32::MAX as usize {
        panic!(
            "max_bytes ({}) exceeds u32::MAX ({}). Maximum supported buffer size is ~4GB.",
            max_bytes,
            u32::MAX
        );
    }

    let (tx, rx) = mpsc::channel::<T>(max_items);
    let semaphore = Arc::new(Semaphore::new(max_bytes));

    let sender = ByteBoundedSender {
        inner: tx,
        semaphore: semaphore.clone(),
        max_bytes,
    };

    let receiver = ByteBoundedReceiver {
        inner: rx,
        semaphore,
        max_bytes,
    };

    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_basic_send_receive() {
        let (tx, mut rx) = byte_bounded_channel::<Bytes>(1024, 10);

        let data = Bytes::from_static(b"hello");
        tx.send(data.clone()).await.unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received, data);
    }

    #[tokio::test]
    async fn test_byte_limit_enforcement() {
        // 100-byte limit, 5 item capacity
        let (tx, _rx) = byte_bounded_channel::<Bytes>(100, 5);

        // Send 80 bytes (should succeed)
        let chunk1 = Bytes::from(vec![1u8; 80]);
        tx.send(chunk1).await.unwrap();

        // Send 20 bytes (should succeed, total = 100)
        let chunk2 = Bytes::from(vec![2u8; 20]);
        tx.send(chunk2).await.unwrap();

        // Try to send 1 more byte (should block)
        let chunk3 = Bytes::from(vec![3u8; 1]);
        let send_future = tx.send(chunk3);

        // Should timeout because buffer is full
        assert!(timeout(Duration::from_millis(50), send_future)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_permits_released_after_recv() {
        let (tx, mut rx) = byte_bounded_channel::<Bytes>(100, 5);

        // Fill buffer to capacity
        let chunk1 = Bytes::from(vec![1u8; 60]);
        let chunk2 = Bytes::from(vec![2u8; 40]);
        tx.send(chunk1).await.unwrap();
        tx.send(chunk2).await.unwrap();

        // Buffer should be full, next send should block
        let chunk3 = Bytes::from(vec![3u8; 1]);
        let send_future = tx.send(chunk3.clone());
        assert!(timeout(Duration::from_millis(50), send_future)
            .await
            .is_err());

        // Consume one chunk, freeing 60 bytes
        let _received = rx.recv().await.unwrap();

        // Now the blocked send should succeed
        let send_future = tx.send(chunk3);
        assert!(timeout(Duration::from_millis(50), send_future)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_oversized_chunk_handling() {
        // 50-byte limit
        let (tx, mut rx) = byte_bounded_channel::<Bytes>(50, 5);

        // Send 100-byte chunk (larger than limit)
        let big_chunk = Bytes::from(vec![1u8; 100]);

        // Should succeed (acquires min(100, 50) = 50 permits)
        tx.send(big_chunk.clone()).await.unwrap();

        // Should be able to receive it
        let received = rx.recv().await.unwrap();
        assert_eq!(received, big_chunk);
    }

    #[tokio::test]
    async fn test_multiple_chunk_sizes() {
        let (tx, mut rx) = byte_bounded_channel::<Bytes>(1000, 100);

        let chunks = vec![
            Bytes::from(vec![1u8; 100]), // Small
            Bytes::from(vec![2u8; 500]), // Medium
            Bytes::from(vec![3u8; 300]), // Large
            Bytes::from(vec![4u8; 50]),  // Tiny
        ];

        // Send all chunks
        for chunk in &chunks {
            tx.send(chunk.clone()).await.unwrap();
        }

        // Receive and verify
        for expected in &chunks {
            let received = rx.recv().await.unwrap();
            assert_eq!(received, *expected);
        }
    }

    #[tokio::test]
    async fn test_channel_closure() {
        let (tx, mut rx) = byte_bounded_channel::<Bytes>(100, 5);

        tx.send(Bytes::from_static(b"data")).await.unwrap();
        drop(tx); // Close sender

        // Should receive the sent data
        let received = rx.recv().await.unwrap();
        assert_eq!(received, Bytes::from_static(b"data"));

        // Next recv should return None (channel closed)
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_blocking_recv() {
        let (tx, mut rx) = byte_bounded_channel::<Bytes>(100, 5);

        // Test in spawn_blocking since blocking_recv is sync
        let handle = tokio::task::spawn_blocking(move || {
            // This should block until data is available
            rx.blocking_recv()
        });

        // Give it a moment to start blocking
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Send data
        tx.send(Bytes::from_static(b"test")).await.unwrap();

        // Should now unblock and return the data
        let result = handle.await.unwrap();
        assert_eq!(result.unwrap(), Bytes::from_static(b"test"));
    }

    #[test]
    #[should_panic(expected = "max_bytes (4294967296) exceeds u32::MAX")]
    fn test_max_bytes_overflow_guard() {
        // Try to create a channel with max_bytes > u32::MAX
        let oversized = (u32::MAX as usize) + 1;
        let _ = byte_bounded_channel::<Bytes>(oversized, 100);
    }
}
