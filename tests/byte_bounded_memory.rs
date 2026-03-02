// Integration tests for byte-bounded memory behavior

use bytes::Bytes;
use fls::fls::byte_channel::byte_bounded_channel;
use std::time::Duration;
use tokio::time::timeout;

/// Test that backpressure kicks in with large chunks
#[tokio::test]
async fn test_backpressure_with_large_chunks() {
    let max_bytes = 128 * 1024; // 128KB limit
    let (tx, mut rx) = byte_bounded_channel::<Bytes>(max_bytes, 100);

    // Step 1: Fill buffer to capacity
    let chunk = Bytes::from(vec![1u8; 128 * 1024]); // Exactly the buffer size
    tx.send(chunk).await.unwrap();

    // Step 2: Try to send another chunk - this should block due to backpressure
    let blocking_chunk = Bytes::from(vec![2u8; 64 * 1024]);
    let send_task = tokio::spawn(async move {
        tx.send(blocking_chunk).await.unwrap();
        "completed"
    });

    // Step 3: Verify the send is blocked (structural check, not timing)
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !send_task.is_finished(),
        "Send should be blocked by backpressure"
    );

    // Step 4: Consume the buffered chunk to free space
    let _consumed = rx.recv().await.unwrap();

    // Step 5: Now the blocked send should complete
    let result = timeout(Duration::from_millis(100), send_task).await;
    assert!(result.is_ok(), "Send should unblock after freeing space");
    assert_eq!(result.unwrap().unwrap(), "completed");
}

/// Test that small chunks don't artificially limit throughput
#[tokio::test]
async fn test_small_chunks_high_throughput() {
    let max_bytes = 64 * 1024; // 64KB limit
    let (tx, mut rx) = byte_bounded_channel::<Bytes>(max_bytes, 10000);

    let start_time = std::time::Instant::now();

    // Producer: send many small chunks quickly
    let producer = tokio::spawn(async move {
        for i in 0..1000 {
            let chunk = Bytes::from(vec![i as u8; 32]); // 32-byte chunks
            tx.send(chunk).await.unwrap();
        }
    });

    // Consumer: receive all chunks
    let consumer = tokio::spawn(async move {
        let mut count = 0;
        while let Some(_chunk) = rx.recv().await {
            count += 1;
            if count >= 1000 {
                break;
            }
        }
        count
    });

    let (_, received_count) = tokio::join!(producer, consumer);
    let elapsed = start_time.elapsed();

    assert_eq!(received_count.unwrap(), 1000);
    // Should complete quickly (small chunks shouldn't be bottlenecked)
    assert!(
        elapsed < Duration::from_secs(1),
        "Small chunks took too long: {:?}",
        elapsed
    );
}

/// Test backpressure behavior with mixed chunk sizes
#[tokio::test]
async fn test_backpressure_with_mixed_sizes() {
    let max_bytes = 256 * 1024; // 256KB limit
    let (tx, mut rx) = byte_bounded_channel::<Bytes>(max_bytes, 100);

    // Fill buffer with mixed-size chunks
    tx.send(Bytes::from(vec![1u8; 128 * 1024])).await.unwrap(); // 128KB
    tx.send(Bytes::from(vec![2u8; 64 * 1024])).await.unwrap(); // 64KB
    tx.send(Bytes::from(vec![3u8; 32 * 1024])).await.unwrap(); // 32KB
                                                               // Total: 224KB (getting close to 256KB limit)

    // Try to send another 64KB chunk - this should block
    let blocking_chunk = Bytes::from(vec![4u8; 64 * 1024]); // Would exceed limit
    let send_task = tokio::spawn(async move {
        tx.send(blocking_chunk).await.unwrap();
        "completed"
    });

    // Verify backpressure is working
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !send_task.is_finished(),
        "Send should be blocked when buffer would exceed limit"
    );

    // Consume the 128KB chunk to free space
    let consumed = rx.recv().await.unwrap();
    assert_eq!(consumed.len(), 128 * 1024);

    // Now the blocked send should complete
    let result = timeout(Duration::from_millis(100), send_task).await;
    assert!(result.is_ok(), "Send should unblock after consuming data");
    assert_eq!(result.unwrap().unwrap(), "completed");
}

/// Test that oversized single chunks don't deadlock
#[tokio::test]
async fn test_oversized_chunk_no_deadlock() {
    let max_bytes = 100 * 1024; // 100KB limit
    let (tx, mut rx) = byte_bounded_channel::<Bytes>(max_bytes, 10);

    // Send a chunk larger than the buffer
    let oversized_chunk = Bytes::from(vec![42u8; 256 * 1024]); // 256KB chunk

    let producer = tokio::spawn(async move {
        tx.send(oversized_chunk.clone()).await.unwrap();
        oversized_chunk
    });

    let consumer = tokio::spawn(async move { rx.recv().await.unwrap() });

    // Should complete without deadlock
    let result = timeout(Duration::from_secs(1), async move {
        let (sent, received) = tokio::join!(producer, consumer);
        (sent.unwrap(), received.unwrap())
    })
    .await;

    assert!(result.is_ok(), "Oversized chunk should not deadlock");
    let (sent, received) = result.unwrap();
    assert_eq!(sent, received);
}

/// Test property: regardless of chunk pattern, all data flows through correctly
#[tokio::test]
async fn test_chunk_size_independence() {
    let max_bytes = 256 * 1024; // 256KB limit

    // Test different chunk patterns
    let test_cases = vec![
        ("uniform_small", vec![4096; 100]),     // 100 × 4KB
        ("uniform_large", vec![64 * 1024; 10]), // 10 × 64KB
        ("mixed", vec![1024, 32 * 1024, 1024, 128 * 1024, 1024]), // Mixed sizes
        ("single_large", vec![200 * 1024]),     // 1 × 200KB
    ];

    for (name, chunk_sizes) in test_cases {
        println!("Testing chunk pattern: {}", name);

        let (tx, mut rx) = byte_bounded_channel::<Bytes>(max_bytes, 1000);

        // Producer: send the chunk pattern
        let chunk_pattern = chunk_sizes.clone();
        let producer = tokio::spawn(async move {
            let mut total_bytes = 0;
            for (i, size) in chunk_pattern.iter().enumerate() {
                let chunk = Bytes::from(vec![i as u8; *size]);
                total_bytes += chunk.len();
                tx.send(chunk).await.unwrap();
            }
            total_bytes
        });

        // Consumer: receive all chunks
        let consumer = tokio::spawn(async move {
            let mut total_bytes = 0;
            let mut chunk_count = 0;
            while let Some(chunk) = rx.recv().await {
                total_bytes += chunk.len();
                chunk_count += 1;

                if chunk_count >= chunk_sizes.len() {
                    break;
                }
            }
            total_bytes
        });

        let (sent_bytes, received_bytes) = tokio::join!(producer, consumer);
        assert_eq!(
            sent_bytes.unwrap(),
            received_bytes.unwrap(),
            "All bytes should flow through for pattern: {}",
            name
        );
    }
}
