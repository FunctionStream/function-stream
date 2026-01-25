// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{Context, Result};
use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;
use tracing::{error, info, warn};
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct AppConfig {
    /// Kafka Broker List (e.g., "127.0.0.1:9092")
    #[arg(long, default_value = "127.0.0.1:9092", env = "KAFKA_BROKERS")]
    brokers: String,

    /// Input Topic (Producer Target)
    #[arg(long, default_value = "input-topic", env = "INPUT_TOPIC")]
    input_topic: String,

    /// Output Topic (Consumer Source)
    #[arg(long, default_value = "output-topic", env = "OUTPUT_TOPIC")]
    output_topic: String,

    /// Consumer Group ID
    #[arg(long, default_value = "industrial-verifier-v1", env = "GROUP_ID")]
    group_id: String,

    /// Total number of messages to produce for the test
    #[arg(long, default_value_t = 10000)]
    msg_count: usize,
}

/// Represents the expected JSON structure from the processor.
/// Example: {"total_processed": 100, "counter_map": {"apple": 50, "banana": 50}}
#[derive(Debug, Deserialize)]
struct ProcessingResult {
    total_processed: u64,
    counter_map: HashMap<String, u64>,
}

impl ProcessingResult {
    /// Business Logic Validation:
    /// Checks if the sum of all counters equals the declared total.
    fn validate_consistency(&self) -> bool {
        let calculated_sum: u64 = self.counter_map.values().sum();
        calculated_sum == self.total_processed
    }
}

// ============================================================================
// 2. Main Entry Point
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize structured logging (Info level by default)
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    // Parse configuration
    let config = AppConfig::parse();
    info!("Starting Kafka Driver with config: {:?}", config);

    // Create a notification channel for graceful shutdown
    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_consumer = shutdown_notify.clone();

    // Spawn Producer and Consumer as concurrent async tasks
    let producer_handle = tokio::spawn(run_producer(config.clone()));
    let consumer_handle = tokio::spawn(run_consumer(config.clone(), shutdown_consumer));

    // Wait for system signals (Ctrl+C / SIGTERM)
    match signal::ctrl_c().await {
        Ok(()) => {
            info!("Shutdown signal received. Stopping services gracefully...");
            // Notify the consumer loop to break
            shutdown_notify.notify_waiters();
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }

    // Wait for tasks to finish (cleanup)
    let _ = tokio::join!(producer_handle, consumer_handle);

    info!("Application execution completed.");
    Ok(())
}

// ============================================================================
// 3. Producer Logic
// ============================================================================

async fn run_producer(config: AppConfig) -> Result<()> {
    info!("Initializing Producer targeting topic: {}", config.input_topic);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("message.timeout.ms", "5000")
        // Enable idempotence for exactly-once semantics
        .set("enable.idempotence", "true")
        .create()
        .context("Failed to create Kafka Producer")?;

    let test_data = vec!["apple", "banana", "cherry", "date", "elderberry"];

    // Simulate data stream
    for i in 0..config.msg_count {
        let data = test_data[i % test_data.len()];
        let key = format!("key-{}", i);

        // Construct the record
        let record = FutureRecord::to(&config.input_topic)
            .key(&key)
            .payload(data);

        // Send asynchronously with a timeout
        let delivery_status = producer
            .send(record, Timeout::After(Duration::from_secs(5)))
            .await;

        match delivery_status {
            Ok((partition, offset)) => {
                // Log progress every 1000 messages to reduce noise
                if (i + 1) % 1000 == 0 {
                    info!(
                        "✓ Produced [{}/{}] -> Partition: {}, Offset: {}",
                        i + 1, config.msg_count, partition, offset
                    );
                }
            }
            Err((e, _)) => error!("✗ Failed to send message #{}: {:?}", i, e),
        }

        // Slight delay to simulate real-world throughput (prevents local buffer overflow)
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    info!("Producer finished. Total messages sent: {}", config.msg_count);
    Ok(())
}

// ============================================================================
// 4. Consumer Logic
// ============================================================================

async fn run_consumer(config: AppConfig, shutdown: Arc<Notify>) -> Result<()> {
    info!("Initializing Consumer listening on topic: {}", config.output_topic);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &config.group_id)
        .set("bootstrap.servers", &config.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Auto-commit is enabled, but strictly robust apps might manage offsets manually
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("Failed to create Kafka Consumer")?;

    consumer
        .subscribe(&[&config.output_topic])
        .context("Failed to subscribe to topic")?;

    info!("Consumer started. Waiting for messages...");

    loop {
        tokio::select! {
            // Branch 1: Receive message from Kafka
            recv_result = consumer.recv() => {
                match recv_result {
                    Ok(m) => {
                        // Process the message (Validation & Logging)
                        if let Err(e) = process_message(&m) {
                            error!("Business logic error while processing message: {:?}", e);
                        }
                    },
                    Err(e) => warn!("Kafka error during consumption: {}", e),
                }
            }
            // Branch 2: Handle graceful shutdown signal
            _ = shutdown.notified() => {
                info!("Consumer received shutdown signal. Exiting loop.");
                break;
            }
        }
    }

    Ok(())
}

// ============================================================================
// 5. Business Logic & Validation
// ============================================================================

/// Parses the payload, validates business rules, and logs the result.
fn process_message(message: &BorrowedMessage) -> Result<()> {
    // 1. Extract payload as string
    let payload = match message.payload_view::<str>() {
        None => return Ok(()), // Ignore empty messages
        Some(Ok(s)) => s,
        Some(Err(e)) => {
            warn!("Payload encoding error (not UTF-8): {:?}", e);
            return Ok(());
        }
    };

    // 2. Strong Typing: Parse JSON into struct
    match serde_json::from_str::<ProcessingResult>(payload) {
        Ok(result) => {
            // 3. Data Integrity Check
            if result.validate_consistency() {
                info!(
                    topic = message.topic(),
                    offset = message.offset(),
                    total = result.total_processed,
                    details = ?result.counter_map,
                    "✓ Data Validation Passed"
                );
            } else {
                // Calculation error detection
                let actual_sum: u64 = result.counter_map.values().sum();
                error!(
                    topic = message.topic(),
                    offset = message.offset(),
                    expected = result.total_processed,
                    actual = actual_sum,
                    "✗ Data Validation FAILED: Sum mismatch"
                );
            }
        },
        Err(e) => {
            // Handling malformed JSON or schema mismatch
            error!(
                topic = message.topic(),
                offset = message.offset(),
                raw_payload = payload,
                error = ?e,
                "Failed to deserialize JSON payload"
            );
        }
    }

    Ok(())
}