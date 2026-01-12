use std::string::String;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    // 记录开始时间
    let start_time = std::time::Instant::now();
    println!("Kafka Broker: {}", "127.0.0.1:9092");
    println!("Input Topic: {}", "input_topic");
    println!("Output Topic: {}", "output_topic");
    let broker = "127.0.0.1:9092";
    let input_topic = "input-topic";
    let output_topic ="output-topic";

    println!("Kafka Broker: {}", "");
    println!("Input Topic: {}", input_topic);
    println!("Output Topic: {}", output_topic);

    // 先创建消费者并验证创建成功
    println!("\n=== 创建消费者 ===");
    let consumer = create_consumer(broker, output_topic).await;
    println!("✓ 消费者创建成功");

    // 然后生产消息
    println!("\n=== 开始生产消息 ===");
    produce_messages(broker, input_topic).await;

    // 等待一下，让消息被处理
    println!("\n等待 5 秒让处理器处理消息...");
    sleep(Duration::from_secs(5)).await;

    // 使用之前创建的消费者验证消息
    println!("\n=== 开始消费输出消息 ===");
    consume_messages_with_consumer(consumer).await;

    
    // 计算并打印总耗时
    let elapsed = start_time.elapsed();
    let total_seconds = elapsed.as_secs_f64();
    println!("\n=== 执行完成 ===");
    println!("总耗时: {:.3} 秒", total_seconds);
}

async fn produce_messages(broker: &str, topic: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("创建生产者失败");

    // 测试数据：发送多个字符串，每个字符串发送多次
    let test_data = vec!["apple", "banana", "cherry", "apple", "banana", "apple"];

    for (i, data) in test_data.iter().enumerate() {
        let key = format!("key-{}", i);
        let record = FutureRecord::to(topic)
            .key(&key)
            .payload(data.as_bytes());

        match producer.send(record, Timeout::After(Duration::from_secs(5))).await {
            Ok(delivery) => {
                println!("✓ 已发送消息 #{}: {} -> partition: {}, offset: {}", 
                    i + 1, data, delivery.partition, delivery.offset);
            }
            Err((e, _)) => {
                eprintln!("✗ 发送消息 #{} 失败: {:?}", i + 1, e);
            }
        }

        // 稍微延迟，避免发送过快
        sleep(Duration::from_millis(100)).await;
    }

    println!("\n总共发送了 {} 条消息", test_data.len());
}

async fn create_consumer(broker: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "counter-test-consumer")
        .set("bootstrap.servers", broker)
        .set("session.timeout.ms", "6000")
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("创建消费者失败");

    consumer
        .subscribe(&[topic])
        .expect("订阅主题失败");

    consumer
}

async fn consume_messages_with_consumer(consumer: StreamConsumer) {
    println!("等待消费消息（最多等待 30 秒）...");

    let mut message_count = 0;
    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(30);

    loop {
        if start_time.elapsed() > timeout {
            println!("\n超时：30 秒内未收到新消息");
            break;
        }

        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(message)) => {
                message_count += 1;
                process_message(&message, message_count);
            }
            Ok(Err(e)) => {
                eprintln!("消费消息时出错: {}", e);
                break;
            }
            Err(_) => {
                // 超时，继续等待
                continue;
            }
        }
    }

    if message_count == 0 {
        println!("\n未收到任何消息");
    } else {
        println!("\n总共消费了 {} 条消息", message_count);
    }
}

fn process_message(message: &BorrowedMessage, count: usize) {
    match message.payload_view::<str>() {
        None => {
            println!("消息 #{}: (无 payload)", count);
        }
        Some(Ok(payload)) => {
            println!("\n--- 消息 #{} ---", count);
            println!("分区: {}, 偏移量: {}", message.partition(), message.offset());
            println!("Payload: {}", payload.to_string());

            // 尝试解析为 JSON
            match serde_json::from_str::<HashMap<String, i64>>(payload) {
                Ok(counter_map) => {
                    println!("解析的计数器结果:");
                    for (key, value) in counter_map.iter() {
                        println!("  {}: {}", key, value);
                    }
                }
                Err(e) => {
                    println!("无法解析为 JSON: {}", e);
                }
            }
        }
        Some(Err(e)) => {
            println!("消息 #{}: 解码 payload 失败: {}", count, e);
        }
    }
}

