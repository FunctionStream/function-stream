import asyncio
import pulsar
import json
import uuid
import time

async def test_string_function():
    # 创建 Pulsar 客户端
    client = pulsar.Client('pulsar://localhost:6650')
    
    # 创建消费者来接收响应
    consumer = client.subscribe(
        'response-topic',  # 响应主题
        'test-subscription',
        consumer_type=pulsar.ConsumerType.Shared
    )
    
    # 创建生产者来发送请求
    producer = client.create_producer('string-topic')  # 请求主题
    
    try:
        # 生成唯一的请求ID
        request_id = str(uuid.uuid4())
        
        # 准备请求数据
        request_data = {
            'text': 'Hello World'
        }
        
        # 发送请求
        producer.send(
            json.dumps(request_data).encode('utf-8'),
            properties={
                'request_id': request_id,
                'response_topic': 'response-topic'
            }
        )
        
        print(f"已发送请求，等待响应...")
        
        # 等待并接收响应
        while True:
            try:
                msg = consumer.receive(timeout_millis=5000)  # 5秒超时
                msg_props = msg.properties()
                
                # 检查是否是我们的请求的响应
                if msg_props.get('request_id') == request_id:
                    response_data = json.loads(msg.data().decode('utf-8'))
                    print(f"收到响应: {response_data}")
                    consumer.acknowledge(msg)
                    break
                else:
                    # 如果不是我们的响应，重新入队
                    consumer.negative_acknowledge(msg)
            except pulsar.Timeout:
                print("等待响应超时")
                break
                
    finally:
        # 清理资源
        producer.close()
        consumer.close()
        client.close()

if __name__ == "__main__":
    asyncio.run(test_string_function()) 