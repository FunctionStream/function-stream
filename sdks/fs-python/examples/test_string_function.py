"""
String Function Test Client

This module provides a test client for the string processing function example.
It demonstrates how to:
1. Connect to a Pulsar broker
2. Send a request to the string processing function
3. Receive and process the response
4. Handle timeouts and errors
5. Clean up resources properly

The test client:
- Creates a unique request ID for tracking
- Sends a test message to the string processing function
- Waits for and validates the response
- Implements proper error handling and resource cleanup
"""

import asyncio
import pulsar
import json
import uuid
import time

async def test_string_function():
    """
    Test the string processing function by sending a request and waiting for a response.
    
    This function:
    1. Connects to the Pulsar broker
    2. Sets up a consumer for responses
    3. Creates a producer for sending requests
    4. Sends a test request with a unique ID
    5. Waits for and processes the response
    6. Cleans up all resources
    
    The test uses a 5-second timeout for receiving responses.
    """
    # Create a Pulsar client connection
    client = pulsar.Client('pulsar://localhost:6650')
    
    # Set up a consumer to receive responses
    consumer = client.subscribe(
        'response-topic',  # Response topic name
        'test-subscription',
        consumer_type=pulsar.ConsumerType.Shared
    )
    
    # Create a producer to send requests
    producer = client.create_producer('string-topic')  # Request topic name
    
    try:
        # Generate a unique request ID for tracking
        request_id = str(uuid.uuid4())
        
        # Prepare the test request data
        request_data = {
            'text': 'Hello World'
        }
        
        # Send the request with metadata
        producer.send(
            json.dumps(request_data).encode('utf-8'),
            properties={
                'request_id': request_id,
                'response_topic': 'response-topic'
            }
        )
        
        print(f"Request sent, waiting for response...")
        
        # Wait for and process the response
        while True:
            try:
                # Receive message with a 5-second timeout
                msg = consumer.receive(timeout_millis=5000)
                msg_props = msg.properties()
                
                # Verify if this is the response to our request
                if msg_props.get('request_id') == request_id:
                    response_data = json.loads(msg.data().decode('utf-8'))
                    print(f"Received response: {response_data}")
                    consumer.acknowledge(msg)
                    break
                else:
                    # If not our response, requeue the message
                    consumer.negative_acknowledge(msg)
            except pulsar.Timeout:
                print("Response timeout - no response received within 5 seconds")
                break
                
    finally:
        # Clean up resources in the correct order
        producer.close()
        consumer.close()
        client.close()

if __name__ == "__main__":
    # Run the test function in an asyncio event loop
    asyncio.run(test_string_function()) 