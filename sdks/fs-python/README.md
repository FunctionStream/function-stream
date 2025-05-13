# FS SDK

A simple RPC service SDK that allows users to focus on their core business logic.

## Installation

```bash
pip install fs-sdk
```

## Quick Start

Here's a simple example of how to use the FS SDK:

```python
import asyncio
from fs_sdk import FSService

async def my_process_function(request_data: dict) -> dict:
    """
    Process the incoming request data.
    
    Args:
        request_data (dict): The request data containing:
            - action (str): The action to perform
            - message (str, optional): A message to process
            - data (dict, optional): Additional data to process
            
    Returns:
        dict: The response containing:
            - status (str): The status of the operation
            - message (str): A response message
    """
    # Your business logic here
    return {
        'status': 'success',
        'message': 'Hello from my service!'
    }

async def main():
    # Initialize the service with your process function
    service = FSService(
        service_url='pulsar://localhost:6650',
        request_topic='request-topic',
        process_func=my_process_function
    )
    
    # Get the JSON Schema for your process function
    schema = service.get_schema_json()
    print("Function Schema:", schema)
    
    try:
        # Start processing requests
        await service.start()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        await service.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## Features

- Simple and intuitive API
- Focus on your business logic
- Automatic request/response handling
- Built-in error handling
- Asynchronous processing
- Automatic JSON Schema generation from function docstrings and type hints

## JSON Schema Generation

The FS SDK can automatically generate JSON Schema from your process function's docstring and type hints. This is useful for:

- Documentation
- API validation
- Client code generation
- OpenAPI/Swagger integration

To use this feature, simply add type hints and docstrings to your process function:

```python
async def my_process_function(request_data: dict) -> dict:
    """
    Process the incoming request data.
    
    Args:
        request_data (dict): The request data containing:
            - action (str): The action to perform
            - message (str, optional): A message to process
            - data (dict, optional): Additional data to process
            
    Returns:
        dict: The response containing:
            - status (str): The status of the operation
            - message (str): A response message
    """
    # Your business logic here
    return {
        'status': 'success',
        'message': 'Hello from my service!'
    }
```

The SDK will automatically generate a JSON Schema that describes both the input and output of your function. You can access this schema using:

```python
# Get schema as a dictionary
schema = service.get_schema()

# Get schema as a JSON string
schema_json = service.get_schema_json()
```

## Requirements

- Python 3.7+
- Apache Pulsar

## License

MIT License 