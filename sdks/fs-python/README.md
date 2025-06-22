<!--
  Copyright 2024 Function Stream Org.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# FunctionStream Python SDK

FunctionStream SDK is a powerful Python library for building and deploying serverless functions that process messages
from Apache Pulsar. It provides a simple yet flexible framework for creating event-driven applications with robust error
handling, metrics collection, and resource management.

## Features

- **Easy Function Development**: Simple API for creating serverless functions
- **Message Processing**: Built-in support for Apache Pulsar message processing
- **Metrics Collection**: Automatic collection of performance metrics
- **Resource Management**: Efficient handling of connections and resources
- **Configuration Management**: Flexible configuration through YAML files
- **Error Handling**: Comprehensive error handling and logging

## Installation

```bash
pip install function-stream
```

## Quick Start

1. Create a function that processes messages:

```python
from function_stream import FSFunction

async def my_process_function(request_data: dict) -> dict:
    # Process the request data
    result = process_data(request_data)
    return {"result": result}

# Initialize and run the function
function = FSFunction(
    process_funcs={
        'my_module': my_process_function
    }
)

await function.start()
```

2. Create a configuration file (`config.yaml`):

```yaml
pulsar:
  service_url: "pulsar://localhost:6650"
  authPlugin: ""  # Optional
  authParams: ""  # Optional

module: "my_module"
subscriptionName: "my-subscription"

requestSource:
  - pulsar:
      topic: "input-topic"

sink:
  pulsar:
    topic: "output-topic"
```

3. Define your function package (`package.yaml`):

```yaml
name: my_function
type: pulsar
modules:
  my_module:
    name: my_process
    description: "Process incoming messages"
    inputSchema:
      type: object
      properties:
        data:
          type: string
      required:
        - data
    outputSchema:
      type: object
      properties:
        result:
          type: string
```

## Core Components

### FSFunction

The main class for creating serverless functions. It handles:

- Message consumption and processing
- Response generation
- Resource management
- Metrics collection
- Error handling

### Configuration

The SDK uses YAML configuration files to define:

- Pulsar connection settings
- Module selection
- Topic subscriptions
- Input/output topics
- Custom configuration parameters

### Metrics

Built-in metrics collection for:

- Request processing time
- Success/failure rates
- Message throughput
- Resource utilization

## Examples

Check out the `examples` directory for complete examples:

- `string_function.py`: A simple string processing function
- `test_string_function.py`: Test client for the string function
- `config.yaml`: Example configuration
- `package.yaml`: Example package definition

## Best Practices

1. **Error Handling**
    - Always handle exceptions in your process functions
    - Use proper logging for debugging
    - Implement graceful shutdown

2. **Resource Management**
    - Close resources properly
    - Use context managers when possible
    - Monitor resource usage

3. **Configuration**
    - Use environment variables for sensitive data
    - Validate configuration values
    - Document configuration options

4. **Testing**
    - Write unit tests for your functions
    - Test error scenarios
    - Validate input/output schemas

## Development

### Prerequisites

- Python 3.7+
- Apache Pulsar
- pip

### Setup Development Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
.\venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Install the package in development mode
python -m pip install -e .
```

### Running Tests

```bash
make test
```

## Support

For support, please open an issue in the GitHub repository or contact the maintainers. 