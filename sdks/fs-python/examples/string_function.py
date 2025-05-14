"""
String Processing Function Example

This module demonstrates a simple string processing function that appends an exclamation mark
to the input text. It serves as a basic example of how to create and run a FunctionStream
serverless function.

The function:
1. Receives a request containing a text field
2. Appends an exclamation mark to the text
3. Returns the modified text in a response

This example shows the basic structure of a FunctionStream function, including:
- Function definition and implementation
- FSFunction initialization
- Service startup and graceful shutdown
- Error handling
"""

import asyncio
from typing import Dict, Any

from fs_sdk import FSFunction
from fs_sdk.context import FSContext

async def string_process_function(context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a string by appending an exclamation mark.
    
    This function demonstrates a simple string transformation that can be used
    as a building block for more complex text processing pipelines.
    
    Args:
        data (Dict[str, Any]): Request data containing a 'text' field with the input string
        
    Returns:
        Dict[str, Any]: Response containing the processed string with an exclamation mark appended
        
    Example:
        Input:  {"text": "Hello"}
        Output: {"result": "Hello!"}
    """
    # Extract the input text from the request data
    text = data.get('text', '')
    
    # Append an exclamation mark to the text
    result = f"{text}!"

    # Log the result for debugging purposes
    print(f"Result: {result}")
    print(f"Config: {context.get_config('test')}")
    
    return {"result": result}

async def main():
    """
    Main function to initialize and run the string processing service.
    
    This function:
    1. Creates an FSFunction instance with the string processing function
    2. Starts the service
    3. Handles graceful shutdown and error cases
    """
    # Initialize the FunctionStream function with our string processor
    function = FSFunction(
        process_funcs={
            'string': string_process_function
        }
    )

    try:
        print("Starting string processing function service...")
        await function.start()
    except asyncio.CancelledError:
        print("\nInitiating graceful shutdown...")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        await function.close()

if __name__ == "__main__":
    try:
        # Run the main function in an asyncio event loop
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nService stopped") 