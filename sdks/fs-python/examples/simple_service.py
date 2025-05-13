import asyncio
from fs_sdk import FSFunction

async def module1_process_function(request_data: dict) -> dict:
    """
    Process function for module1.
    
    Args:
        request_data (dict): The request data
        
    Returns:
        dict: The response data
    """
    # Example: Process the request data
    action = request_data.get('action')
    
    if action == 'ping':
        return {
            'status': 'success',
            'message': f"Module1 Pong! Received: {request_data.get('message', '')}"
        }
    
    elif action == 'process':
        data = request_data.get('data', {})
        items = data.get('items', [])
        config = data.get('config', {})
        
        # Your business logic here
        result = {
            'status': 'success',
            'processed_items': len(items),
            'sum': sum(items),
            'config_applied': config,
            'module': 'module1'
        }
        return result
    
    else:
        return {
            'status': 'error',
            'message': f"Unknown action: {action}"
        }

async def module2_process_function(request_data: dict) -> dict:
    """
    Process function for module2.
    
    Args:
        request_data (dict): The request data
        
    Returns:
        dict: The response data
    """
    # Example: Process the request data
    action = request_data.get('action')
    
    if action == 'ping':
        return {
            'status': 'success',
            'message': f"Module2 Pong! Received: {request_data.get('message', '')}"
        }
    
    elif action == 'process':
        data = request_data.get('data', {})
        items = data.get('items', [])
        config = data.get('config', {})
        
        # Different business logic for module2
        result = {
            'status': 'success',
            'processed_items': len(items),
            'average': sum(items) / len(items) if items else 0,
            'config_applied': config,
            'module': 'module2'
        }
        return result
    
    else:
        return {
            'status': 'error',
            'message': f"Unknown action: {action}"
        }

async def main():
    # Initialize the function with process functions for each module
    function = FSFunction(
        process_funcs={
            'module1': module1_process_function,
            'module2': module2_process_function
        }
    )
    
    try:
        # Start processing requests
        await function.start()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        await function.close()

if __name__ == "__main__":
    asyncio.run(main()) 