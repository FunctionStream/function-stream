"""
FSModule module provides the base class for all FunctionStream modules.

This module defines the abstract base class FSModule that all FunctionStream modules
must inherit from. It provides a common interface for module initialization and
data processing, ensuring consistency across different module implementations.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any

from .context import FSContext


class FSModule(ABC):
    """
    Base class for all FunctionStream modules.
    
    This abstract base class provides a common interface for all modules in the 
    FunctionStream SDK. Each module must implement the init and process methods
    to handle module initialization and incoming data processing.
    
    Attributes:
        name (str): The name of the module (to be set during initialization).
    """

    @abstractmethod
    def init(self, context: FSContext):
        """
        Initialize the module with the provided context.
        
        This method is called during module initialization to set up the module
        with the necessary context and configuration. Subclasses must implement
        this method to handle any required setup.
        
        Args:
            context (FSContext): The context object containing configuration and 
                               runtime information for the module.
        """

    @abstractmethod
    async def process(self, context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process incoming data asynchronously.
        
        This method is the core processing function that handles incoming data.
        Subclasses must implement this method to define the specific data processing
        logic for their module. The method should be asynchronous to support
        non-blocking operations.
        
        Args:
            context (FSContext): The context object containing configuration and 
                               runtime information.
            data (Dict[str, Any]): The input data to process. This is typically
                                  a dictionary containing the message payload
                                  and any associated metadata.
            
        Returns:
            Dict[str, Any]: The processed data that should be returned as the
                           result of the processing operation.
            
        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement process method")
