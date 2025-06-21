from abc import ABC, abstractmethod
from typing import Dict, Any

from .context import FSContext


class FSModule(ABC):
    """
    Base class for all FunctionStream modules.
    
    This class provides a common interface for all modules in the FunctionStream SDK.
    Each module must implement the process method to handle incoming data.
    
    Attributes:
        name (str): The name of the module
    """

    @abstractmethod
    def init(self, context: FSContext):
        """
        Initialize the module with a name.
        
        Args:
            name (str): The name of the module
        """

    @abstractmethod
    async def process(self, context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process incoming data.
        
        Args:
            context (FSContext): The context object containing configuration and runtime information
            data (Dict[str, Any]): The input data to process
            
        Returns:
            Union[Dict[str, Any], Awaitable[Dict[str, Any]]]: The processed data or an awaitable that will resolve to the processed data
        """
        raise NotImplementedError("Subclasses must implement process method")
