"""
FSContext module provides a context object that manages configuration access for FunctionStream SDK.
"""

import logging
from typing import Optional
from .config import Config

# Configure logging
logger = logging.getLogger(__name__)

class FSContext:
    """
    Context class that provides access to configuration values and runtime context.
    
    This class serves as a wrapper around the Config object, providing a clean interface
    for accessing configuration values and handling any potential errors during access.
    
    Attributes:
        config (Config): The configuration object containing all settings.
    """
    
    def __init__(self, config: Config):
        """
        Initialize the FSContext with a configuration object.

        Args:
            config (Config): The configuration object to be used by this context.
        """
        self.config = config

    def get_config(self, config_name: str) -> str:
        """
        Get a configuration value by name.

        This method safely retrieves a configuration value, handling any potential
        errors during the retrieval process. If an error occurs, it logs the error
        and returns an empty string.

        Args:
            config_name (str): The name of the configuration to retrieve.

        Returns:
            str: The configuration value if found, empty string if not found or error occurs.
        """
        try:
            return str(self.config.get_config_value(config_name))
        except Exception as e:
            logger.error(f"Error getting config {config_name}: {str(e)}")
            return "" 