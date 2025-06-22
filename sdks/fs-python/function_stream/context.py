"""
FSContext module provides a context object that manages configuration access for FunctionStream SDK.
"""

import logging
from typing import Any, Dict

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
        function (FSFunction, optional): Reference to the parent FSFunction instance.
    """

    def __init__(self, config: Config):
        """
        Initialize the FSContext with a configuration object and optional FSFunction reference.

        Args:
            config (Config): The configuration object to be used by this context.
            function (FSFunction, optional): The parent FSFunction instance.
        """
        self.config = config

    def get_config(self, config_name: str) -> Any:
        """
        Get a configuration value by name.

        This method safely retrieves a configuration value, handling any potential
        errors during the retrieval process. If an error occurs, it logs the error
        and returns an empty string.

        Args:
            config_name (str): The name of the configuration to retrieve.

        Returns:
            Any: The configuration value if found, empty string if not found or error occurs.
        """
        try:
            return self.config.get_config_value(config_name)
        except Exception as e:
            logger.error(f"Error getting config {config_name}: {str(e)}")
            return ""

    def get_configs(self) -> Dict[str, Any]:
        return self.config.config

    def get_module(self) -> str:
        return self.config.module
