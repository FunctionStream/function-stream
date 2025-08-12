"""
FSContext module provides a context object that manages configuration access for FunctionStream SDK.

This module defines the FSContext class which serves as a wrapper around the Config object,
providing a clean interface for accessing configuration values and handling any potential
errors during access. It also provides methods for metadata access and data production.
"""

import logging
from typing import Any, Dict
from datetime import datetime

from .config import Config

# Configure logging
logger = logging.getLogger(__name__)


class FSContext:
    """
    Context class that provides access to configuration values and runtime context.
    
    This class serves as a wrapper around the Config object, providing a clean interface
    for accessing configuration values and handling any potential errors during access.
    It also provides methods for metadata access and data production capabilities.
    
    Attributes:
        config (Config): The configuration object containing all settings.
        function (FSFunction, optional): Reference to the parent FSFunction instance.
    """

    def __init__(self, config: Config):
        """
        Initialize the FSContext with a configuration object.

        Args:
            config (Config): The configuration object to be used by this context.
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

    def get_metadata(self, key: str) -> Any:
        """
        Get metadata value by key.

        This method retrieves metadata associated with the current message.

        Args:
            key (str): The metadata key to retrieve.

        Returns:
            Any: The metadata value, currently always None.
        """
        return None

    def produce(self, data: Dict[str, Any], event_time: datetime = None) -> None:
        """
        Produce data to the output stream.

        This method is intended to send processed data to the output stream.

        Args:
            data (Dict[str, Any]): The data to produce.
            event_time (datetime, optional): The timestamp for the event. Defaults to None.

        Returns:
            None: Currently always returns None.
        """
        return None

    def get_configs(self) -> Dict[str, Any]:
        """
        Get all configuration values.

        Returns a dictionary containing all configuration key-value pairs.

        Returns:
            Dict[str, Any]: A dictionary containing all configuration values.
        """
        return self.config.config

    def get_module(self) -> str:
        """
        Get the current module name.

        Returns the name of the module currently being executed.

        Returns:
            str: The name of the current module.
        """
        return self.config.module
