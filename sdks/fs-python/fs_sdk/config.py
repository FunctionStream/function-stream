import os
import yaml
from typing import Dict, Any, Optional, List

class Config:
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration from YAML file.
        
        Args:
            config_path (str): Path to the configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Returns:
            Dict[str, Any]: Configuration dictionary
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
            
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    def get_config_value(self, config_name: str) -> Any:
        """
        Get a configuration value by name from the config section.
        
        Args:
            config_name (str): The name of the configuration to retrieve
            
        Returns:
            Any: The configuration value, or None if not found
        """
        config_section = self.config.get('config', [])
        for item in config_section:
            if config_name in item:
                return item[config_name]
        return None
            
    @property
    def service_url(self) -> str:
        """Get Pulsar service URL."""
        return self.config.get('pulsar', {}).get('service_url', 'pulsar://localhost:6650')
        
    @property
    def auth_plugin(self) -> str:
        """Get Pulsar auth plugin."""
        return self.config.get('pulsar', {}).get('authPlugin', '')
        
    @property
    def auth_params(self) -> str:
        """Get Pulsar auth parameters."""
        return self.config.get('pulsar', {}).get('authParams', '')

    @property
    def module(self) -> str:
        """Get the module name."""
        return self.config.get('module')

    @property
    def sources(self) -> List[Dict[str, Any]]:
        """Get the sources configuration."""
        return self.config.get('sources', [])

    @property
    def request_sources(self) -> List[Dict[str, Any]]:
        """Get the request sources configuration."""
        return self.config.get('requestSource', [])

    @property
    def sinks(self) -> List[Dict[str, Any]]:
        """Get the sinks configuration."""
        return self.config.get('sink', [])

    def get_pulsar_source_config(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """Get Pulsar source configuration from a source spec."""
        return source.get('pulsar', {})

    def get_pulsar_sink_config(self, sink: Dict[str, Any]) -> Dict[str, Any]:
        """Get Pulsar sink configuration from a sink spec."""
        return sink.get('pulsar', {})

    @property
    def request_topic(self) -> str:
        """Get request topic for the active module."""
        topic = self.config.get('request_topic')
        if not topic:
            raise ValueError("request_topic is not set in config.yaml")
        return topic
        
    @property
    def subscription_name(self) -> str:
        """Get subscription name for the active module."""
        return self.config.get('subscription_name', 'fs-sdk-subscription')

    @property
    def name(self) -> str:
        """Get the function name."""
        return self.config.get('name')

    @property
    def description(self) -> str:
        """Get the function description."""
        return self.config.get('description')

    @property
    def max_concurrent_requests(self) -> int:
        """Get maximum number of concurrent requests."""
        return self.config.get('pulsar', {}).get('max_concurrent_requests', 10)

    @property
    def max_producer_cache_size(self) -> int:
        """Get maximum number of producers to cache."""
        return self.config.get('pulsar', {}).get('max_producer_cache_size', 100)

    @property
    def active_module(self) -> Optional[str]:
        """Get the name of the active module."""
        return self.config.get('modules', {}).get('active_module')

    def get_module_config(self, module_name: str) -> Dict[str, Any]:
        """Get configuration for a specific module."""
        return self.config.get('modules', {}).get(module_name, {}) 