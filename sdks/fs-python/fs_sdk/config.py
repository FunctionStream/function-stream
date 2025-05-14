import os
import yaml
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field

class PulsarConfig(BaseModel):
    service_url: str = "pulsar://localhost:6650"
    auth_plugin: str = ""
    auth_params: str = ""
    max_concurrent_requests: int = 10
    max_producer_cache_size: int = 100

class ModuleConfig(BaseModel):
    active_module: Optional[str] = None
    module_configs: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

class SourceSpec(BaseModel):
    pulsar: Dict[str, Any] = Field(default_factory=dict)

class SinkSpec(BaseModel):
    pulsar: Dict[str, Any] = Field(default_factory=dict)

class Config(BaseModel):
    pulsar: PulsarConfig = Field(default_factory=PulsarConfig)
    module: Optional[str] = None
    sources: List[SourceSpec] = Field(default_factory=list)
    requestSource: Optional[SourceSpec] = None
    sink: Optional[SinkSpec] = None
    subscription_name: str = "fs-sdk-subscription"
    name: Optional[str] = None
    description: Optional[str] = None
    modules: ModuleConfig = Field(default_factory=ModuleConfig)
    config: List[Dict[str, Any]] = Field(default_factory=list)

    @classmethod
    def from_yaml(cls, config_path: str = "config.yaml") -> "Config":
        """
        Initialize configuration from YAML file.
        
        Args:
            config_path (str): Path to the configuration file
            
        Returns:
            Config: Configuration instance
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
            return cls(**config_data)

    def get_config_value(self, config_name: str) -> Any:
        """
        Get a configuration value by name from the config section.
        
        Args:
            config_name (str): The name of the configuration to retrieve
            
        Returns:
            Any: The configuration value, or None if not found
        """
        for item in self.config:
            if config_name in item:
                return item[config_name]
        return None

    def get_module_config(self, module_name: str) -> Dict[str, Any]:
        """Get configuration for a specific module."""
        return self.modules.module_configs.get(module_name, {}) 