import os
import yaml
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field

class PulsarConfig(BaseModel):
    serviceUrl: str = "pulsar://localhost:6650"
    authPlugin: str = ""
    authParams: str = ""
    max_concurrent_requests: int = 10

class PulsarSourceConfig(BaseModel):
    topic: str
    serviceUrl: Optional[str] = None
    authPlugin: Optional[str] = None
    authParams: Optional[str] = None

class SourceSpec(BaseModel):
    pulsar: Optional[PulsarSourceConfig] = None

class SinkSpec(BaseModel):
    pulsar: Optional[PulsarSourceConfig] = None

class Config(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    pulsar: PulsarConfig = Field(default_factory=PulsarConfig)
    module: str = "default"
    sources: List[SourceSpec] = Field(default_factory=list)
    requestSource: Optional[SourceSpec] = None
    sink: Optional[SinkSpec] = None
    subscriptionName: str = "fs-sdk-subscription"
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
