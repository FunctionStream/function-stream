import os
from typing import Dict, Any, Optional, List

import yaml
from pydantic import BaseModel, Field


class PulsarConfig(BaseModel):
    """
    Configuration for Pulsar connection settings.
    
    This class defines the connection parameters for connecting to a Pulsar cluster.
    It includes authentication settings and performance tuning options.
    """
    serviceUrl: str = "pulsar://localhost:6650"
    """Pulsar service URL in format 'pulsar://host:port' or 'pulsar+ssl://host:port' for SSL"""

    authPlugin: str = ""
    """Authentication plugin class name (e.g., 'org.apache.pulsar.client.impl.auth.AuthenticationTls')"""

    authParams: str = ""
    """Authentication parameters in JSON format or key-value pairs"""

    max_concurrent_requests: int = 10
    """Maximum number of concurrent requests allowed for this connection"""


class PulsarSourceConfig(BaseModel):
    """
    Configuration for Pulsar source/sink specific settings.
    
    This class defines topic-specific Pulsar configuration that can override
    the global PulsarConfig settings for individual sources or sinks.
    """
    topic: str
    """Pulsar topic name to consume from or produce to"""


class SourceSpec(BaseModel):
    """
    Specification for data sources.
    
    This class defines the configuration for input data sources.
    Currently supports Pulsar as a source type.
    """
    pulsar: Optional[PulsarSourceConfig] = None
    """Pulsar source configuration (optional)"""


class SinkSpec(BaseModel):
    """
    Specification for data sinks.
    
    This class defines the configuration for output data sinks.
    Currently supports Pulsar as a sink type.
    """
    pulsar: Optional[PulsarSourceConfig] = None
    """Pulsar sink configuration (optional)"""


class Metric(BaseModel):
    """
    Configuration for metrics and monitoring.
    
    This class defines settings for metrics collection and monitoring endpoints.
    """
    port: Optional[int] = 9099
    """Port number for metrics endpoint (default: 9099)"""


class Config(BaseModel):
    """
    Main configuration class for FunctionStream SDK.
    
    This is the root configuration class that contains all settings for the SDK,
    including Pulsar connection, sources, sinks, metrics, and custom configuration.
    """
    name: Optional[str] = None
    """Function name identifier (optional)"""

    description: Optional[str] = None
    """Function description (optional)"""

    pulsar: PulsarConfig = Field(default_factory=PulsarConfig)
    """Pulsar connection configuration"""

    module: str = "default"
    """Module name for the function (default: 'default')"""

    sources: List[SourceSpec] = Field(default_factory=list)
    """List of input data sources"""

    requestSource: Optional[SourceSpec] = None
    """Request source configuration for request-response pattern (optional)"""

    sink: Optional[SinkSpec] = None
    """Output sink configuration (optional)"""

    subscriptionName: str = "function-stream-sdk-subscription"
    """Pulsar subscription name for consuming messages"""

    metric: Metric = Field(default_factory=Metric)
    """Metrics and monitoring configuration"""

    config: Dict[str, Any] = Field(default_factory=dict)
    """Custom configuration key-value pairs for function-specific settings"""

    @classmethod
    def from_yaml(cls, config_path: str = "config.yaml") -> "Config":
        """
        Initialize configuration from YAML file.
        
        This method loads configuration from a YAML file and creates a Config instance.
        The YAML file should contain configuration keys that match the Config class fields.
        
        Args:
            config_path (str): Path to the configuration file (default: "config.yaml")
            
        Returns:
            Config: Configuration instance loaded from the YAML file
            
        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            yaml.YAMLError: If the YAML file is malformed
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
            return cls(**config_data)

    def get_config_value(self, config_name: str) -> Any:
        """
        Get a configuration value by name from the config section.
        
        This method retrieves custom configuration values that were set in the
        config dictionary. Useful for accessing function-specific settings.
        
        Args:
            config_name (str): The name of the configuration to retrieve
            
        Returns:
            Any: The configuration value, or None if not found
        """
        return self.config.get(config_name)
