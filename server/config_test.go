/*
 * Copyright 2024 Function Stream Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package server

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigFromYaml(t *testing.T) {
	c, err := LoadConfigFromFile("../tests/test_config.yaml")
	require.NoError(t, err)
	assertConfig(t, c)
}

func TestLoadConfigFromJson(t *testing.T) {
	c, err := LoadConfigFromFile("../tests/test_config.json")
	require.Nil(t, err)
	assertConfig(t, c)
}

func TestLoadConfigFromFiles(t *testing.T) {
	// Setup test cases with different file formats
	testCases := []struct {
		name        string
		filePath    string
		setup       func() (string, func())
		wantErr     bool
		errContains string
	}{
		{
			name:     "ValidYAMLConfig",
			filePath: "test_config.yaml",
			setup: func() (string, func()) {
				content := `
listen-address: ":17300"
enable-tls: true
tls-cert-file: /path/to/cert.pem
tls-key-file: /path/to/key.pem
package-loader:
  type: my-package-loader
  config:
    path: tests/packages
event-storage:
  type: my-event-storage
  config:
    url: xxx://localhost:12345
state-store:
  type: my-state-store
  config:
    url: xxx://localhost:12345
runtimes:
  - type: my-runtime-1
    config:
      my-config: xxx
  - type: my-runtime-2
    config:
      my-config: xxx
`
				return createTempFile(t, content, "*.yaml")
			},
		},
		{
			name:     "ValidJSONConfig",
			filePath: "test_config.json",
			setup: func() (string, func()) {
				content := `{
  "listen-address": ":17300",
  "enable-tls": true,
  "tls-cert-file": "/path/to/cert.pem",
  "tls-key-file": "/path/to/key.pem",
  "package-loader": {
    "type": "my-package-loader",
    "config": {
      "path": "tests/packages"
    }
  },
  "event-storage": {
    "type": "my-event-storage",
    "config": {
      "url": "xxx://localhost:12345"
    }
  },
  "state-store": {
    "type": "my-state-store",
    "config": {
      "url": "xxx://localhost:12345"
    }
  },
  "runtimes": [
    {
      "type": "my-runtime-1",
      "config": {
        "my-config": "xxx"
      }
    },
    {
      "type": "my-runtime-2",
      "config": {
        "my-config": "xxx"
      }
    }
  ]
}`
				return createTempFile(t, content, "*.json")
			},
		},
		{
			name:     "InvalidYAMLFormat",
			filePath: "invalid.yaml",
			setup: func() (string, func()) {
				content := `listen-address: ":17300
		enable-tls: true`
				return createTempFile(t, content, "*.yaml")
			},
			wantErr:     true,
			errContains: "unexpected end of stream",
		},
		{
			name:     "InvalidJSONFormat",
			filePath: "invalid.json",
			setup: func() (string, func()) {
				content := `{"listen-address": ":17300,}`
				return createTempFile(t, content, "*.json")
			},
			wantErr:     true,
			errContains: "unexpected end of JSON input",
		},
		{
			name:     "MissingRequiredFields",
			filePath: "missing_fields.yaml",
			setup: func() (string, func()) {
				content := `enable-tls: false`
				return createTempFile(t, content, "*.yaml")
			},
			wantErr:     true,
			errContains: "listen-address",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup test environment
			viper.Reset()
			filePath, cleanup := tc.setup()
			defer cleanup()

			// Execute test
			cfg, err := LoadConfigFromFile(filePath)

			// Validate results
			if tc.wantErr {
				require.Error(t, err, "Expected error but got none")
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains,
						"Error message should contain %q", tc.errContains)
				}
				return
			}

			require.NoError(t, err, "Config loading failed")
			assertConfig(t, cfg)
		})
	}
}

// createTempFile creates a temporary test file with cleanup
func createTempFile(t *testing.T, content, pattern string) (string, func()) {
	tmpFile, err := os.CreateTemp("", pattern)
	require.NoError(t, err, "Failed to create temp file")

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err, "Failed to write test content")

	err = tmpFile.Close()
	require.NoError(t, err, "Failed to close temp file")

	return tmpFile.Name(), func() {
		os.Remove(tmpFile.Name())
	}
}

func assertConfig(t *testing.T, c *Config) {
	t.Helper()

	t.Run("BasicConfig", func(t *testing.T) {
		if c.ListenAddress != ":17300" {
			t.Errorf("expected listen-address :17300, got %q", c.ListenAddress)
		}
		if !c.EnableTLS {
			t.Error("expected enable-tls true")
		}
		if c.TLSCertFile != "/path/to/cert.pem" {
			t.Errorf("expected tls-cert-file /path/to/cert.pem, got %q", c.TLSCertFile)
		}
		if c.TLSKeyFile != "/path/to/key.pem" {
			t.Errorf("expected tls-key-file /path/to/key.pem, got %q", c.TLSKeyFile)
		}
	})

	testCases := []struct {
		name       string
		component  ComponentConfig
		expectType string
		configKeys map[string]interface{}
	}{
		{
			name:       "PackageLoader",
			component:  c.PackageLoader,
			expectType: "my-package-loader",
			configKeys: map[string]interface{}{
				"path": "tests/packages",
			},
		},
		{
			name:       "EventStorage",
			component:  c.EventStorage,
			expectType: "my-event-storage",
			configKeys: map[string]interface{}{
				"url": "xxx://localhost:12345",
			},
		},
		{
			name:       "StateStore",
			component:  c.StateStore,
			expectType: "my-state-store",
			configKeys: map[string]interface{}{
				"url": "xxx://localhost:12345",
			},
		},
		{
			name:       "Runtime",
			component:  c.Runtimes[0],
			expectType: "my-runtime-1",
			configKeys: map[string]interface{}{
				"my-config": "xxx",
			},
		},
		{
			name:       "Runtime",
			component:  c.Runtimes[1],
			expectType: "my-runtime-2",
			configKeys: map[string]interface{}{
				"my-config": "xxx",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.component.Type != tc.expectType {
				t.Errorf("expected type %q, got %q", tc.expectType, tc.component.Type)
			}

			for key, expected := range tc.configKeys {
				actual, exists := tc.component.Config[key]
				if !exists {
					t.Errorf("missing config key: %q", key)
					continue
				}

				var value interface{}
				switch v := actual.(type) {
				case string:
					value = v
				case int:
					value = v
				case bool:
					value = v
				default:
					t.Errorf("unsupported type %T for key %q", actual, key)
					continue
				}

				if value != expected {
					t.Errorf("config %q: expected %v (%T), got %v (%T)",
						key, expected, expected, value, value)
				}
			}
		})
	}
}
