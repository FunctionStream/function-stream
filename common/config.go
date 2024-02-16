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

package common

// Config is a struct that holds the configuration for a function stream.
type Config struct {
	ListenAddr string // ListenAddr is the address that the function stream REST service will listen on.
	PulsarURL  string // PulsarURL is the URL of the Pulsar service. It's used for the pulsar_tube
	TubeType   string
}
