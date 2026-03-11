// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Build `nats::Options` from URL and properties map.
//! Used by both NATS input and output protocols so that auth and other options
//! are not silently ignored.

use std::collections::HashMap;
use std::path::Path;

/// Get property by key, case-insensitive. Prefer exact match, then lowercase.
fn get_prop<'a>(props: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    props.get(key).map(String::as_str).or_else(|| {
        let key_lower = key.to_lowercase();
        props
            .iter()
            .find(|(k, _)| k.to_lowercase() == key_lower)
            .map(|(_, v)| v.as_str())
    })
}

/// Build NATS connection options from properties.
///
/// Supported properties (case-insensitive):
/// - `token`: auth token
/// - `user` / `username`: username (with `password` or `pass`)
/// - `password` / `pass`: password
/// - `name`: client connection name
/// - `tls_required`: "true" / "false"
/// - `client_cert`: path to client cert (with `client_key`)
/// - `client_key`: path to client key
/// - `credentials` / `creds`: path to .creds file
/// - `root_certificate` / `tls_ca`: path to root CA PEM
pub fn build_nats_options(properties: &HashMap<String, String>) -> nats::Options {
    let opts = if let Some(token) = get_prop(properties, "token") {
        nats::Options::with_token(token)
    } else if let (Some(u), Some(p)) = (
        get_prop(properties, "user").or_else(|| get_prop(properties, "username")),
        get_prop(properties, "password").or_else(|| get_prop(properties, "pass")),
    ) {
        nats::Options::with_user_pass(u, p)
    } else if let Some(creds) =
        get_prop(properties, "credentials").or_else(|| get_prop(properties, "creds"))
    {
        nats::Options::with_credentials(Path::new(creds))
    } else {
        nats::Options::new()
    };

    let opts = if let Some(name) = get_prop(properties, "name") {
        opts.with_name(name)
    } else {
        opts
    };

    let opts = if let Some(s) = get_prop(properties, "tls_required") {
        opts.tls_required(s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("1"))
    } else {
        opts
    };

    let opts = if let (Some(cert), Some(key)) = (
        get_prop(properties, "client_cert"),
        get_prop(properties, "client_key"),
    ) {
        opts.client_cert(Path::new(cert), Path::new(key))
    } else {
        opts
    };

    let opts = if let Some(ca) =
        get_prop(properties, "root_certificate").or_else(|| get_prop(properties, "tls_ca"))
    {
        opts.add_root_certificate(Path::new(ca))
    } else {
        opts
    };

    opts
}

/// Connect to NATS using URL and properties (auth, name, TLS, etc.).
pub fn nats_connect(
    url: &str,
    properties: &HashMap<String, String>,
) -> std::io::Result<nats::Connection> {
    build_nats_options(properties).connect(url)
}
