use crate::{ByteArray, ByteArrayRef};
use serde::Serialize;
use std::collections::HashMap;
use temporalio_common::envconfig::{
    self, ClientConfig as CoreClientConfig, ClientConfigCodec as CoreClientConfigCodec,
    ClientConfigProfile as CoreClientConfigProfile, ClientConfigTLS as CoreClientConfigTLS,
    DataSource as CoreDataSource, LoadClientConfigOptions, LoadClientConfigProfileOptions, toml,
};

/// OrFail result for client config loading operations.
/// Either success or fail will be null, but never both.
/// If success is not null, it contains JSON-serialized client configuration data.
/// If fail is not null, it contains UTF-8 encoded error message.
/// The returned ByteArrays must be freed by the caller.
#[repr(C)]
pub struct ClientEnvConfigOrFail {
    pub success: *const ByteArray,
    pub fail: *const ByteArray,
}

/// OrFail result for client config profile loading operations.
/// Either success or fail will be null, but never both.
/// If success is not null, it contains JSON-serialized client configuration profile data.
/// If fail is not null, it contains UTF-8 encoded error message.
/// The returned ByteArrays must be freed by the caller.
#[repr(C)]
pub struct ClientEnvConfigProfileOrFail {
    pub success: *const ByteArray,
    pub fail: *const ByteArray,
}

/// Options for loading client configuration.
#[repr(C)]
pub struct ClientEnvConfigLoadOptions {
    pub path: ByteArrayRef,
    pub data: ByteArrayRef,
    pub config_file_strict: bool,
    pub env_vars: ByteArrayRef,
}

/// Options for loading a specific client configuration profile.
#[repr(C)]
pub struct ClientEnvConfigProfileLoadOptions {
    pub profile: ByteArrayRef,
    pub path: ByteArrayRef,
    pub data: ByteArrayRef,
    pub disable_file: bool,
    pub disable_env: bool,
    pub config_file_strict: bool,
    pub env_vars: ByteArrayRef,
}

// Wrapper types for JSON serialization
#[derive(Serialize)]
struct ClientEnvConfig {
    profiles: HashMap<String, ClientEnvConfigProfile>,
}

impl TryFrom<CoreClientConfig> for ClientEnvConfig {
    type Error = String;

    fn try_from(c: CoreClientConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            profiles: c
                .profiles
                .into_iter()
                .map(|(name, profile)| Ok((name, profile.try_into()?)))
                .collect::<Result<_, String>>()?,
        })
    }
}

#[derive(Serialize)]
struct ClientEnvConfigProfile {
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls: Option<ClientEnvConfigTLS>,
    #[serde(skip_serializing_if = "Option::is_none")]
    codec: Option<ClientEnvConfigCodec>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    grpc_meta: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    ext: HashMap<String, serde_json::Map<String, serde_json::Value>>,
}

impl TryFrom<CoreClientConfigProfile> for ClientEnvConfigProfile {
    type Error = String;

    fn try_from(c: CoreClientConfigProfile) -> Result<Self, Self::Error> {
        Ok(Self {
            address: c.address,
            namespace: c.namespace,
            api_key: c.api_key,
            tls: c.tls.map(TryInto::try_into).transpose()?,
            codec: c.codec.map(Into::into),
            grpc_meta: c.grpc_meta,
            ext: c
                .ext
                .into_iter()
                .map(|(name, table)| (name, toml_table_to_json(table)))
                .collect(),
        })
    }
}

fn toml_table_to_json(table: toml::Table) -> serde_json::Map<String, serde_json::Value> {
    table
        .into_iter()
        .map(|(k, v)| (k, toml_value_to_json(v)))
        .collect()
}

// Serde would serialize TOML datetimes as `{"$__toml_private_datetime": "..."}`, a wrapper the toml crate uses
// internally, so values are converted explicitly. JSON has no datetime type, so datetimes become ISO 8601 strings
// (e.g. `1979-05-27T07:32:00Z`, or `1979-05-27` for a local date).
fn toml_value_to_json(value: toml::Value) -> serde_json::Value {
    match value {
        toml::Value::String(s) => s.into(),
        toml::Value::Integer(i) => i.into(),
        // Non-finite floats have no JSON representation and become null
        toml::Value::Float(f) => f.into(),
        toml::Value::Boolean(b) => b.into(),
        toml::Value::Datetime(dt) => dt.to_string().into(),
        toml::Value::Array(a) => a.into_iter().map(toml_value_to_json).collect(),
        toml::Value::Table(t) => toml_table_to_json(t).into(),
    }
}

#[derive(Serialize)]
struct ClientEnvConfigTLS {
    #[serde(skip_serializing_if = "Option::is_none")]
    disabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_ca_cert: Option<DataSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_cert: Option<DataSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_key: Option<DataSource>,
}

impl TryFrom<CoreClientConfigTLS> for ClientEnvConfigTLS {
    type Error = String;

    fn try_from(c: CoreClientConfigTLS) -> Result<Self, Self::Error> {
        Ok(Self {
            disabled: c.disabled,
            server_name: c.server_name,
            server_ca_cert: c.server_ca_cert.map(TryInto::try_into).transpose()?,
            client_cert: c.client_cert.map(TryInto::try_into).transpose()?,
            client_key: c.client_key.map(TryInto::try_into).transpose()?,
        })
    }
}

#[derive(Serialize)]
struct ClientEnvConfigCodec {
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<String>,
}

impl From<CoreClientConfigCodec> for ClientEnvConfigCodec {
    fn from(c: CoreClientConfigCodec) -> Self {
        Self {
            endpoint: c.endpoint,
            auth: c.auth,
        }
    }
}

#[derive(Serialize)]
struct DataSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Vec<u8>>,
}

impl TryFrom<CoreDataSource> for DataSource {
    type Error = String;

    fn try_from(c: CoreDataSource) -> Result<Self, Self::Error> {
        Ok(match c {
            CoreDataSource::Path(p) => Self {
                path: Some(p),
                data: None,
            },
            CoreDataSource::Data(d) => Self {
                path: None,
                data: Some(d),
            },
            _ => return Err("Unsupported envconfig data source".to_string()),
        })
    }
}

// Helper functions
fn parse_config_source(
    path: &ByteArrayRef,
    data: &ByteArrayRef,
) -> Result<Option<CoreDataSource>, String> {
    if !path.data.is_null() && path.size > 0 {
        Ok(Some(CoreDataSource::Path(path.to_string())))
    } else if !data.data.is_null() && data.size > 0 {
        Ok(Some(CoreDataSource::Data(data.to_vec())))
    } else {
        Ok(None)
    }
}

fn parse_env_vars(env_vars: &ByteArrayRef) -> Result<Option<HashMap<String, String>>, String> {
    if env_vars.data.is_null() || env_vars.size == 0 {
        return Ok(None);
    }

    let env_json = std::str::from_utf8(env_vars.to_slice())
        .map_err(|e| format!("Invalid env vars UTF-8: {e}"))?;

    serde_json::from_str(env_json)
        .map(Some)
        .map_err(|e| format!("Invalid env vars JSON: {e}"))
}

// Simple helper to handle serialization errors consistently
fn serialize_or_error<T: Serialize>(data: T) -> Result<*const ByteArray, *const ByteArray> {
    match serde_json::to_vec(&data) {
        Ok(json_bytes) => {
            let result = ByteArray::from_vec(json_bytes);
            Ok(result.into_raw())
        }
        Err(e) => {
            let err = ByteArray::from_utf8(format!("Failed to serialize: {e}"));
            Err(err.into_raw())
        }
    }
}

/// Load all client profiles from given sources.
/// Returns ClientConfigOrFail with either success JSON or error message.
/// The returned ByteArrays must be freed by the caller.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_env_config_load(
    options: *const ClientEnvConfigLoadOptions,
) -> ClientEnvConfigOrFail {
    if options.is_null() {
        let err = ByteArray::from_utf8("Options cannot be null".to_string());
        return ClientEnvConfigOrFail {
            success: std::ptr::null(),
            fail: err.into_raw(),
        };
    }

    let result = || -> Result<ClientEnvConfig, String> {
        let opts = unsafe { &*options };
        let env_vars_map = parse_env_vars(&opts.env_vars)?;

        let load_options = LoadClientConfigOptions::builder()
            .maybe_config_source(parse_config_source(&opts.path, &opts.data)?)
            .config_file_strict(opts.config_file_strict)
            .build();

        let core_config = envconfig::load_client_config(load_options, env_vars_map.as_ref())
            .map_err(|e| e.to_string())?;

        core_config.try_into()
    };

    match result() {
        Ok(data) => match serialize_or_error(data) {
            Ok(success) => ClientEnvConfigOrFail {
                success,
                fail: std::ptr::null(),
            },
            Err(fail) => ClientEnvConfigOrFail {
                success: std::ptr::null(),
                fail,
            },
        },
        Err(e) => {
            let err = ByteArray::from_utf8(e);
            ClientEnvConfigOrFail {
                success: std::ptr::null(),
                fail: err.into_raw(),
            }
        }
    }
}

/// Load a single client profile from given sources with env overrides.
/// Returns ClientConfigProfileOrFail with either success JSON or error message.
/// The returned ByteArrays must be freed by the caller.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_env_config_profile_load(
    options: *const ClientEnvConfigProfileLoadOptions,
) -> ClientEnvConfigProfileOrFail {
    if options.is_null() {
        let err = ByteArray::from_utf8("Options cannot be null".to_string());
        return ClientEnvConfigProfileOrFail {
            success: std::ptr::null(),
            fail: err.into_raw(),
        };
    }

    let result = || -> Result<ClientEnvConfigProfile, String> {
        let opts = unsafe { &*options };

        let profile_name = if !opts.profile.data.is_null() && opts.profile.size > 0 {
            Some(opts.profile.to_string())
        } else {
            None
        };

        let config_source = parse_config_source(&opts.path, &opts.data)?;
        let env_vars_map = parse_env_vars(&opts.env_vars)?;

        let load_options = LoadClientConfigProfileOptions::builder()
            .maybe_config_source(config_source)
            .maybe_config_file_profile(profile_name)
            .config_file_strict(opts.config_file_strict)
            .disable_file(opts.disable_file)
            .disable_env(opts.disable_env)
            .build();

        let profile = envconfig::load_client_config_profile(load_options, env_vars_map.as_ref())
            .map_err(|e| e.to_string())?;

        profile.try_into()
    };

    match result() {
        Ok(data) => match serialize_or_error(data) {
            Ok(success) => ClientEnvConfigProfileOrFail {
                success,
                fail: std::ptr::null(),
            },
            Err(fail) => ClientEnvConfigProfileOrFail {
                success: std::ptr::null(),
                fail,
            },
        },
        Err(e) => {
            let err = ByteArray::from_utf8(e);
            ClientEnvConfigProfileOrFail {
                success: std::ptr::null(),
                fail: err.into_raw(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_ext_serializes_to_json() {
        let toml_str = r#"
[profile.default]
address = "localhost:7233"

[profile.default.ext.my_ext]
str = "value"
int = 5
float = 1.5
bool = true
datetime = 1979-05-27T07:32:00Z
local_date = 1979-05-27
array = [1, "two"]
nested = { key = "nested_value" }
"#;
        let profile = envconfig::load_client_config_profile(
            LoadClientConfigProfileOptions::builder()
                .config_source(CoreDataSource::Data(toml_str.as_bytes().to_vec()))
                .disable_env(true)
                .build(),
            None,
        )
        .unwrap();
        let json =
            serde_json::to_value(ClientEnvConfigProfile::try_from(profile).unwrap()).unwrap();
        assert_eq!(
            json["ext"],
            serde_json::json!({
                "my_ext": {
                    "str": "value",
                    "int": 5,
                    "float": 1.5,
                    "bool": true,
                    "datetime": "1979-05-27T07:32:00Z",
                    "local_date": "1979-05-27",
                    "array": [1, "two"],
                    "nested": { "key": "nested_value" },
                }
            })
        );
    }

    #[test]
    fn profile_without_ext_omits_field() {
        let profile = CoreClientConfigProfile::default();
        let json =
            serde_json::to_value(ClientEnvConfigProfile::try_from(profile).unwrap()).unwrap();
        assert!(json.get("ext").is_none());
    }
}
