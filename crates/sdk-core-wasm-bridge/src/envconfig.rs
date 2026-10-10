use std::collections::HashMap;

use serde::Deserialize;
use serde_json::{Value, json};
use temporalio_common::envconfig::{
    self, ClientConfigProfile, DataSource, LoadClientConfigOptions, LoadClientConfigProfileOptions,
};

use crate::{BRIDGE_BUFFER_TOO_SMALL, checked_result_len, pack_result, read_bytes, write_result};

#[derive(Deserialize)]
struct LoadRequest {
    all: bool,
    profile: Option<String>,
    data: Option<String>,
    strict: bool,
    disable_file: bool,
    disable_env: bool,
    env: HashMap<String, String>,
}

/// Parse a host-provided config file and environment with Core's envconfig rules.
#[unsafe(no_mangle)]
pub(crate) extern "C" fn temporal_core_load_client_config(
    request_ptr: *const u8,
    request_len: usize,
    output_ptr: *mut u8,
    output_capacity: usize,
) -> i64 {
    let result = (|| {
        let request: LoadRequest = serde_json::from_slice(read_bytes(request_ptr, request_len))
            .map_err(|err| format!("invalid envconfig request: {err}"))?;
        let source = request.data.map(|data| DataSource::Data(data.into_bytes()));
        let value = if request.all {
            let config = envconfig::load_client_config(
                LoadClientConfigOptions::builder()
                    .maybe_config_source(source)
                    .config_file_strict(request.strict)
                    .build(),
                Some(&request.env),
            )
            .map_err(|err| err.to_string())?;
            json!({ "profiles": config.profiles.into_iter().map(|(name, profile)| (name, profile_value(profile))).collect::<HashMap<_, _>>() })
        } else {
            let profile = envconfig::load_client_config_profile(
                LoadClientConfigProfileOptions::builder()
                    .maybe_config_source(source)
                    .maybe_config_file_profile(request.profile)
                    .config_file_strict(request.strict)
                    .disable_file(request.disable_file)
                    .disable_env(request.disable_env)
                    .build(),
                Some(&request.env),
            )
            .map_err(|err| err.to_string())?;
            profile_value(profile)
        };
        serde_json::to_vec(&value).map_err(|err| err.to_string())
    })();
    if let Ok(ref output) = result
        && output.len() > output_capacity
    {
        return match checked_result_len(output.len(), "envconfig output") {
            Ok(len) => pack_result(BRIDGE_BUFFER_TOO_SMALL, len),
            Err(error) => write_result(output_ptr, output_capacity, || Err(error)),
        };
    }
    write_result(output_ptr, output_capacity, || result)
}

fn profile_value(profile: ClientConfigProfile) -> Value {
    let tls = profile.tls.map(|tls| {
        json!({
            "disabled": tls.disabled,
            "server_name": tls.server_name,
            "server_ca_cert": tls.server_ca_cert.map(source_value),
            "client_cert": tls.client_cert.map(source_value),
            "client_key": tls.client_key.map(source_value),
            "disable_host_verification": tls.disable_host_verification,
        })
    });
    json!({
        "address": profile.address,
        "namespace": profile.namespace,
        "api_key": profile.api_key,
        "tls": tls,
        "codec": profile.codec.map(|codec| json!({"endpoint": codec.endpoint, "auth": codec.auth})),
        "grpc_meta": profile.grpc_meta,
    })
}

fn source_value(source: DataSource) -> Value {
    match source {
        DataSource::Path(path) => json!({ "path": path }),
        DataSource::Data(data) => json!({ "data": data }),
        other => panic!("unsupported configuration data source: {other:?}"),
    }
}
