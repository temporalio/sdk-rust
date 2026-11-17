use crate::{
    ByteArray, CancellationToken,
    client::{GrpcMetadataHolder, RpcService},
    runtime::{Runtime, temporal_core_byte_array_free},
};
use std::ops::Deref;
use temporalio_client::ConnectionOptions;
use temporalio_sdk_core::ephemeral_server::{TemporalDevServerConfig, default_cached_download};
use url::Url;

pub(super) fn byte_array_to_vec(
    runtime: *mut Runtime,
    byte_array: *const ByteArray,
) -> Option<Vec<u8>> {
    (!byte_array.is_null()).then(|| {
        let ret = unsafe { &*byte_array }.as_ref().to_vec();
        temporal_core_byte_array_free(runtime, byte_array);
        ret
    })
}

pub(super) fn byte_array_to_string(
    runtime: *mut Runtime,
    byte_array: *const ByteArray,
) -> Option<String> {
    (!byte_array.is_null()).then(|| {
        let ret = unsafe { &*byte_array }.as_ref().to_string();
        temporal_core_byte_array_free(runtime, byte_array);
        ret
    })
}

pub(super) fn pointer_or_null<T>(x: Option<impl Deref<Target = T>>) -> *const T {
    x.map(|x| &*x as *const T).unwrap_or(std::ptr::null())
}

pub(super) fn default_server_config() -> TemporalDevServerConfig {
    TemporalDevServerConfig::builder()
        .exe(default_cached_download())
        .build()
}

pub(super) fn default_connection_options(target: &str) -> ConnectionOptions {
    ConnectionOptions::new(Url::parse(&format!("http://{target}")).unwrap())
        .client_name("core-c-bridge-tests".to_owned())
        .client_version("0.1.0".to_owned())
        .build()
}

pub(super) struct OwnedRpcCallOptions {
    pub service: RpcService,
    pub rpc: String,
    pub req: Vec<u8>,
    pub retry: bool,
    pub metadata: Option<GrpcMetadataHolder>,
    pub binary_metadata: Option<GrpcMetadataHolder>,
    pub timeout_millis: u32,
    pub cancellation_token: Option<*mut CancellationToken>,
}

unsafe impl Send for OwnedRpcCallOptions {}
unsafe impl Sync for OwnedRpcCallOptions {}

#[derive(thiserror::Error, Debug)]
#[error("{message} (status code {status_code})")]
pub(super) struct RpcCallError {
    pub status_code: u32,
    pub message: String,
    pub details: Option<Vec<u8>>,
}
