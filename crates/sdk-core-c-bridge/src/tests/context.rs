use crate::{
    client::{
        ClientDnsLoadBalancingOptions, ClientHttpConnectProxyOptions, ClientKeepAliveOptions,
        ClientRetryOptions, ClientTlsOptions, Connection, GrpcMetadataHolder, RpcCallOptions,
        temporal_core_client_connect, temporal_core_client_free, temporal_core_client_rpc_call,
    },
    runtime::{
        Runtime, RuntimeOptions, RuntimeOrFail, temporal_core_byte_array_free,
        temporal_core_runtime_free, temporal_core_runtime_new,
    },
    testing::{
        DevServerOptions, EphemeralServer, TestServerOptions, temporal_core_ephemeral_server_free,
        temporal_core_ephemeral_server_shutdown, temporal_core_ephemeral_server_start_dev_server,
    },
};

use crate::{
    ByteArray, ByteArrayRef,
    tests::utils::{
        OwnedRpcCallOptions, RpcCallError, byte_array_to_string, byte_array_to_vec, pointer_or_null,
    },
};
use anyhow::anyhow;
use std::{
    any::Any,
    collections::HashMap,
    panic::{AssertUnwindSafe, UnwindSafe},
    ptr::NonNull,
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak},
    time::Duration,
};
use temporalio_client::ConnectionOptions;
use temporalio_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralExeVersion, TemporalDevServerConfig,
};

#[derive(Debug)]
enum ContextOperationState {
    Available,
    InProgress,
    CallbackOk(Option<Box<dyn Any + Send>>),
    CallbackError(anyhow::Error),
}

struct InnerContext {
    is_disposed: bool,
    operation_state: ContextOperationState,
    runtime: *mut Runtime,
    ephemeral_server: *mut EphemeralServer,
    ephemeral_server_target: String,
    client: *mut Connection,
}

unsafe impl Send for InnerContext {}

impl Default for InnerContext {
    fn default() -> Self {
        Self {
            is_disposed: false,
            operation_state: ContextOperationState::Available,
            runtime: std::ptr::null_mut(),
            ephemeral_server: std::ptr::null_mut(),
            ephemeral_server_target: String::new(),
            client: std::ptr::null_mut(),
        }
    }
}

pub struct Context {
    inner: Mutex<InnerContext>,
    condvar: Condvar,
}

impl Context {
    fn new(inner: InnerContext) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(inner),
            condvar: Condvar::new(),
        })
    }

    pub fn with(func: impl FnOnce(&Arc<Self>) + UnwindSafe) {
        let context = Self::new(Default::default());
        let func_panic = std::panic::catch_unwind(|| func(&context));
        let dispose_panic = std::panic::catch_unwind(|| context.dispose());
        if let Err(e) = func_panic.and(dispose_panic) {
            std::panic::resume_unwind(e);
        }
    }

    #[allow(dead_code)]
    pub fn runtime(&self) -> anyhow::Result<Option<NonNull<Runtime>>> {
        Ok(NonNull::new(self.wait_for_available()?.runtime))
    }

    #[allow(dead_code)]
    pub fn ephemeral_server(&self) -> anyhow::Result<Option<NonNull<EphemeralServer>>> {
        Ok(NonNull::new(self.wait_for_available()?.ephemeral_server))
    }

    pub fn ephemeral_server_target(&self) -> anyhow::Result<Option<String>> {
        let guard = self.wait_for_available()?;
        Ok((!guard.ephemeral_server.is_null()).then(|| guard.ephemeral_server_target.clone()))
    }

    #[allow(dead_code)]
    pub fn client(&self) -> anyhow::Result<Option<NonNull<Connection>>> {
        Ok(NonNull::new(self.wait_for_available()?.client))
    }

    pub fn dispose(self: Arc<Context>) -> anyhow::Result<()> {
        let inner = {
            let mut guard = self
                .inner
                .lock()
                .and_then(|lock| {
                    self.condvar.wait_while(lock, |inner| {
                        !matches!(inner.operation_state, ContextOperationState::Available)
                    })
                })
                .unwrap_or_else(PoisonError::into_inner);
            if guard.is_disposed {
                return Err(anyhow!("Context already disposed"));
            }
            guard.is_disposed = true;
            std::mem::take(&mut *guard)
        };

        if !inner.client.is_null() {
            temporal_core_client_free(inner.client);
        }
        if !inner.ephemeral_server.is_null() {
            // Creating new non-disposed context to execute server shutdown in
            let _ = Self::new(InnerContext {
                runtime: inner.runtime,
                ephemeral_server: inner.ephemeral_server,
                ..Default::default()
            })
            .ephemeral_server_shutdown();
        }
        if !inner.runtime.is_null() {
            temporal_core_runtime_free(inner.runtime);
        }

        Ok(())
    }

    pub fn runtime_new(self: &Arc<Self>) -> anyhow::Result<()> {
        let mut guard = self.wait_for_available()?;
        if !guard.runtime.is_null() {
            return Err(anyhow!("Runtime already exists"));
        }

        let RuntimeOrFail { runtime, fail } = temporal_core_runtime_new(&RuntimeOptions {
            telemetry: std::ptr::null(),
            worker_heartbeat_interval_millis: 0,
        });

        if let Some(fail) = byte_array_to_string(runtime, fail) {
            let runtime_is_null = if runtime.is_null() {
                "(!!! runtime is null) "
            } else {
                temporal_core_runtime_free(runtime);
                ""
            };
            Err(anyhow!("Runtime creation failed: {runtime_is_null}{fail}"))
        } else if runtime.is_null() {
            Err(anyhow!("Runtime creation failed: runtime is null"))
        } else {
            guard.runtime = runtime;
            Ok(())
        }
    }

    pub fn start_dev_server(
        self: &Arc<Self>,
        config: Box<TemporalDevServerConfig>,
    ) -> anyhow::Result<()> {
        let extra_args = config.extra_args.join("\n");

        let mut test_server_options = Box::new(TestServerOptions {
            existing_path: ByteArrayRef::empty(),
            sdk_name: ByteArrayRef::empty(),
            sdk_version: ByteArrayRef::empty(),
            download_version: ByteArrayRef::empty(),
            download_dest_dir: ByteArrayRef::empty(),
            port: config.port.unwrap_or(0),
            extra_args: extra_args.as_str().into(),
            download_ttl_seconds: 0,
        });

        match config.exe {
            EphemeralExe::ExistingPath(ref path) => {
                test_server_options.existing_path = path.as_str().into();
            }
            EphemeralExe::CachedDownload {
                ref version,
                ref dest_dir,
                ref ttl,
            } => {
                test_server_options.download_dest_dir = dest_dir.as_deref().into();
                test_server_options.download_ttl_seconds =
                    ttl.as_ref().map(Duration::as_secs).unwrap_or(0);
                match version {
                    EphemeralExeVersion::SDKDefault {
                        sdk_name,
                        sdk_version,
                    } => {
                        test_server_options.download_version = "default".into();
                        test_server_options.sdk_name = sdk_name.as_str().into();
                        test_server_options.sdk_version = sdk_version.as_str().into();
                    }
                    EphemeralExeVersion::Fixed(version) => {
                        test_server_options.download_version = version.as_str().into();
                    }
                }
            }
        }

        let dev_server_options = Box::new(DevServerOptions {
            test_server: &*test_server_options,
            namespace: config.namespace.as_str().into(),
            ip: config.ip.as_str().into(),
            database_filename: config.db_filename.as_deref().into(),
            ui: config.ui,
            ui_port: config.ui_port.unwrap_or(0),
            log_format: config.log.0.as_str().into(),
            log_level: config.log.1.as_str().into(),
        });

        let dev_server_options_ptr = &*dev_server_options as *const _;
        let user_data = Box::into_raw(Box::new(CallbackUserData {
            data: (),
            context: Arc::downgrade(self),
            _allocations: Box::new((config, extra_args, test_server_options, dev_server_options)),
        })) as *mut libc::c_void;

        let runtime = {
            let mut guard = self.wait_for_available()?;
            if !guard.ephemeral_server.is_null() {
                return Err(anyhow!("Ephemeral server already started"));
            }
            guard.operation_state = ContextOperationState::InProgress;
            guard.runtime
        };

        temporal_core_ephemeral_server_start_dev_server(
            runtime,
            dev_server_options_ptr,
            user_data,
            ephemeral_server_start_callback,
        );
        self.wait_for_operation()
    }

    pub fn ephemeral_server_shutdown(self: &Arc<Context>) -> anyhow::Result<()> {
        let server = {
            let mut guard = self.wait_for_available()?;
            let server = guard.ephemeral_server;
            if server.is_null() {
                return Err(anyhow!("Ephemeral server is null"));
            }
            guard.ephemeral_server = std::ptr::null_mut();
            guard.ephemeral_server_target = String::new();
            guard.operation_state = ContextOperationState::InProgress;
            server
        };

        let user_data = Box::into_raw(Box::new(CallbackUserData {
            data: server,
            context: Arc::downgrade(self),
            _allocations: Box::new(()),
        })) as *mut libc::c_void;
        temporal_core_ephemeral_server_shutdown(
            server,
            user_data,
            ephemeral_server_shutdown_callback,
        );
        self.wait_for_operation()
    }

    pub fn client_connect(self: &Arc<Self>, options: Box<ConnectionOptions>) -> anyhow::Result<()> {
        Self::client_connect_with_override(self, options, None, std::ptr::null_mut())
    }

    pub fn client_connect_with_override(
        self: &Arc<Self>,
        options: Box<ConnectionOptions>,
        grpc_override_callback: crate::client::ClientGrpcOverrideCallback,
        grpc_override_callback_user_data: *mut libc::c_void,
    ) -> anyhow::Result<()> {
        let metadata: Option<GrpcMetadataHolder> = options.headers.as_ref().map(Into::into);

        let binary_metadata: Option<GrpcMetadataHolder> =
            options.binary_headers.as_ref().map(Into::into);

        let tls_options = options.tls_options.as_ref().map(|tls_cfg| {
            let client_tls_cfg = tls_cfg.client_tls_options.as_ref();
            Box::new(ClientTlsOptions {
                server_root_ca_cert: tls_cfg.server_root_ca_cert.as_deref().into(),
                domain: tls_cfg.domain.as_deref().into(),
                client_cert: client_tls_cfg.map(|c| c.client_cert.as_slice()).into(),
                client_private_key: client_tls_cfg
                    .map(|c| c.client_private_key.as_slice())
                    .into(),
            })
        });

        let retry_options = Box::new(ClientRetryOptions {
            initial_interval_millis: options.retry_options.initial_interval.as_millis() as u64,
            randomization_factor: options.retry_options.randomization_factor,
            multiplier: options.retry_options.multiplier,
            max_interval_millis: options.retry_options.max_interval.as_millis() as u64,
            max_elapsed_time_millis: options
                .retry_options
                .max_elapsed_time
                .as_ref()
                .map(Duration::as_millis)
                .unwrap_or(0) as u64,
            max_retries: options.retry_options.max_retries,
        });

        let keep_alive_options = options.keep_alive.as_ref().map(|keep_alive| {
            Box::new(ClientKeepAliveOptions {
                interval_millis: keep_alive.interval.as_millis() as u64,
                timeout_millis: keep_alive.timeout.as_millis() as u64,
            })
        });

        let proxy_options = options.http_connect_proxy.as_ref().map(|proxy| {
            let (username, password) = match &proxy.basic_auth {
                Some((username, password)) => (username.as_str().into(), password.as_str().into()),
                None => (ByteArrayRef::empty(), ByteArrayRef::empty()),
            };

            Box::new(ClientHttpConnectProxyOptions {
                target_host: proxy.target_addr.as_str().into(),
                username,
                password,
            })
        });

        let dns_load_balancing_options = options.dns_load_balancing.as_ref().map(|dns| {
            Box::new(ClientDnsLoadBalancingOptions {
                resolution_interval_millis: dns.resolution_interval.as_millis() as u64,
            })
        });

        let client_options = Box::new(crate::client::ConnectionOptions {
            target_url: options.target.as_str().into(),
            client_name: options.get_client_name().into(),
            client_version: options.get_client_version().into(),
            metadata: metadata.as_ref().into(),
            binary_metadata: binary_metadata.as_ref().into(),
            api_key: options.api_key.as_deref().into(),
            identity: options.identity.as_str().into(),
            tls_options: pointer_or_null(tls_options.as_deref()),
            retry_options: &*retry_options,
            keep_alive_options: pointer_or_null(keep_alive_options.as_deref()),
            http_connect_proxy_options: pointer_or_null(proxy_options.as_deref()),
            dns_load_balancing_options: pointer_or_null(dns_load_balancing_options.as_deref()),
            grpc_override_callback,
            grpc_override_callback_user_data,
            grpc_compression: match options.grpc_compression {
                temporalio_client::GrpcCompression::None => {
                    crate::client::ClientGrpcCompression::None
                }
                _ => crate::client::ClientGrpcCompression::Gzip,
            },
        });

        let client_options_ptr = &*client_options as *const _;
        let user_data = Box::into_raw(Box::new(CallbackUserData {
            data: (),
            context: Arc::downgrade(self),
            _allocations: Box::new((
                options,
                metadata,
                binary_metadata,
                tls_options,
                retry_options,
                keep_alive_options,
                proxy_options,
                dns_load_balancing_options,
                client_options,
            )),
        })) as *mut libc::c_void;

        let runtime = {
            let mut guard = self.wait_for_available()?;
            if !guard.client.is_null() {
                return Err(anyhow!("Client already exists"));
            }
            guard.operation_state = ContextOperationState::InProgress;
            guard.runtime
        };

        temporal_core_client_connect(
            runtime,
            client_options_ptr,
            user_data,
            client_connect_callback,
        );
        self.wait_for_operation()
    }

    pub fn rpc_call(
        self: &Arc<Self>,
        options: Box<OwnedRpcCallOptions>,
    ) -> anyhow::Result<Vec<u8>> {
        let c_options = Box::new(RpcCallOptions {
            service: options.service,
            rpc: options.rpc.as_str().into(),
            req: options.req.as_slice().into(),
            retry: options.retry,
            metadata: options.metadata.as_ref().into(),
            binary_metadata: options.binary_metadata.as_ref().into(),
            timeout_millis: options.timeout_millis,
            cancellation_token: options
                .cancellation_token
                .unwrap_or_else(std::ptr::null_mut),
        });

        let (runtime, client) = {
            let mut guard = self.wait_for_available()?;
            let client = guard.client;
            if client.is_null() {
                return Err(anyhow!("Client is null"));
            }
            guard.operation_state = ContextOperationState::InProgress;
            (guard.runtime, client)
        };

        let c_options_ptr = &*c_options as *const _;
        let user_data = Box::into_raw(Box::new(CallbackUserData {
            data: runtime,
            context: Arc::downgrade(self),
            _allocations: Box::new((options, c_options)),
        })) as *mut libc::c_void;

        temporal_core_client_rpc_call(client, c_options_ptr, user_data, rpc_call_callback);
        self.wait_for_operation_result().map(|r| *r)
    }

    fn wait_while(
        &self,
        condition: impl FnMut(&mut InnerContext) -> bool,
    ) -> anyhow::Result<MutexGuard<'_, InnerContext>> {
        self.inner
            .lock()
            .and_then(|lock| self.condvar.wait_while(lock, condition))
            .map_err(|_| anyhow!("Context mutex poisoned"))
    }

    fn wait_for_available(&self) -> anyhow::Result<MutexGuard<'_, InnerContext>> {
        let guard = self.wait_while(|inner| {
            !matches!(inner.operation_state, ContextOperationState::Available) && !inner.is_disposed
        })?;

        if guard.is_disposed {
            Err(anyhow!("Context already disposed"))
        } else {
            Ok(guard)
        }
    }

    /// First, runs provided function under context mutex lock. Then, asserts `operation_state` is
    /// `InProgress` and updates it. Finally, notifies the context condvar. Panic inside `func` or wrong
    /// `operation_state` will poison the mutex.
    fn complete_operation_catch_unwind(
        &self,
        func: impl FnOnce(&mut MutexGuard<InnerContext>) -> ContextOperationState,
    ) -> std::thread::Result<()> {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut guard = self.inner.lock().unwrap();
            let new_state = func(&mut guard);
            assert!(matches!(
                guard.operation_state,
                ContextOperationState::InProgress
            ));
            guard.operation_state = new_state;
        }));
        self.condvar.notify_all();
        result
    }

    /// Waits for operation state to become `CallbackOk` or `CallbackError`, then changes it to `Available`.
    /// Panics on wrong operation state or if `CallbackOk` contains a result.
    fn wait_for_operation(&self) -> anyhow::Result<()> {
        self.wait_for_operation_any()
            .map(|r| assert!(r.is_none(), "Expected empty callback result"))
    }

    /// Waits for operation state to become `CallbackOk` or `CallbackError`, then changes it to `Available`.
    /// Panics on wrong operation state or if `CallbackOk` has no result or the result is wrong type.
    fn wait_for_operation_result<T>(&self) -> anyhow::Result<Box<T>>
    where
        T: Send + 'static,
    {
        self.wait_for_operation_any()
            .map(|r| r.unwrap().downcast().unwrap())
    }

    /// Waits for operation state to become `CallbackOk` or `CallbackError`, then changes it to `Available`.
    /// Panics on wrong operation state.
    fn wait_for_operation_any(&self) -> anyhow::Result<Option<Box<dyn Any + Send>>> {
        let mut guard = self
            .wait_while(|inner| matches!(inner.operation_state, ContextOperationState::InProgress))
            .unwrap();
        match std::mem::replace(&mut guard.operation_state, ContextOperationState::Available) {
            ContextOperationState::CallbackOk(result) => Ok(result),
            ContextOperationState::CallbackError(e) => Err(e),
            other => panic!("Unexpected operation state {other:?}"),
        }
    }
}

struct CallbackUserData<T, C> {
    data: T,
    context: Weak<C>,
    _allocations: Box<dyn Any + 'static>,
}

extern "C" fn ephemeral_server_start_callback(
    user_data: *mut libc::c_void,
    mut server: *mut EphemeralServer,
    mut server_target: *const ByteArray,
    mut fail: *const ByteArray,
) {
    let user_data = unsafe { Box::from_raw(user_data as *mut CallbackUserData<(), Context>) };
    if let Some(context) = user_data.context.upgrade() {
        let _ = context.complete_operation_catch_unwind(|guard| {
            let server_target =
                byte_array_to_string(guard.runtime, std::mem::take(&mut server_target));
            let fail = byte_array_to_string(guard.runtime, std::mem::take(&mut fail));

            if let Some(fail) = fail {
                ContextOperationState::CallbackError(anyhow!(
                    "Ephemeral server start failed: {fail}"
                ))
            } else if server.is_null() {
                ContextOperationState::CallbackError(anyhow!(
                    "Ephemeral server start failed: server is null"
                ))
            } else if let Some(server_target) = server_target {
                guard.ephemeral_server = std::mem::take(&mut server);
                guard.ephemeral_server_target = server_target;
                ContextOperationState::CallbackOk(None)
            } else {
                ContextOperationState::CallbackError(anyhow!(
                    "Ephemeral server start failed: server target is null"
                ))
            }
        });
    }

    if !server_target.is_null() {
        temporal_core_byte_array_free(std::ptr::null_mut(), server_target);
    }
    if !fail.is_null() {
        temporal_core_byte_array_free(std::ptr::null_mut(), fail);
    }
    if !server.is_null() {
        // Creating new context as to not conflict with operation_status in original context
        let shutdown_context = Context::new(InnerContext {
            ephemeral_server: server,
            ..Default::default()
        });
        let _ = shutdown_context.ephemeral_server_shutdown();
    }
}

extern "C" fn ephemeral_server_shutdown_callback(
    user_data: *mut libc::c_void,
    mut fail: *const ByteArray,
) {
    let user_data = unsafe { Box::from_raw(user_data as *mut CallbackUserData<_, Context>) };
    temporal_core_ephemeral_server_free(user_data.data);

    if let Some(context) = user_data.context.upgrade() {
        let _ = context.complete_operation_catch_unwind(|guard| {
            if let Some(fail) = byte_array_to_string(guard.runtime, std::mem::take(&mut fail)) {
                ContextOperationState::CallbackError(anyhow!(
                    "Ephemeral server shutdown failed: {fail}"
                ))
            } else {
                ContextOperationState::CallbackOk(None)
            }
        });
    }

    if !fail.is_null() {
        temporal_core_byte_array_free(std::ptr::null_mut(), fail);
    }
}

extern "C" fn client_connect_callback(
    user_data: *mut libc::c_void,
    mut client: *mut Connection,
    mut fail: *const ByteArray,
) {
    let user_data = unsafe { Box::from_raw(user_data as *mut CallbackUserData<(), Context>) };
    if let Some(context) = user_data.context.upgrade() {
        let _ = context.complete_operation_catch_unwind(|guard| {
            if let Some(fail) = byte_array_to_string(guard.runtime, std::mem::take(&mut fail)) {
                ContextOperationState::CallbackError(anyhow!("Client connect failed: {fail}"))
            } else {
                guard.client = std::mem::take(&mut client);
                ContextOperationState::CallbackOk(None)
            }
        });
    }

    if !fail.is_null() {
        temporal_core_byte_array_free(std::ptr::null_mut(), fail);
    }
    if !client.is_null() {
        temporal_core_client_free(client);
    }
}

extern "C" fn rpc_call_callback(
    user_data: *mut libc::c_void,
    mut success: *const ByteArray,
    status_code: u32,
    mut failure_message: *const ByteArray,
    mut failure_details: *const ByteArray,
) {
    let user_data = unsafe { Box::from_raw(user_data as *mut CallbackUserData<(), Context>) };

    if let Some(context) = user_data.context.upgrade() {
        let _ = context.complete_operation_catch_unwind(|guard| {
            let success = byte_array_to_vec(guard.runtime, std::mem::take(&mut success));
            let mut failure_message =
                byte_array_to_string(guard.runtime, std::mem::take(&mut failure_message));
            let failure_details =
                byte_array_to_vec(guard.runtime, std::mem::take(&mut failure_details));

            if failure_message.is_none() {
                if status_code != 0 {
                    failure_message = Some("No message".into());
                } else if failure_details.is_some() {
                    failure_message = Some("No message, failure details not empty".into());
                } else if success.is_none() {
                    failure_message = Some("No message, response is null".into());
                }
            }

            if let Some(failure_message) = failure_message {
                ContextOperationState::CallbackError(
                    RpcCallError {
                        status_code,
                        message: failure_message,
                        details: failure_details,
                    }
                    .into(),
                )
            } else {
                ContextOperationState::CallbackOk(Some(Box::new(success.unwrap()) as _))
            }
        });
    }

    if !success.is_null() {
        temporal_core_byte_array_free(std::ptr::null_mut(), success);
    }
    if !failure_message.is_null() {
        temporal_core_byte_array_free(std::ptr::null_mut(), failure_message);
    }
    if !failure_details.is_null() {
        temporal_core_byte_array_free(std::ptr::null_mut(), failure_details);
    }
}

impl From<&HashMap<String, String>> for GrpcMetadataHolder {
    fn from(value: &HashMap<String, String>) -> GrpcMetadataHolder {
        let refs: Vec<Vec<u8>> = value
            .iter()
            .map(|(k, v)| format!("{k}\n{v}").into_bytes())
            .collect();

        GrpcMetadataHolder {
            data: refs.iter().map(ByteArrayRef::from).collect(),
            _allocations: refs,
        }
    }
}

impl From<&HashMap<String, Vec<u8>>> for GrpcMetadataHolder {
    fn from(value: &HashMap<String, Vec<u8>>) -> GrpcMetadataHolder {
        let refs: Vec<Vec<u8>> = value
            .iter()
            .map(|(k, v)| {
                let mut nv = format!("{k}\n").into_bytes();
                nv.extend(v);
                nv
            })
            .collect();

        GrpcMetadataHolder {
            data: refs.iter().map(ByteArrayRef::from).collect(),
            _allocations: refs,
        }
    }
}
