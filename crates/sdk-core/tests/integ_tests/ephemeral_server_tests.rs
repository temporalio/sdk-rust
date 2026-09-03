use crate::common::{
    INTEG_CLIENT_IDENTITY, INTEG_CLIENT_NAME, INTEG_CLIENT_VERSION, NAMESPACE, rand_6_chars,
};
use futures_util::{TryStreamExt, stream};
use std::time::{SystemTime, UNIX_EPOCH};
use temporalio_client::{
    Connection, ConnectionOptions, WorkflowStartOptions,
    grpc::{TestService, WorkflowService},
};
use temporalio_common::protos::temporal::api::workflowservice::v1::DescribeNamespaceRequest;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    Runtime, Worker, WorkerOptions, WorkflowContext, WorkflowResult,
    testing::{LocalWorkflowEnvironmentOptions, WorkflowEnvironment},
};
use temporalio_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralExeVersion, EphemeralServer, TemporalDevServerConfig,
    default_cached_download,
};
use tonic::IntoRequest;
use url::Url;

#[workflow]
#[derive(Default)]
struct TestEnvironmentWorkflow;

#[workflow_methods]
impl TestEnvironmentWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_workflow_environment_local() {
    let env = WorkflowEnvironment::start_local(LocalWorkflowEnvironmentOptions::default())
        .await
        .unwrap();
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let worker_options = WorkerOptions::new(format!("test-env-{}", rand_6_chars()))
        .register_workflow::<TestEnvironmentWorkflow>()
        .unwrap()
        .build();
    let task_queue = worker_options.task_queue.clone();
    let mut worker = Worker::new(&runtime, env.client().clone(), worker_options).unwrap();
    let shutdown = worker.shutdown_handle();
    let handle = env
        .client()
        .start_workflow(
            TestEnvironmentWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, format!("test-env-{}", rand_6_chars())).build(),
        )
        .await
        .unwrap();

    let (worker_result, ()) = tokio::join!(worker.run(), async move {
        handle.get_result(Default::default()).await.unwrap();
        shutdown();
    });
    worker_result.unwrap();
    env.shutdown().await.unwrap();
}

#[tokio::test]
async fn temporal_cli_default() {
    let config = TemporalDevServerConfig::builder()
        .exe(default_cached_download())
        .build();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;

    // Make sure process is there on start and not there after shutdown
    let pid = sysinfo::Pid::from_u32(server.child_process_id().unwrap());
    assert!(sysinfo::System::new_all().process(pid).is_some());
    server.shutdown().await.unwrap();
    assert!(sysinfo::System::new_all().process(pid).is_none());
}

#[tokio::test]
async fn temporal_cli_fixed() {
    let config = TemporalDevServerConfig::builder()
        .exe(fixed_cached_download("v1.2.0"))
        .build();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn temporal_cli_shutdown_port_reuse() {
    // Start, test shutdown, do again immediately on same port to ensure we can
    // reuse after shutdown
    let config = TemporalDevServerConfig::builder()
        .exe(default_cached_download())
        .port(10123)
        .build();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
}

// This test will fail on Linux until https://github.com/temporalio/cli/pull/564
// gets released (presumably in 0.12.1). To test locally, build CLI manually
// and use that specific binary instead:
// ```
//   .exe(EphemeralExe::ExistingPath(
//       "/usr/local/bin/temporal".to_string(),
//   ))
// ```
#[tokio::test]
#[ignore]
async fn temporal_cli_concurrent_starts() -> Result<(), Box<dyn std::error::Error>> {
    stream::iter((0..80).map(|_| {
        Ok::<TemporalDevServerConfig, Box<dyn std::error::Error>>(
            TemporalDevServerConfig::builder()
                .exe(default_cached_download())
                .build(),
        )
    }))
    .try_for_each_concurrent(8, |config| async move {
        let mut server = config.start_server().await?;
        server.shutdown().await?;
        Ok(())
    })
    .await?;

    Ok(())
}

// Test server downloads aren't available for arm linux
#[cfg(not(all(target_os = "linux", any(target_arch = "arm", target_arch = "aarch64"))))]
mod test_server {
    use super::*;
    use temporalio_sdk_core::ephemeral_server::TestServerConfig;

    #[tokio::test]
    async fn test_server_default() {
        let config = TestServerConfig::builder()
            .exe(default_cached_download())
            .build();
        let mut server = config.start_server().await.unwrap();
        assert_ephemeral_server(&server).await;
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_server_fixed() {
        let config = TestServerConfig::builder()
            .exe(fixed_cached_download("v1.16.0"))
            .build();
        let mut server = config.start_server().await.unwrap();
        assert_ephemeral_server(&server).await;
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_server_shutdown_port_reuse() {
        // Start, test shutdown, do again immediately on same port to ensure we can
        // reuse after shutdown
        let config = TestServerConfig::builder()
            .exe(default_cached_download())
            .port(10124)
            .build();
        let mut server = config.start_server().await.unwrap();
        assert_ephemeral_server(&server).await;
        server.shutdown().await.unwrap();
        let mut server = config.start_server().await.unwrap();
        assert_ephemeral_server(&server).await;
        server.shutdown().await.unwrap();
    }
}

fn fixed_cached_download(version: &str) -> EphemeralExe {
    EphemeralExe::CachedDownload {
        version: EphemeralExeVersion::Fixed(version.to_string()),
        dest_dir: None,
        ttl: None,
    }
}

async fn assert_ephemeral_server(server: &EphemeralServer) {
    // Connect and describe namespace
    let connection_opts =
        ConnectionOptions::new(Url::try_from(&*format!("http://{}", server.target)).unwrap())
            .identity(INTEG_CLIENT_IDENTITY.to_string())
            .client_name(INTEG_CLIENT_NAME.to_string())
            .client_version(INTEG_CLIENT_VERSION.to_string())
            .build();
    let mut connection = Connection::connect(connection_opts).await.unwrap();
    let resp = connection
        .describe_namespace(
            DescribeNamespaceRequest {
                namespace: NAMESPACE.to_string(),
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .unwrap();
    assert!(resp.into_inner().namespace_info.unwrap().name == "default");

    // If it has test service, make sure we can use it too
    if server.has_test_service {
        let resp = connection
            .get_current_time(().into_request())
            .await
            .unwrap();
        // Make sure it's within 5 mins of now
        let resp_seconds = resp.get_ref().time.as_ref().unwrap().seconds as u64;
        let curr_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(curr_seconds - 300 < resp_seconds && curr_seconds + 300 > resp_seconds);
    }
}
