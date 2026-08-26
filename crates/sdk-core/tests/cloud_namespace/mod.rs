use anyhow::{Context, bail};
use std::{collections::HashMap, env, fs::OpenOptions, io::Write, time::Duration};
use temporalio_client::{Connection, ConnectionOptions, TlsOptions, grpc::CloudService};
use temporalio_common::protos::temporal::api::cloud::{
    cloudservice::v1::{
        CreateNamespaceRequest, DeleteNamespaceRequest, GetAsyncOperationRequest,
        GetNamespaceRequest,
    },
    namespace::v1::{MtlsAuthSpec, NamespaceSpec, ReplicaSpec},
    operation::v1::{AsyncOperation, async_operation},
};
use tokio::time::Instant;
use tonic::IntoRequest;
use url::Url;
use uuid::Uuid;

const CLOUD_OPS_ADDRESS: &str = "https://saas-api.tmprl.cloud:443";
const CLOUD_REGION: &str = "aws-ca-central-1";
const OPERATION_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const DEFAULT_POLL_DELAY: Duration = Duration::from_secs(10);
const MIN_POLL_DELAY: Duration = Duration::from_secs(1);

pub(crate) async fn create_namespace() -> anyhow::Result<()> {
    let namespace_name = format!(
        "sdk-rust-ci-{}-{}",
        required_env("GITHUB_RUN_ID")?,
        required_env("GITHUB_RUN_ATTEMPT")?
    );
    let accepted_client_ca = tokio::fs::read(required_env("TEMPORAL_CLOUD_CLIENT_CA_PATH")?)
        .await
        .context("failed to read the Cloud test CA certificate")?;
    let connection = cloud_connection().await?;
    let mut client = connection.cloud_service();
    let response = client
        .create_namespace(
            CreateNamespaceRequest {
                spec: Some(NamespaceSpec {
                    name: namespace_name,
                    retention_days: 1,
                    mtls_auth: Some(MtlsAuthSpec {
                        accepted_client_ca,
                        enabled: true,
                        ..Default::default()
                    }),
                    replicas: vec![ReplicaSpec {
                        region: CLOUD_REGION.to_owned(),
                    }],
                    ..Default::default()
                }),
                async_operation_id: Uuid::new_v4().to_string(),
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .context("failed to create Cloud namespace")?
        .into_inner();

    if response.namespace.is_empty() {
        bail!("create namespace response did not include a namespace");
    }
    append_github_output("namespace", &response.namespace)?;
    wait_for_operation(
        client.as_mut(),
        response
            .async_operation
            .context("create namespace response did not include an operation")?,
    )
    .await
}

pub(crate) async fn delete_namespace(namespace: String) -> anyhow::Result<()> {
    let connection = cloud_connection().await?;
    let mut client = connection.cloud_service();
    let existing = client
        .get_namespace(
            GetNamespaceRequest {
                namespace: namespace.clone(),
            }
            .into_request(),
        )
        .await
        .context("failed to read Cloud namespace before deletion")?
        .into_inner();
    let resource_version = existing
        .namespace
        .map(|namespace| namespace.resource_version)
        .filter(|version| !version.is_empty())
        .context("Cloud namespace did not include a resource version")?;
    let response = client
        .delete_namespace(
            DeleteNamespaceRequest {
                namespace,
                resource_version,
                async_operation_id: Uuid::new_v4().to_string(),
            }
            .into_request(),
        )
        .await
        .context("failed to delete Cloud namespace")?
        .into_inner();
    wait_for_operation(
        client.as_mut(),
        response
            .async_operation
            .context("delete namespace response did not include an operation")?,
    )
    .await
}

async fn cloud_connection() -> anyhow::Result<Connection> {
    let api_version = required_env("TEMPORAL_CLIENT_CLOUD_API_VERSION")?;
    let options = ConnectionOptions::new(Url::parse(CLOUD_OPS_ADDRESS)?)
        .api_key(required_env("TEMPORAL_CLIENT_CLOUD_API_KEY")?)
        .headers(HashMap::from([(
            "temporal-cloud-api-version".to_owned(),
            api_version,
        )]))
        .tls_options(TlsOptions::default())
        // The Cloud Operations endpoint does not expose the Workflow Service probe.
        .skip_get_system_info(true)
        .build();
    Connection::connect(options)
        .await
        .context("failed to connect to the Cloud Operations API")
}

async fn wait_for_operation(
    client: &mut dyn CloudService,
    operation: AsyncOperation,
) -> anyhow::Result<()> {
    if operation.id.is_empty() {
        bail!("Cloud operation response did not include an ID");
    }
    let operation_id = operation.id;
    let deadline = Instant::now() + OPERATION_TIMEOUT;

    loop {
        let operation = client
            .get_async_operation(
                GetAsyncOperationRequest {
                    async_operation_id: operation_id.clone(),
                }
                .into_request(),
            )
            .await?
            .into_inner()
            .async_operation
            .with_context(|| {
                format!("Cloud operation {operation_id} response did not include an operation")
            })?;
        let state = async_operation::State::try_from(operation.state)
            .with_context(|| format!("Cloud operation {operation_id} had an unknown state"))?;

        match state {
            async_operation::State::Fulfilled => return Ok(()),
            async_operation::State::Failed
            | async_operation::State::Cancelled
            | async_operation::State::Rejected => {
                bail!(
                    "Cloud operation {operation_id} {}: {}",
                    state.as_str_name(),
                    operation.failure_reason
                );
            }
            async_operation::State::Unspecified
            | async_operation::State::Pending
            | async_operation::State::InProgress => {}
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("timed out waiting for Cloud operation {operation_id}");
        }
        let delay = operation
            .check_duration
            .and_then(|duration| Duration::try_from(duration).ok())
            .unwrap_or(DEFAULT_POLL_DELAY)
            .max(MIN_POLL_DELAY)
            .min(remaining);
        tokio::time::sleep(delay).await;
    }
}

fn append_github_output(name: &str, value: &str) -> anyhow::Result<()> {
    let output_path = required_env("GITHUB_OUTPUT")?;
    let mut output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_path)
        .context("failed to open GITHUB_OUTPUT")?;
    writeln!(output, "{name}={value}").context("failed to write GITHUB_OUTPUT")
}

fn required_env(name: &str) -> anyhow::Result<String> {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .with_context(|| format!("missing required environment variable {name}"))
}
