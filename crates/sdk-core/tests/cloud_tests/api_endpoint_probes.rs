use futures_util::{FutureExt, future::BoxFuture};
use std::{collections::HashSet, time::Duration};
use temporalio_client::{
    Client, RetryOptions,
    grpc::{HealthService, WorkflowService},
    request_extensions::RetryConfigForCall,
};
use temporalio_common::protos::grpc::health::v1::HealthCheckRequest;
use tonic::{Code, IntoRequest, Request, Response, Status};

pub(super) async fn run_all(gzip_client: &Client, uncompressed_client: &Client) -> Vec<String> {
    let mut failures = run_all_workflow_probes(gzip_client, uncompressed_client).await;

    let spec = ProbeSpec::normal();
    let gzip = call_health_check_probe(gzip_client.clone(), spec).await;
    let uncompressed = call_health_check_probe(uncompressed_client.clone(), spec).await;
    if let Err(failure) = compare_probe_outcomes("health_check", spec, &gzip, &uncompressed) {
        failures.push(failure);
    }

    failures
}

pub(super) fn assert_all_workflow_rpc_probes_covered() {
    let probe_names = workflow_probe_names();
    let proto_def = include_str!(
        "../../../protos/protos/api_upstream/temporal/api/workflowservice/v1/service.proto"
    );
    let implemented: HashSet<_> = probe_names
        .iter()
        .map(|name| name.replace('_', ""))
        .collect();
    let missing: Vec<_> = proto_def
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("rpc"))
        .filter_map(|line| {
            let method = line.strip_prefix("rpc ")?.split('(').next()?.trim();
            let normalized = method.to_lowercase();
            (!implemented.contains(&normalized)).then_some(method)
        })
        .collect();

    if !missing.is_empty() {
        panic!("Cloud endpoint probe list is missing WorkflowService RPCs: {missing:?}");
    }
}

#[derive(Clone, Copy)]
struct ProbeSpec {
    rpc_timeout: Duration,
    allow_deadline_exceeded: bool,
    allow_unimplemented: bool,
}

impl ProbeSpec {
    fn normal() -> Self {
        Self {
            rpc_timeout: Duration::from_secs(10),
            allow_deadline_exceeded: false,
            allow_unimplemented: false,
        }
    }

    fn short_deadline_ok() -> Self {
        Self {
            rpc_timeout: Duration::from_millis(1500),
            allow_deadline_exceeded: true,
            allow_unimplemented: false,
        }
    }
}

#[derive(Debug)]
enum ProbeOutcome {
    Ok,
    Status { code: Code, message: String },
    ClientTimeout,
}

impl ProbeOutcome {
    fn class(&self) -> ProbeOutcomeClass {
        match self {
            Self::Ok => ProbeOutcomeClass::Ok,
            Self::Status { code, .. } => ProbeOutcomeClass::Status(*code),
            Self::ClientTimeout => ProbeOutcomeClass::ClientTimeout,
        }
    }

    fn has_compression_error(&self) -> bool {
        let Self::Status { message, .. } = self else {
            return false;
        };
        let message = message.to_lowercase();
        message.contains("compress") || message.contains("gzip")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProbeOutcomeClass {
    Ok,
    Status(Code),
    ClientTimeout,
}

async fn call_workflow_probe<Resp, F>(client: Client, spec: ProbeSpec, call: F) -> ProbeOutcome
where
    Resp: Send + 'static,
    F: FnOnce(Client, ProbeSpec) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
{
    probe_outcome(spec, call(client, spec)).await
}

async fn call_health_check_probe(mut client: Client, spec: ProbeSpec) -> ProbeOutcome {
    probe_outcome(
        spec,
        async move {
            HealthService::check(
                &mut client,
                probe_request(HealthCheckRequest::default(), spec),
            )
            .await
        }
        .boxed(),
    )
    .await
}

fn probe_request<Req>(request: Req, spec: ProbeSpec) -> Request<Req> {
    let mut request = request.into_request();
    request.set_timeout(spec.rpc_timeout);
    request
        .extensions_mut()
        .insert(RetryConfigForCall(RetryOptions::no_retries()));
    request
}

async fn probe_outcome<Resp>(
    spec: ProbeSpec,
    call: BoxFuture<'static, Result<Response<Resp>, Status>>,
) -> ProbeOutcome
where
    Resp: Send + 'static,
{
    match tokio::time::timeout(spec.rpc_timeout + Duration::from_secs(5), call).await {
        Ok(Ok(_)) => ProbeOutcome::Ok,
        Ok(Err(status)) => ProbeOutcome::Status {
            code: status.code(),
            message: status.message().to_owned(),
        },
        Err(_) => ProbeOutcome::ClientTimeout,
    }
}

fn compare_probe_outcomes(
    name: &str,
    spec: ProbeSpec,
    gzip: &ProbeOutcome,
    uncompressed: &ProbeOutcome,
) -> Result<(), String> {
    if gzip.has_compression_error() {
        return Err(format!(
            "{name}: default gzip client returned compression-related error: {gzip:?}; \
             no-compression control returned {uncompressed:?}"
        ));
    }

    if gzip.class() != uncompressed.class() {
        return Err(format!(
            "{name}: default gzip outcome {gzip:?} differed from no-compression control \
             {uncompressed:?}"
        ));
    }

    match gzip.class() {
        ProbeOutcomeClass::Ok => Ok(()),
        ProbeOutcomeClass::Status(Code::DeadlineExceeded) if spec.allow_deadline_exceeded => Ok(()),
        ProbeOutcomeClass::Status(Code::Unimplemented) if !spec.allow_unimplemented => {
            Err(format!(
                "{name}: endpoint returned UNIMPLEMENTED for both default gzip and no-compression \
             clients: {gzip:?}"
            ))
        }
        ProbeOutcomeClass::Status(_) => Ok(()),
        ProbeOutcomeClass::ClientTimeout => Err(format!(
            "{name}: timed out in the client for both default gzip and no-compression clients"
        )),
    }
}

macro_rules! default_workflow_rpc_probes {
    ($probe:ident) => {
        $probe!(register_namespace);
        $probe!(describe_namespace);
        $probe!(list_namespaces);
        $probe!(update_namespace);
        $probe!(deprecate_namespace);
        $probe!(start_workflow_execution);
        $probe!(get_workflow_execution_history);
        $probe!(get_workflow_execution_history_reverse);
        $probe!(respond_workflow_task_completed);
        $probe!(respond_workflow_task_failed);
        $probe!(record_activity_task_heartbeat);
        $probe!(record_activity_task_heartbeat_by_id);
        $probe!(respond_activity_task_completed);
        $probe!(respond_activity_task_completed_by_id);
        $probe!(respond_activity_task_failed);
        $probe!(respond_activity_task_failed_by_id);
        $probe!(respond_activity_task_canceled);
        $probe!(respond_activity_task_canceled_by_id);
        $probe!(request_cancel_workflow_execution);
        $probe!(signal_workflow_execution);
        $probe!(signal_with_start_workflow_execution);
        $probe!(reset_workflow_execution);
        $probe!(terminate_workflow_execution);
        $probe!(delete_workflow_execution);
        $probe!(list_open_workflow_executions);
        $probe!(list_closed_workflow_executions);
        $probe!(list_workflow_executions);
        $probe!(list_archived_workflow_executions);
        $probe!(scan_workflow_executions);
        $probe!(count_workflow_executions);
        $probe!(create_workflow_rule);
        $probe!(describe_workflow_rule);
        $probe!(delete_workflow_rule);
        $probe!(list_workflow_rules);
        $probe!(trigger_workflow_rule);
        $probe!(get_search_attributes);
        $probe!(respond_query_task_completed);
        $probe!(reset_sticky_task_queue);
        $probe!(query_workflow);
        $probe!(describe_workflow_execution);
        $probe!(describe_task_queue);
        $probe!(get_cluster_info);
        $probe!(get_system_info);
        $probe!(list_task_queue_partitions);
        $probe!(create_schedule);
        $probe!(describe_schedule);
        $probe!(update_schedule);
        $probe!(patch_schedule);
        $probe!(list_schedule_matching_times);
        $probe!(delete_schedule);
        $probe!(list_schedules);
        $probe!(count_schedules);
        $probe!(update_worker_build_id_compatibility);
        $probe!(get_worker_build_id_compatibility);
        $probe!(get_worker_task_reachability);
        $probe!(start_batch_operation);
        $probe!(stop_batch_operation);
        $probe!(describe_batch_operation);
        $probe!(describe_deployment);
        $probe!(list_batch_operations);
        $probe!(list_deployments);
        $probe!(execute_multi_operation);
        $probe!(get_current_deployment);
        $probe!(get_deployment_reachability);
        $probe!(get_worker_versioning_rules);
        $probe!(update_worker_versioning_rules);
        $probe!(respond_nexus_task_completed);
        $probe!(respond_nexus_task_failed);
        $probe!(set_current_deployment);
        $probe!(shutdown_worker);
        $probe!(update_activity_options);
        $probe!(pause_activity);
        $probe!(unpause_activity);
        $probe!(update_workflow_execution_options);
        $probe!(reset_activity);
        $probe!(delete_worker_deployment);
        $probe!(delete_worker_deployment_version);
        $probe!(describe_worker_deployment);
        $probe!(describe_worker_deployment_version);
        $probe!(list_worker_deployments);
        $probe!(set_worker_deployment_current_version);
        $probe!(set_worker_deployment_ramping_version);
        $probe!(update_worker_deployment_version_metadata);
        $probe!(list_workers);
        $probe!(record_worker_heartbeat);
        $probe!(update_task_queue_config);
        $probe!(fetch_worker_config);
        $probe!(update_worker_config);
        $probe!(describe_worker);
        $probe!(set_worker_deployment_manager);
        $probe!(pause_workflow_execution);
        $probe!(unpause_workflow_execution);
        $probe!(start_activity_execution);
        $probe!(describe_activity_execution);
        $probe!(list_activity_executions);
        $probe!(count_activity_executions);
        $probe!(request_cancel_activity_execution);
        $probe!(terminate_activity_execution);
        $probe!(delete_activity_execution);
        $probe!(pause_activity_execution);
        $probe!(unpause_activity_execution);
        $probe!(reset_activity_execution);
        $probe!(update_activity_execution_options);
        $probe!(count_nexus_operation_executions);
        $probe!(create_worker_deployment);
        $probe!(create_worker_deployment_version);
        $probe!(delete_nexus_operation_execution);
        $probe!(describe_nexus_operation_execution);
        $probe!(list_nexus_operation_executions);
        $probe!(request_cancel_nexus_operation_execution);
        $probe!(start_nexus_operation_execution);
        $probe!(terminate_nexus_operation_execution);
        $probe!(update_worker_deployment_version_compute_config);
        $probe!(validate_worker_deployment_version_compute_config);
    };
}

macro_rules! workflow_rpc_probes {
    ($probe:ident) => {
        default_workflow_rpc_probes!($probe);
        $probe!(poll_workflow_task_queue, ProbeSpec::short_deadline_ok());
        $probe!(poll_activity_task_queue, ProbeSpec::short_deadline_ok());
        $probe!(update_workflow_execution, ProbeSpec::short_deadline_ok());
        $probe!(
            poll_workflow_execution_update,
            ProbeSpec::short_deadline_ok()
        );
        $probe!(poll_nexus_task_queue, ProbeSpec::short_deadline_ok());
        $probe!(poll_activity_execution, ProbeSpec::short_deadline_ok());
        $probe!(
            poll_nexus_operation_execution,
            ProbeSpec::short_deadline_ok()
        );
    };
}

async fn run_all_workflow_probes(
    gzip_client: &Client,
    uncompressed_client: &Client,
) -> Vec<String> {
    let mut failures = Vec::new();

    macro_rules! run_workflow_probe {
        ($method:ident) => {
            run_workflow_probe!($method, ProbeSpec::normal(), Default::default());
        };
        ($method:ident, $spec:expr) => {
            run_workflow_probe!($method, $spec, Default::default());
        };
        ($method:ident, $spec:expr, $request:expr) => {{
            let spec = $spec;
            let gzip =
                call_workflow_probe(gzip_client.clone(), spec, |mut client, spec| {
                    async move {
                        WorkflowService::$method(&mut client, probe_request($request, spec)).await
                    }
                    .boxed()
                })
                .await;
            let uncompressed =
                call_workflow_probe(uncompressed_client.clone(), spec, |mut client, spec| {
                    async move {
                        WorkflowService::$method(&mut client, probe_request($request, spec)).await
                    }
                    .boxed()
                })
                .await;
            if let Err(failure) =
                compare_probe_outcomes(stringify!($method), spec, &gzip, &uncompressed)
            {
                failures.push(failure);
            }
        }};
    }

    workflow_rpc_probes!(run_workflow_probe);
    failures
}

fn workflow_probe_names() -> Vec<&'static str> {
    let mut probe_names = Vec::new();
    macro_rules! collect_probe_name {
        ($method:ident) => {
            probe_names.push(stringify!($method));
        };
        ($method:ident, $spec:expr) => {
            probe_names.push(stringify!($method));
        };
        ($method:ident, $spec:expr, $request:expr) => {
            probe_names.push(stringify!($method));
        };
    }
    workflow_rpc_probes!(collect_probe_name);
    probe_names
}
