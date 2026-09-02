//! Tests that exercise the WASM workflow execution path. These are kept in a separate test binary
//! because they require `cargo component` and extra wasm targets to build the sample components,
//! which not every CI environment has installed.

#[allow(dead_code)]
mod common;

use crate::common::{CoreWfStarter, eventually};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::{UntypedWorkflow, WorkflowStartOptions};
use temporalio_common::{
    data_converters::{PayloadConverter, RawValue},
    protos::{
        constants::PATCH_MARKER_NAME,
        temporal::api::{
            enums::v1::{EventType, WorkflowTaskFailedCause},
            failure::v1::failure::FailureInfo,
            history::v1::{
                WorkflowTaskFailedEventAttributes,
                history_event::Attributes as HistoryEventAttributes,
            },
        },
    },
};
use temporalio_sdk::{PatchActivationCallback, WasmWorkflowComponent};
use tokio::process::Command;

const WASM_COMPONENT_ID: &str = "hello-workflow-component";
const WASM_WORKFLOW_TYPE: &str = "HelloWorkflow";
const WASM_INTERCEPTOR_WORKFLOW_TYPE: &str = "InterceptorWorkflow";
const WASM_PATCH_ACTIVATION_WORKFLOW_TYPE: &str = "PatchActivationWorkflow";
const WASM_TASK_FAILURE_WORKFLOW_TYPE: &str = "WasmTaskFailureWorkflow";
const WASM_PATCH_ID: &str = "wasm-patch-activation";

#[tokio::test]
async fn wasm_workflow_component_executes() {
    let component_path = build_wasm_hello_component().await;
    let component = WasmWorkflowComponent::from_file(WASM_COMPONENT_ID, component_path)
        .expect("sample WASM component should be loadable");
    run_string_workflow(
        "wasm_workflow_component_executes",
        component,
        WASM_WORKFLOW_TYPE,
        "Hello, workflow!",
    )
    .await;
}

// Mirrors `wasm_workflow_component_executes` but loads the component bytes into memory and
// registers via `from_bytes`, exercising the dynamic-blob loading path that callers will use
// for runtime-supplied components (e.g. fetched over the network rather than read from disk).
#[tokio::test]
async fn wasm_workflow_component_executes_from_bytes() {
    let component_path = build_wasm_hello_component().await;
    let bytes = tokio::fs::read(&component_path)
        .await
        .expect("WASM component file should be readable");
    let component = WasmWorkflowComponent::from_bytes(WASM_COMPONENT_ID, bytes)
        .expect("WASM component bytes should be loadable");
    run_string_workflow(
        "wasm_workflow_component_executes_from_bytes",
        component,
        WASM_WORKFLOW_TYPE,
        "Hello, workflow!",
    )
    .await;
}

#[tokio::test]
async fn wasm_workflow_interceptor_executes() {
    let component_path = build_wasm_interceptor_component().await;
    let component = WasmWorkflowComponent::from_file(WASM_COMPONENT_ID, component_path)
        .expect("interceptor WASM component should be loadable");
    run_string_workflow(
        "wasm_workflow_interceptor_executes",
        component,
        WASM_INTERCEPTOR_WORKFLOW_TYPE,
        "Hello, workflow-intercepted! [intercepted]",
    )
    .await;
}

#[tokio::test]
async fn wasm_patch_activation_callback_can_decline() {
    let calls = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(Mutex::new(None));
    let callback_calls = calls.clone();
    let callback_input = input.clone();
    let callback: PatchActivationCallback = Arc::new(move |value| {
        callback_calls.fetch_add(1, Ordering::Relaxed);
        *callback_input.lock().unwrap() = Some((
            value.workflow_info.workflow_type().to_string(),
            value.patch_id,
        ));
        false
    });

    let (result, marker_count) =
        run_patch_activation_workflow("wasm_patch_activation_callback_can_decline", Some(callback))
            .await;

    assert_eq!(result, vec![false, false]);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(marker_count, 0);
    let input = input.lock().unwrap();
    let input = input.as_ref().unwrap();
    assert_eq!(input.0, WASM_PATCH_ACTIVATION_WORKFLOW_TYPE);
    assert_eq!(input.1, WASM_PATCH_ID);
}

#[tokio::test]
async fn wasm_patch_activation_callback_can_activate() {
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = calls.clone();
    let callback: PatchActivationCallback = Arc::new(move |_| {
        callback_calls.fetch_add(1, Ordering::Relaxed);
        true
    });

    let (result, marker_count) = run_patch_activation_workflow(
        "wasm_patch_activation_callback_can_activate",
        Some(callback),
    )
    .await;

    assert_eq!(result, vec![true, true]);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(marker_count, 1);
}

#[tokio::test]
async fn wasm_patch_activation_defaults_to_active() {
    let (result, marker_count) =
        run_patch_activation_workflow("wasm_patch_activation_defaults_to_active", None).await;

    assert_eq!(result, vec![true, true]);
    assert_eq!(marker_count, 1);
}

#[tokio::test]
async fn wasm_patch_activation_callback_panic_fails_workflow_task() {
    let component_path = build_wasm_patch_activation_component().await;
    let component = WasmWorkflowComponent::from_file(WASM_COMPONENT_ID, component_path)
        .expect("sample WASM component should be loadable");
    let mut starter =
        CoreWfStarter::new("wasm_patch_activation_callback_panic_fails_workflow_task");
    starter.sdk_config.patch_activation_callback =
        Some(Arc::new(|_| panic!("wasm patch activation callback panic")));
    starter.sdk_config.register_wasm_workflow(component);

    let mut worker = starter.worker().await;
    let payload_converter = PayloadConverter::default();
    let input = RawValue::from_value(&WASM_PATCH_ID, &payload_converter);
    let workflow_id = starter.get_wf_id().to_owned();
    let mut start_options =
        WorkflowStartOptions::new(starter.get_task_queue().to_owned(), workflow_id).build();
    start_options.execution_timeout = Some(Duration::from_secs(60));
    worker
        .submit_wf(
            WASM_PATCH_ACTIVATION_WORKFLOW_TYPE,
            input.payloads,
            start_options,
        )
        .await
        .expect("WASM workflow should start");

    let core = worker.core_worker();
    let run_worker = async {
        worker
            .inner_mut()
            .run()
            .await
            .expect("worker should shut down cleanly");
    };
    let observe_failure = async {
        let attrs = eventually(
            || async {
                wasm_task_failure_attrs(&starter)
                    .await
                    .ok_or("workflow task failure not yet recorded")
            },
            Duration::from_secs(20),
        )
        .await
        .expect("WASM patch callback panic should fail the workflow task");
        core.shutdown().await;
        attrs
    };
    let (_, attrs) = tokio::join!(run_worker, observe_failure);
    assert!(
        attrs
            .failure
            .expect("workflow task failure should include a failure")
            .message
            .contains("wasm patch activation callback panic")
    );
}

#[tokio::test]
async fn wasm_task_failure_preserves_wit_failure_details() {
    let component_path = build_wasm_hello_component().await;
    let component = WasmWorkflowComponent::from_file(WASM_COMPONENT_ID, component_path)
        .expect("sample WASM component should be loadable");

    let mut starter = CoreWfStarter::new("wasm_task_failure_preserves_wit_failure_details");
    starter.sdk_config.register_wasm_workflow(component);

    let mut worker = starter.worker().await;
    let workflow_id = starter.get_wf_id().to_owned();
    let mut start_options =
        WorkflowStartOptions::new(starter.get_task_queue().to_owned(), workflow_id).build();
    start_options.execution_timeout = Some(Duration::from_secs(60));
    worker
        .submit_wf(WASM_TASK_FAILURE_WORKFLOW_TYPE, vec![], start_options)
        .await
        .expect("WASM workflow should start");

    let core = worker.core_worker();
    let run_worker = async {
        worker
            .inner_mut()
            .run()
            .await
            .expect("worker should shut down cleanly");
    };
    let observe_failure = async {
        let attrs = eventually(
            || async {
                wasm_task_failure_attrs(&starter)
                    .await
                    .ok_or("workflow task failure not yet recorded")
            },
            Duration::from_secs(20),
        )
        .await
        .expect("WASM workflow task failure should be recorded in history");
        core.shutdown().await;
        attrs
    };
    let (_, attrs) = tokio::join!(run_worker, observe_failure);

    assert_eq!(
        attrs.cause(),
        WorkflowTaskFailedCause::NonDeterministicError
    );
    let failure = attrs
        .failure
        .expect("workflow task failure should preserve structured failure");
    assert_eq!(failure.message, "structured wasm workflow task failure");
    let app_info = match failure.failure_info {
        Some(FailureInfo::ApplicationFailureInfo(info)) => info,
        other => panic!("expected application failure info, got {other:?}"),
    };
    assert_eq!(app_info.r#type, "WasmTaskFailure");
    assert!(app_info.non_retryable);
}

async fn run_string_workflow(
    test_name: &'static str,
    component: WasmWorkflowComponent,
    workflow_type: &'static str,
    expected_result: &'static str,
) {
    let mut starter = CoreWfStarter::new(test_name);
    starter.sdk_config.register_wasm_workflow(component);

    let mut worker = starter.worker().await;
    let client = starter.get_core_client().await;
    let payload_converter = PayloadConverter::default();
    let input = RawValue::from_value(&"workflow", &payload_converter);
    let workflow_id = starter.get_wf_id().to_owned();

    let mut start_options =
        WorkflowStartOptions::new(starter.get_task_queue().to_owned(), workflow_id.clone()).build();
    start_options.execution_timeout = Some(Duration::from_secs(60));
    worker
        .submit_wf(workflow_type, input.payloads, start_options)
        .await
        .expect("WASM workflow should start");
    worker
        .run_until_done()
        .await
        .expect("WASM workflow should complete");

    let result = client
        .get_workflow_handle::<UntypedWorkflow>(&workflow_id)
        .get_result(Default::default())
        .await
        .expect("WASM workflow result should be available");
    let greeting: String = result.to_value(&payload_converter);
    assert_eq!(greeting, expected_result);
}

async fn run_patch_activation_workflow(
    test_name: &'static str,
    callback: Option<PatchActivationCallback>,
) -> (Vec<bool>, usize) {
    let component_path = build_wasm_patch_activation_component().await;
    let component = WasmWorkflowComponent::from_file(WASM_COMPONENT_ID, component_path)
        .expect("sample WASM component should be loadable");
    let mut starter = CoreWfStarter::new(test_name);
    starter.sdk_config.patch_activation_callback = callback;
    starter.sdk_config.register_wasm_workflow(component);

    let mut worker = starter.worker().await;
    let client = starter.get_core_client().await;
    let payload_converter = PayloadConverter::default();
    let input = RawValue::from_value(&WASM_PATCH_ID, &payload_converter);
    let workflow_id = starter.get_wf_id().to_owned();
    let mut start_options =
        WorkflowStartOptions::new(starter.get_task_queue().to_owned(), workflow_id.clone()).build();
    start_options.execution_timeout = Some(Duration::from_secs(60));
    worker
        .submit_wf(
            WASM_PATCH_ACTIVATION_WORKFLOW_TYPE,
            input.payloads,
            start_options,
        )
        .await
        .expect("WASM workflow should start");
    worker
        .run_until_done()
        .await
        .expect("WASM workflow should complete");

    let result = client
        .get_workflow_handle::<UntypedWorkflow>(&workflow_id)
        .get_result(Default::default())
        .await
        .expect("WASM workflow result should be available");
    let result: Vec<bool> = result.to_value(&payload_converter);
    let marker_count = starter
        .get_history()
        .await
        .events
        .into_iter()
        .filter(|event| {
            matches!(
                &event.attributes,
                Some(HistoryEventAttributes::MarkerRecordedEventAttributes(attrs))
                    if attrs.marker_name == PATCH_MARKER_NAME
            )
        })
        .count();
    (result, marker_count)
}

async fn wasm_task_failure_attrs(
    starter: &CoreWfStarter,
) -> Option<WorkflowTaskFailedEventAttributes> {
    starter
        .get_history()
        .await
        .events
        .into_iter()
        .find_map(|event| {
            if event.event_type() != EventType::WorkflowTaskFailed {
                return None;
            }
            match event.attributes {
                Some(HistoryEventAttributes::WorkflowTaskFailedEventAttributes(attrs)) => {
                    Some(attrs)
                }
                _ => None,
            }
        })
}

async fn build_wasm_hello_component() -> PathBuf {
    let sample_dir = repository_root().join("crates/sdk/examples/wasm_workflows");
    build_wasm_component(sample_dir, "temporal_wasm_hello_workflow.wasm").await
}

async fn build_wasm_interceptor_component() -> PathBuf {
    let fixture_dir = repository_root().join("crates/sdk-core/tests/fixtures/wasm_interceptor");
    build_wasm_component(fixture_dir, "temporal_wasm_interceptor_workflow.wasm").await
}

async fn build_wasm_patch_activation_component() -> PathBuf {
    let fixture_dir =
        repository_root().join("crates/sdk-core/tests/fixtures/wasm_patch_activation");
    build_wasm_component(fixture_dir, "temporal_wasm_patch_activation_workflow.wasm").await
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("sdk-core crate should live under crates/")
        .to_path_buf()
}

async fn build_wasm_component(component_dir: PathBuf, artifact_name: &str) -> PathBuf {
    let output = Command::new(env!("CARGO"))
        .args([
            "component",
            "build",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
            "--target-dir",
        ])
        .arg(component_dir.join("target"))
        .current_dir(&component_dir)
        .output()
        .await
        .expect("cargo component should be runnable");

    assert!(
        output.status.success(),
        "cargo component build --release --target wasm32-unknown-unknown failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let component_path = component_dir
        .join("target/wasm32-unknown-unknown/release")
        .join(artifact_name);
    assert!(
        component_path.exists(),
        "cargo component did not create {}",
        component_path.display()
    );
    component_path
}
