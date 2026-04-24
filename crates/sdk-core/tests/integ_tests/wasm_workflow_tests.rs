use crate::common::CoreWfStarter;
use std::{path::PathBuf, time::Duration};
use temporalio_client::{UntypedWorkflow, WorkflowStartOptions};
use temporalio_common::{
    data_converters::{PayloadConverter, RawValue},
    worker::WorkerTaskTypes,
};
use temporalio_sdk::WasmWorkflowComponent;
use tokio::process::Command;

const WASM_COMPONENT_ID: &str = "hello-workflow-component";
const WASM_WORKFLOW_TYPE: &str = "HelloWorkflow";

#[tokio::test]
async fn wasm_workflow_component_executes() {
    let component_path = build_wasm_hello_component().await;
    let mut starter = CoreWfStarter::new("wasm_workflow_component_executes");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.sdk_config.register_wasm_workflow(
        WasmWorkflowComponent::from_file(WASM_COMPONENT_ID, component_path)
            .expect("sample WASM component should be loadable"),
    );

    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    let payload_converter = PayloadConverter::default();
    let input = RawValue::from_value(&"workflow", &payload_converter);
    let workflow_id = starter.get_wf_id().to_owned();

    let mut start_options =
        WorkflowStartOptions::new(starter.get_task_queue().to_owned(), workflow_id.clone()).build();
    start_options.execution_timeout = Some(Duration::from_secs(60));
    worker
        .submit_wf(WASM_WORKFLOW_TYPE, input.payloads, start_options)
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
    assert_eq!(greeting, "Hello, workflow!");
}

async fn build_wasm_hello_component() -> PathBuf {
    let sample_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("sdk-core crate should live under crates/")
        .join("samples/wasm-workflows/hello");
    let output = Command::new(env!("CARGO"))
        .args([
            "component",
            "build",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
        ])
        .current_dir(&sample_dir)
        .output()
        .await
        .expect("cargo component should be runnable");

    assert!(
        output.status.success(),
        "cargo component build --release --target wasm32-unknown-unknown failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let component_path =
        sample_dir.join("target/wasm32-unknown-unknown/release/temporal_wasm_hello_workflow.wasm");
    assert!(
        component_path.exists(),
        "cargo component did not create {}",
        component_path.display()
    );
    component_path
}
