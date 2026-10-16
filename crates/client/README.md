# Temporal Rust Client

[![crates.io](https://img.shields.io/crates/v/temporalio-client.svg)](https://crates.io/crates/temporalio-client)
[![docs.rs](https://docs.rs/temporalio-client/badge.svg)](https://docs.rs/temporalio-client)

This crate provides a Rust client for interacting with the Temporal service. It can be used
standalone to start and manage workflows, or together with the
[`temporalio-sdk`](https://crates.io/crates/temporalio-sdk) crate to run workers.

## Experimental standalone Nexus operations

With the `experimental` feature, `Client::nexus_client` starts Temporal-managed Nexus executions
outside workflows. This is not a direct Nexus HTTP client. The server must support standalone Nexus
execution RPCs and have an endpoint registered for the service.

```rust
use nexus_rpc::service;
use temporalio_client::{Client, NexusOperationStartOptions};

#[service]
trait PaymentService {
    #[operation]
    fn charge(input: String) -> String;
}

async fn charge(client: &Client, input: String) -> Result<String, temporalio_client::nexus::NexusClientError> {
    let payments = client.nexus_client(PaymentService, "payments-endpoint");
    let handle = payments.start(
        PaymentService::charge,
        input,
        NexusOperationStartOptions::new("charge-123").build(),
    ).await?;
    assert_eq!(handle.operation_id(), "charge-123");
    let receipt: String = handle.result().await?;
    Ok(receipt)
}
```

`start` returns once Temporal accepts the execution, not when the handler starts. Use
`handle.operation_token()` to wait for handler start, `cancel()` to request cancellation, and
`describe()` for a codec-decoded description. Dropping a handle or waiting future does not cancel
the durable operation. Persist both `operation_id()` and `run_id()`, then reconnect using
`payments.get_handle(PaymentService::charge, operation_id, run_id)`. Handles always target an exact
run, including when a start's conflict policy returns an existing execution.

Start options accept timeouts, ID reuse/conflict policies, search attributes, UI metadata, and
`RpcOptions`. Handle methods have `*_with_options` variants for per-call RPC controls. Nexus headers
are available only through `ClientInterceptor::start_nexus_operation`'s request, and are transmitted
as plaintext; typed start options deliberately do not expose them.

Input, result, and failure payloads use the configured data converter and codecs in a Nexus
serialization context. Search attributes and fixed UI metadata follow Temporal's standard encoding
rather than custom codecs. The current experimental Nexus serialization context carries no target
fields, so target-aware codecs cannot yet consume the server's propagated serialization context.
This API and its pinned experimental `nexus-rpc` dependency may change.

## Quick Start

### Connecting and Starting Workflows with Environment Configuration

The simplest way to connect is to load connection settings from environment variables and/or a
`temporal.toml` config file:

```rust
use temporalio_client::{
    Client, Connection, WorkflowOptions, GetWorkflowResultOptions,
    envconfig::LoadClientConfigProfileOptions,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_options, client_options) = ClientOptions::load_from_config(
        LoadClientConfigProfileOptions::default()
    )?;

    let connection = Connection::connect(conn_options).await?;
    let client = Client::new(connection, client_options);

    // Start a workflow
    let handle = client.start_workflow(
        GreetingWorkflow::run,
        "World".to_string(),
        WorkflowOptions::new("my-task-queue", "greeting-workflow-1").build()
    ).await?;

    // Wait for the result
    let result = handle.get_result(GetWorkflowResultOptions::default()).await?;
    Ok(())
}
```

This reads `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, TLS settings, and more
from the environment. See the [`envconfig` module docs](https://docs.rs/temporalio-client/latest/temporalio_client/envconfig/) for the full list of
supported variables and the TOML file format.

### Connecting and Starting Workflows with Explicit Options

```rust
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions,
    WorkflowOptions, GetWorkflowResultOptions, Url,
};
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = Connection::connect(
        ConnectionOptions::new(Url::from_str("http://localhost:7233")?)
            .identity("my-client".to_string())
            .build()
    ).await?;

    // "default" is your namespace
    let client = Client::new(connection, ClientOptions::new("default").build());

    // Start a workflow
    let handle = client.start_workflow(
        GreetingWorkflow::run,
        "World".to_string(),
        WorkflowOptions::new("my-task-queue", "greeting-workflow-1").build()
    ).await?;

    // Wait for the result
    let result = handle.get_result(GetWorkflowResultOptions::default()).await?;

    Ok(())
}
```

### Signals, Queries, and Updates

Once you have a workflow handle, you can interact with the running workflow:

```rust
use temporalio_client::{
    SignalOptions, QueryOptions, UpdateOptions,
    StartUpdateOptions,
    UntypedSignal,
};
use temporalio_common::data_converters::{PayloadConverter, RawValue};

// Get a handle to an existing workflow (or use one from start_workflow)
let handle = client.get_workflow_handle::<MyWorkflow>("workflow-id");

// --- Signals (fire-and-forget messages) ---
handle.signal(MyWorkflow::push_value, 42, SignalOptions::default()).await?;

// --- Queries (read workflow state) ---
let values = handle
    .query(MyWorkflow::get_values, (), QueryOptions::default())
    .await?;

// --- Updates (modify state and get a result) ---
let values = handle
    .execute_update(MyWorkflow::add_wait_return, 100, UpdateOptions::default())
    .await?;

// Start an update and wait for acceptance only
let update_handle = handle
    .start_update(
        MyWorkflow::add_wait_return,
        50,
        StartUpdateOptions::default()
    )
    .await?;
update_handle.get_result().await?;

// --- Untyped interactions (when types aren't known at compile time) ---
let pc = PayloadConverter::serde_json();
handle
    .signal(
        UntypedSignal::new("increment"),
        RawValue::from_value(&25i32, &pc),
        SignalOptions::default(),
    )
    .await?;
// UntypedQuery and UntypedUpdate work similarly
```

### Cancelling and Terminating Workflows

```rust
use temporalio_client::{CancelWorkflowOptions, TerminateWorkflowOptions};

// Request cancellation (workflow can handle this gracefully)
handle.cancel(CancelWorkflowOptions::builder().reason("No longer needed").build()).await?;

// Terminate immediately (workflow cannot intercept this)
handle.terminate(TerminateWorkflowOptions::builder().reason("Emergency shutdown").build()).await?;
```

### Listing Workflows

```rust
use temporalio_client::ListWorkflowsOptions;
use futures_util::StreamExt;

let mut stream = client.list_workflows(
    "WorkflowType = 'GreetingWorkflow'",
    ListWorkflowsOptions::builder().limit(10).build()
);

while let Some(result) = stream.next().await {
    let execution = result?;
    println!("Workflow: {} ({})", execution.id(), execution.workflow_type());
}
```

## Experimental APIs

APIs that are still under development require the `experimental` Cargo feature and may change or
be removed before stabilization.

## Building Without `protoc`

Enable the `vendored-protox` Cargo feature to compile protobuf definitions with `protox`
instead of a system `protoc` binary.

## Raw gRPC Access

For operations not covered by the high-level API, access the underlying gRPC service clients
directly:

```rust
let mut workflowf_service = client.connection().workflow_service();
let mut operator_service = client.connection().operator_service();
```
