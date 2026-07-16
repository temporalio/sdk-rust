//! Handle for completing activities asynchronously via a client.

use crate::{NamespacedClient, errors::AsyncActivityError, grpc::WorkflowService};
use temporalio_common::{
    data_converters::{SerializationContextData, TemporalSerializable},
    error::{ApplicationFailure, OutgoingActivityError, OutgoingError},
    payload_visitor::encode_payloads,
    protos::{
        TaskToken,
        temporal::api::{
            common::v1::Payloads,
            workflowservice::v1::{
                RecordActivityTaskHeartbeatByIdRequest, RecordActivityTaskHeartbeatByIdResponse,
                RecordActivityTaskHeartbeatRequest, RecordActivityTaskHeartbeatResponse,
                RespondActivityTaskCanceledByIdRequest, RespondActivityTaskCanceledRequest,
                RespondActivityTaskCompletedByIdRequest, RespondActivityTaskCompletedRequest,
                RespondActivityTaskFailedByIdRequest, RespondActivityTaskFailedRequest,
            },
        },
    },
};
use tonic::IntoRequest;

/// Identifies an async activity for completion outside a worker.
#[derive(Debug, Clone)]
pub enum ActivityIdentifier {
    /// Identify activity by its task token
    TaskToken(TaskToken),
    /// Identify activity by workflow and activity IDs.
    ById {
        /// ID of the workflow that scheduled this activity.
        workflow_id: String,
        /// Run ID of the workflow (optional - if not provided, targets the latest run).
        run_id: String,
        /// ID of the activity to complete.
        activity_id: String,
    },
}

impl ActivityIdentifier {
    /// Create an identifier from a task token.
    pub fn from_task_token(token: TaskToken) -> Self {
        Self::TaskToken(token)
    }

    /// Create an identifier from workflow and activity IDs. Use an empty run id to target the
    /// latest workflow execution.
    pub fn by_id(
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
        activity_id: impl Into<String>,
    ) -> Self {
        Self::ById {
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
            activity_id: activity_id.into(),
        }
    }
}

/// Handle for completing activities asynchronously (outside the worker).
pub struct AsyncActivityHandle<CT> {
    client: CT,
    identifier: ActivityIdentifier,
}

impl<CT> AsyncActivityHandle<CT> {
    /// Create a new async activity handle.
    pub fn new(client: CT, identifier: ActivityIdentifier) -> Self {
        Self { client, identifier }
    }

    /// Get the identifier for this activity.
    pub fn identifier(&self) -> &ActivityIdentifier {
        &self.identifier
    }

    /// Get a reference to the underlying client.
    pub fn client(&self) -> &CT {
        &self.client
    }
}

impl<CT: WorkflowService + NamespacedClient + Clone> AsyncActivityHandle<CT> {
    /// Complete the activity with a successful result.
    pub async fn complete<T>(&self, result: Option<T>) -> Result<(), AsyncActivityError>
    where
        T: TemporalSerializable + 'static,
    {
        let result = match result {
            Some(result) => Some(Payloads {
                payloads: vec![
                    self.client
                        .data_converter()
                        .to_payload(&SerializationContextData::Activity, &result)
                        .await?,
                ],
            }),
            None => None,
        };
        match &self.identifier {
            ActivityIdentifier::TaskToken(token) => {
                WorkflowService::respond_activity_task_completed(
                    &mut self.client.clone(),
                    RespondActivityTaskCompletedRequest {
                        task_token: token.0.clone(),
                        result,
                        identity: self.client.identity(),
                        namespace: self.client.namespace(),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .map_err(AsyncActivityError::from_status)?;
            }
            ActivityIdentifier::ById {
                workflow_id,
                run_id,
                activity_id,
            } => {
                WorkflowService::respond_activity_task_completed_by_id(
                    &mut self.client.clone(),
                    RespondActivityTaskCompletedByIdRequest {
                        namespace: self.client.namespace(),
                        workflow_id: workflow_id.clone(),
                        run_id: run_id.clone(),
                        activity_id: activity_id.clone(),
                        result,
                        identity: self.client.identity(),
                        resource_id: Default::default(),
                    }
                    .into_request(),
                )
                .await
                .map_err(AsyncActivityError::from_status)?;
            }
        }
        Ok(())
    }

    /// Fail the activity with a failure.
    pub async fn fail<E, T>(
        &self,
        failure: E,
        last_heartbeat_details: Option<T>,
    ) -> Result<(), AsyncActivityError>
    where
        E: Into<ApplicationFailure>,
        T: TemporalSerializable + 'static,
    {
        let data_converter = self.client.data_converter();
        let mut failure = data_converter.to_failure(
            &SerializationContextData::Activity,
            OutgoingError::Activity(OutgoingActivityError::Application(Box::new(failure.into()))),
        );
        encode_payloads(
            &mut failure,
            data_converter.codec(),
            &SerializationContextData::Activity,
        )
        .await;
        let last_heartbeat_details = match last_heartbeat_details {
            Some(details) => Some(Payloads {
                payloads: self
                    .client
                    .data_converter()
                    .to_payloads(&SerializationContextData::Activity, &details)
                    .await?,
            }),
            None => None,
        };
        match &self.identifier {
            ActivityIdentifier::TaskToken(token) => {
                WorkflowService::respond_activity_task_failed(
                    &mut self.client.clone(),
                    RespondActivityTaskFailedRequest {
                        task_token: token.0.clone(),
                        failure: Some(failure),
                        identity: self.client.identity(),
                        namespace: self.client.namespace(),
                        last_heartbeat_details,
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .map_err(AsyncActivityError::from_status)?;
            }
            ActivityIdentifier::ById {
                workflow_id,
                run_id,
                activity_id,
            } => {
                WorkflowService::respond_activity_task_failed_by_id(
                    &mut self.client.clone(),
                    RespondActivityTaskFailedByIdRequest {
                        namespace: self.client.namespace(),
                        workflow_id: workflow_id.clone(),
                        run_id: run_id.clone(),
                        activity_id: activity_id.clone(),
                        failure: Some(failure),
                        identity: self.client.identity(),
                        last_heartbeat_details,
                        resource_id: Default::default(),
                    }
                    .into_request(),
                )
                .await
                .map_err(AsyncActivityError::from_status)?;
            }
        }
        Ok(())
    }

    /// Reports the activity as canceled.
    pub async fn report_cancelation<T>(&self, details: Option<T>) -> Result<(), AsyncActivityError>
    where
        T: TemporalSerializable + 'static,
    {
        let details = match details {
            Some(details) => Some(Payloads {
                payloads: self
                    .client
                    .data_converter()
                    .to_payloads(&SerializationContextData::Activity, &details)
                    .await?,
            }),
            None => None,
        };
        match &self.identifier {
            ActivityIdentifier::TaskToken(token) => {
                WorkflowService::respond_activity_task_canceled(
                    &mut self.client.clone(),
                    RespondActivityTaskCanceledRequest {
                        task_token: token.0.clone(),
                        details,
                        identity: self.client.identity(),
                        namespace: self.client.namespace(),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .map_err(AsyncActivityError::from_status)?;
            }
            ActivityIdentifier::ById {
                workflow_id,
                run_id,
                activity_id,
            } => {
                WorkflowService::respond_activity_task_canceled_by_id(
                    &mut self.client.clone(),
                    RespondActivityTaskCanceledByIdRequest {
                        namespace: self.client.namespace(),
                        workflow_id: workflow_id.clone(),
                        run_id: run_id.clone(),
                        activity_id: activity_id.clone(),
                        details,
                        identity: self.client.identity(),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .map_err(AsyncActivityError::from_status)?;
            }
        }
        Ok(())
    }

    /// Record a heartbeat for the activity.
    ///
    /// Heartbeats let the server know the activity is still running and can carry
    /// progress information. The response indicates if cancellation has been requested.
    pub async fn heartbeat<T>(
        &self,
        details: Option<T>,
    ) -> Result<ActivityHeartbeatResponse, AsyncActivityError>
    where
        T: TemporalSerializable + 'static,
    {
        let details = match details {
            Some(details) => Some(Payloads {
                payloads: self
                    .client
                    .data_converter()
                    .to_payloads(&SerializationContextData::Activity, &details)
                    .await?,
            }),
            None => None,
        };
        match &self.identifier {
            ActivityIdentifier::TaskToken(token) => {
                let resp = WorkflowService::record_activity_task_heartbeat(
                    &mut self.client.clone(),
                    RecordActivityTaskHeartbeatRequest {
                        task_token: token.0.clone(),
                        details,
                        identity: self.client.identity(),
                        namespace: self.client.namespace(),
                        resource_id: Default::default(),
                    }
                    .into_request(),
                )
                .await
                .map_err(AsyncActivityError::from_status)?
                .into_inner();
                Ok(ActivityHeartbeatResponse::from(resp))
            }
            ActivityIdentifier::ById {
                workflow_id,
                run_id,
                activity_id,
            } => {
                let resp = WorkflowService::record_activity_task_heartbeat_by_id(
                    &mut self.client.clone(),
                    RecordActivityTaskHeartbeatByIdRequest {
                        namespace: self.client.namespace(),
                        workflow_id: workflow_id.clone(),
                        run_id: run_id.clone(),
                        activity_id: activity_id.clone(),
                        details,
                        identity: self.client.identity(),
                        resource_id: Default::default(),
                    }
                    .into_request(),
                )
                .await
                .map_err(AsyncActivityError::from_status)?
                .into_inner();
                Ok(ActivityHeartbeatResponse::from(resp))
            }
        }
    }
}

/// Response from a heartbeat call.
#[derive(Debug, Clone)]
pub struct ActivityHeartbeatResponse {
    /// True if the activity has been asked to cancel itself.
    pub cancel_requested: bool,
    /// True if the activity is paused.
    pub activity_paused: bool,
    /// True if the activity was reset.
    pub activity_reset: bool,
}

impl From<RecordActivityTaskHeartbeatResponse> for ActivityHeartbeatResponse {
    fn from(resp: RecordActivityTaskHeartbeatResponse) -> Self {
        Self {
            cancel_requested: resp.cancel_requested,
            activity_paused: resp.activity_paused,
            activity_reset: resp.activity_reset,
        }
    }
}

impl From<RecordActivityTaskHeartbeatByIdResponse> for ActivityHeartbeatResponse {
    fn from(resp: RecordActivityTaskHeartbeatByIdResponse) -> Self {
        Self {
            cancel_requested: resp.cancel_requested,
            activity_paused: resp.activity_paused,
            activity_reset: resp.activity_reset,
        }
    }
}
