#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

//! Temporal workflow authoring APIs and runtime glue.

extern crate self as temporalio_workflow;

pub use temporalio_common_wasm as common;
pub use temporalio_macros::{
    init, query, run, signal, update, update_validator, workflow, workflow_methods,
};

#[doc(hidden)]
pub mod __private {
    pub use futures_util::{FutureExt, future::LocalBoxFuture, join, select_biased};

    pub mod macros {
        pub use crate::{
            component::{
                __wit_export, ExportedComponent, StaticWorkflowComponent, bindings,
                instantiate_component_workflow,
                instantiate_component_workflow_with_interceptor_constructors,
            },
            runtime::{
                entry::WorkflowImplementation,
                guest::WorkflowInstance,
                host::WorkflowHost,
                types::{
                    UpdateDefinitionDescriptor, WorkflowDefinitionDescriptor, WorkflowFailure,
                    WorkflowInit,
                },
            },
        };
    }

    pub mod sdk {
        pub use crate::runtime::{
            entry::WorkflowImplementation,
            guest::WorkflowInstance,
            host::WorkflowHost,
            instance::GuestWorkflowInstance,
            is_sdk_wake,
            types::{
                ActivationJobResult, ActivationResult, MAIN_ROUTINE_ID, MainRoutineCompletion,
                QueryResponse, RoutineCompletion, RoutineId, RoutineKind, RoutinePendingState,
                RoutinePollResult, StartedRoutine, TaskFailure, TerminalOutcome,
                UpdateRoutineCompletion, UpdateRoutineKind, WorkflowActivation, WorkflowFailure,
                WorkflowInit,
            },
        };
    }
}

mod cancellation;
mod component;
mod runtime;
mod workflow_context;
pub mod workflow_interceptors;
pub mod workflows;

pub use cancellation::{WorkflowCancellationError, WorkflowCancellationToken};
pub use runtime::model::{TimerResult, WorkflowResult, WorkflowTermination};
pub use temporalio_common_wasm::{
    ActivityCloseTimeouts, Memo, MemoValue, MemoValues, RetryPolicy,
    error::{
        ActivityExecutionError, CancelExternalWorkflowError, ChildWorkflowExecutionError,
        ChildWorkflowStartError, RetryState, TimeoutType, WorkflowSignalError,
    },
};
pub use workflow_context::{
    ActivityCancellationType, ActivityOptions, BaseWorkflowContext, CancellableFuture,
    CancellableFutureWithReason, ChildWorkflowCancellationType, ChildWorkflowOptions,
    ContinueAsNewOptions, ExternalWorkflowHandle, LocalActivityOptions, NamespacedWorkflowInfo,
    ParentClosePolicy, SignalWorkflowOptions, StartChildWorkflowExecutionFailedCause,
    StartChildWorkflowOutput, StartedChildWorkflow, SyncWorkflowContext, TimerOptions,
    VersioningIntent, WaitConditionOptions, WorkflowContext, WorkflowContextView,
    WorkflowIdReusePolicy, WorkflowRandomStream, WorkflowRandomValue,
};
#[cfg(feature = "experimental")]
pub use workflow_context::{
    ContinueAsNewVersioningBehavior, NexusOperationCancellationType, NexusOperationOptions,
    PatchActivationCallback, PatchActivationInput, StartedNexusOperation,
};
#[doc(hidden)]
pub use workflow_context::{
    PatchActivationCallback as InternalPatchActivationCallback, PatchActivationCaller,
};
pub use workflows::{join, join_all, select};

#[macro_export]
#[doc(hidden)]
macro_rules! __temporal_select {
    ($($tokens:tt)*) => {
        $crate::__private::select_biased! { $($tokens)* }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __temporal_join {
    ($($tokens:tt)*) => {
        $crate::__private::join!($($tokens)*)
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __temporalio_export_workflow_component {
    ($export_type:ident) => {
        $crate::__private::macros::__wit_export!(
            $export_type with_types_in $crate::__private::macros::bindings
        );
    };
}

#[macro_export]
/// Export one or more workflow implementations as a component-model workflow module.
///
/// Component-side workflow interceptor constructors can be supplied with
/// `interceptor_constructors = [constructor]`. Each constructor receives a read-only workflow
/// context and is invoked for every workflow instance.
macro_rules! export_workflow_module {
    ([$($workflow:ty),+ $(,)?]) => {
        ::temporalio_workflow::export_workflow_module!(
            [$($workflow),+],
            interceptor_constructors = [],
        );
    };
    ([$($workflow:ty),+ $(,)?], interceptor_constructors = [$($constructor:expr),* $(,)?] $(,)?) => {
        const _: () = {
            struct __TemporalWorkflowModule;

            fn __temporal_workflow_interceptor_constructors() -> ::std::vec::Vec<
                ::temporalio_workflow::workflow_interceptors::WorkflowInterceptorConstructor,
            > {
                ::std::vec![
                    $(
                        ::temporalio_workflow::workflow_interceptors::WorkflowInterceptorConstructor::new(
                            $constructor,
                        )
                    ),*
                ]
            }

            impl $crate::__private::macros::StaticWorkflowComponent for __TemporalWorkflowModule {
                fn list_workflows(
                ) -> ::std::vec::Vec<$crate::__private::macros::WorkflowDefinitionDescriptor> {
                    ::std::vec![$(<$workflow as $crate::__private::macros::WorkflowImplementation>::definition()),*]
                }

                fn instantiate_workflow(
                    workflow_type: &str,
                    init: $crate::__private::macros::WorkflowInit,
                    host: ::std::rc::Rc<dyn $crate::__private::macros::WorkflowHost>,
                ) -> ::std::result::Result<
                    ::std::boxed::Box<dyn $crate::__private::macros::WorkflowInstance>,
                    $crate::__private::macros::WorkflowFailure,
                > {
                    match workflow_type {
                        $(
                            name if name == <$workflow as $crate::__private::macros::WorkflowImplementation>::name() => {
                                $crate::__private::macros::instantiate_component_workflow_with_interceptor_constructors::<$workflow>(
                                    init,
                                    host,
                                    __temporal_workflow_interceptor_constructors(),
                                )
                            }
                        )*
                        _ => Err(::std::boxed::Box::new(
                            ::temporalio_workflow::common::protos::temporal::api::failure::v1::Failure {
                                message: ::std::format!(
                                    "No workflow named '{}' exported by this component",
                                    workflow_type
                                ),
                                ..::std::default::Default::default()
                            },
                        )),
                    }
                }
            }

            type __TemporalWorkflowComponentExport =
                $crate::__private::macros::ExportedComponent<__TemporalWorkflowModule>;

            ::temporalio_workflow::__temporalio_export_workflow_component!(
                __TemporalWorkflowComponentExport
            );
        };
    };
}
