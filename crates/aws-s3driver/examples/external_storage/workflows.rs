#![allow(unreachable_pub)]
use serde::{Deserialize, Serialize};
use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use uuid::Uuid;

/// Random notes don't compress well, so the order list stays above the external storage threshold
/// after the codec compresses it.
const NOTES_UUIDS_PER_ORDER: usize = 112;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub weight_kg: f64,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSummary {
    pub order_count: usize,
    pub total_weight_kg: f64,
}

/// The workflow's own input and result are small. The order list passed from `fetch_orders` to
/// `summarize_orders` is the payload large enough to be offloaded to external storage.
#[workflow]
#[derive(Default)]
pub struct ProcessOrderBatchWorkflow;

#[workflow_methods]
impl ProcessOrderBatchWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        order_count: usize,
    ) -> WorkflowResult<BatchSummary> {
        let opts = || ActivityOptions::start_to_close_timeout(Duration::from_secs(60));
        let orders = ctx
            .execute_activity(OrderActivities::fetch_orders, order_count, opts())
            .await?;
        let summary = ctx
            .execute_activity(OrderActivities::summarize_orders, orders, opts())
            .await?;
        Ok(summary)
    }
}

pub struct OrderActivities;

#[activities]
impl OrderActivities {
    #[activity]
    pub async fn fetch_orders(
        _ctx: ActivityContext,
        order_count: usize,
    ) -> Result<Vec<Order>, ActivityError> {
        Ok((1..=order_count)
            .map(|i| Order {
                id: format!("ORD-{i:06}"),
                weight_kg: (i % 50) as f64 + 0.5,
                notes: (0..NOTES_UUIDS_PER_ORDER)
                    .map(|_| Uuid::new_v4().simple().to_string())
                    .collect(),
            })
            .collect())
    }

    #[activity]
    pub async fn summarize_orders(
        _ctx: ActivityContext,
        orders: Vec<Order>,
    ) -> Result<BatchSummary, ActivityError> {
        Ok(BatchSummary {
            order_count: orders.len(),
            total_weight_kg: orders.iter().map(|o| o.weight_kg).sum(),
        })
    }
}
