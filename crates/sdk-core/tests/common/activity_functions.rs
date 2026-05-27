use std::time::Duration;
use temporalio_common::data_converters::RawValue;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use temporalio_sdk_core::replay::DEFAULT_ACTIVITY_TYPE;
use tokio::time::sleep;

pub(crate) struct StdActivities;

#[activities]
impl StdActivities {
    #[activity]
    pub(crate) async fn echo(_ctx: ActivityContext, e: String) -> Result<String, ActivityError> {
        Ok(e)
    }

    /// Activity that does nothing and returns success
    #[activity]
    pub(crate) async fn no_op(_ctx: ActivityContext, _: ()) -> Result<(), ActivityError> {
        Ok(())
    }

    /// Also a no-op, but uses the default name from history construction to work with
    /// canned histories. Returns RawValue so anything in the canned history will "deserialize"
    /// properly.
    #[activity(name = DEFAULT_ACTIVITY_TYPE)]
    pub(crate) async fn default(_ctx: ActivityContext, _: ()) -> Result<RawValue, ActivityError> {
        Ok(RawValue::empty())
    }

    /// Activity that sleeps for provided duration. Name is overriden to provide compatibility with
    /// some canned histories.
    #[activity(name = "delay")]
    pub(crate) async fn delay(
        _ctx: ActivityContext,
        duration: Duration,
    ) -> Result<(), ActivityError> {
        sleep(duration).await;
        Ok(())
    }

    /// Always fails
    #[activity]
    pub(crate) async fn always_fail(_ctx: ActivityContext) -> Result<(), ActivityError> {
        Err(anyhow::anyhow!("Oh no I failed!").into())
    }

    #[activity]
    pub(crate) async fn concat(
        _ctx: ActivityContext,
        a: String,
        b: String,
    ) -> Result<String, ActivityError> {
        Ok(format!("{a}{b}"))
    }
}
