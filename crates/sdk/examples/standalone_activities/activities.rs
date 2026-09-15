#![allow(unreachable_pub)]
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

pub struct GreetingActivities;

#[activities]
impl GreetingActivities {
    #[activity]
    pub async fn compose_greeting(
        _ctx: ActivityContext,
        input: (String, String),
    ) -> Result<String, ActivityError> {
        let (greeting, name) = input;
        Ok(format!("{greeting}, {name}!"))
    }
}
