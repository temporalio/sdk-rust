use temporalio_macros::{activities, activity_definitions};
use temporalio_sdk::activities::{ActivityContext, ActivityError};

pub struct SharedActivities;

#[activity_definitions]
impl SharedActivities {
    #[activity]
    pub fn greet(name: String) -> Result<String, ActivityError> {
        unimplemented!()
    }
}

pub struct MyImpl;

#[activities]
impl MyImpl {
    // Input type doesn't match the definition: the definition takes `String`,
    // this impl takes `i32`.
    #[activity(definition = shared_activities::Greet)]
    pub async fn greet(_ctx: ActivityContext, _name: i32) -> Result<String, ActivityError> {
        Ok(String::new())
    }
}

fn main() {}
