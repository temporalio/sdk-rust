use crate::shared_tests;

#[temporalio_macros::cloud_test_exclusion(
    NeedsCloudAdaptation,
    "Uses new_cloud_or_local, which treats envconfig as local and calls a cluster-info RPC unavailable to Cloud namespace credentials."
)]
#[tokio::test]
async fn priority_values_sent_to_server() {
    shared_tests::priority::priority_values_sent_to_server().await
}
