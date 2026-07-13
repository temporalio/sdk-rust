use std::time::Duration;

use crate::protos::temporal::api::common::v1::RetryPolicy as ProtoRetryPolicy;

const DEFAULT_INITIAL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_BACKOFF_COEFFICIENT: f64 = 2.0;

/// Options for retrying workflows and activities.
#[derive(Clone, Debug, bon::Builder)]
#[non_exhaustive]
#[builder(state_mod(vis = "pub"))]
pub struct RetryPolicy {
    /// Backoff interval for the first retry.
    #[builder(default = DEFAULT_INITIAL_INTERVAL)]
    pub initial_interval: Duration,
    /// Coefficient used to calculate the next retry interval.
    #[builder(default = DEFAULT_BACKOFF_COEFFICIENT)]
    pub backoff_coefficient: f64,
    /// Maximum backoff interval between retries.
    pub maximum_interval: Option<Duration>,
    /// Maximum number of attempts. Zero means unlimited attempts.
    #[builder(default)]
    pub maximum_attempts: i32,
    /// Error type names that should not be retried.
    #[builder(
        with = |values: impl IntoIterator<Item = impl Into<String>>| values
            .into_iter()
            .map(Into::into)
            .collect(),
        default
    )]
    pub non_retryable_error_types: Vec<String>,
    #[builder(skip = ProtoRetryPolicy {
        initial_interval: initial_interval.try_into().ok(),
        backoff_coefficient,
        maximum_interval: maximum_interval.and_then(|duration| duration.try_into().ok()),
        maximum_attempts,
        non_retryable_error_types: non_retryable_error_types.clone(),
    })]
    raw: ProtoRetryPolicy,
}

impl PartialEq for RetryPolicy {
    fn eq(&self, other: &Self) -> bool {
        self.initial_interval == other.initial_interval
            && self.backoff_coefficient == other.backoff_coefficient
            && self.maximum_interval == other.maximum_interval
            && self.maximum_attempts == other.maximum_attempts
            && self.non_retryable_error_types == other.non_retryable_error_types
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_interval: DEFAULT_INITIAL_INTERVAL,
            backoff_coefficient: DEFAULT_BACKOFF_COEFFICIENT,
            maximum_interval: None,
            maximum_attempts: 0,
            non_retryable_error_types: Vec::new(),
            raw: ProtoRetryPolicy {
                initial_interval: DEFAULT_INITIAL_INTERVAL.try_into().ok(),
                backoff_coefficient: DEFAULT_BACKOFF_COEFFICIENT,
                ..Default::default()
            },
        }
    }
}

impl RetryPolicy {
    /// Access the underlying retry policy protobuf.
    pub fn raw(&self) -> &ProtoRetryPolicy {
        &self.raw
    }

    /// Consume this wrapper and return the underlying retry policy protobuf.
    pub fn into_raw(self) -> ProtoRetryPolicy {
        self.raw
    }
}

impl From<ProtoRetryPolicy> for RetryPolicy {
    fn from(value: ProtoRetryPolicy) -> Self {
        let raw = value.clone();
        Self {
            initial_interval: value
                .initial_interval
                .and_then(|duration| duration.try_into().ok())
                .unwrap_or(DEFAULT_INITIAL_INTERVAL),
            backoff_coefficient: if value.backoff_coefficient == 0.0 {
                DEFAULT_BACKOFF_COEFFICIENT
            } else {
                value.backoff_coefficient
            },
            maximum_interval: value
                .maximum_interval
                .and_then(|duration| duration.try_into().ok()),
            maximum_attempts: value.maximum_attempts,
            non_retryable_error_types: value.non_retryable_error_types,
            raw,
        }
    }
}

impl From<RetryPolicy> for ProtoRetryPolicy {
    fn from(value: RetryPolicy) -> Self {
        Self {
            initial_interval: value.initial_interval.try_into().ok(),
            backoff_coefficient: value.backoff_coefficient,
            maximum_interval: value
                .maximum_interval
                .and_then(|duration| duration.try_into().ok()),
            maximum_attempts: value.maximum_attempts,
            non_retryable_error_types: value.non_retryable_error_types,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_temporal_retry_defaults() {
        assert_eq!(RetryPolicy::default(), RetryPolicy::builder().build());
        assert_eq!(
            RetryPolicy::default().initial_interval,
            Duration::from_secs(1)
        );
        assert_eq!(RetryPolicy::default().backoff_coefficient, 2.0);
        assert_eq!(RetryPolicy::default().maximum_attempts, 0);
    }

    #[test]
    fn builder_uses_rust_duration_and_proto_round_trips() {
        let policy = RetryPolicy::builder()
            .initial_interval(Duration::from_millis(250))
            .backoff_coefficient(1.5)
            .maximum_interval(Duration::from_secs(10))
            .maximum_attempts(5)
            .non_retryable_error_types(["InvalidInput"])
            .build();

        assert_eq!(
            RetryPolicy::from(ProtoRetryPolicy::from(policy.clone())),
            policy
        );
    }

    #[test]
    fn absent_proto_defaults_are_normalized() {
        assert_eq!(
            RetryPolicy::from(ProtoRetryPolicy::default()),
            RetryPolicy::default()
        );
    }

    #[test]
    fn normalization_retains_source_proto() {
        let raw = ProtoRetryPolicy {
            initial_interval: Some(prost_types::Duration {
                seconds: -1,
                nanos: 0,
            }),
            ..Default::default()
        };
        let policy = RetryPolicy::from(raw.clone());

        assert_eq!(policy.initial_interval, DEFAULT_INITIAL_INTERVAL);
        assert_eq!(policy.raw(), &raw);
        assert_eq!(policy.into_raw(), raw);
    }
}
