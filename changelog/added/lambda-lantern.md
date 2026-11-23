Experimental AWS Lambda support is available through `temporalio-sdk-aws-lambda`, with per-invocation Workers, pinned deployment versioning by default, deadline-aware shutdown, and optional OpenTelemetry metrics and tracing.
OpenTelemetry metric exporters support an explicit request timeout and flushing buffered metrics without shutting down the provider.
