//! The generated protos of the Stream service of the API proposal and of the crate's own calls.

#![allow(missing_docs, unreachable_pub, clippy::all)]

/// The Temporal API packages that the crate generates. Others map to `temporalio-common`, and
/// `enums` has only the Stream enums.
pub mod temporal {
    pub mod api {
        pub mod enums {
            pub mod v1 {
                tonic::include_proto!("temporal.api.enums.v1");
            }
        }

        pub mod stream {
            pub mod v1 {
                tonic::include_proto!("temporal.api.stream.v1");
            }
        }

        pub mod streamservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.streamservice.v1");
            }
        }
    }

    pub mod sdk {
        pub mod streams {
            pub mod v1 {
                tonic::include_proto!("temporal.sdk.streams.v1");
            }
        }
    }
}
