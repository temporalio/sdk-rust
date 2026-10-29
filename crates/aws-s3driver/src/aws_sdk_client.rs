use crate::client::{BoxError, S3StorageDriverClient};
use aws_sdk_s3::{Client, primitives::ByteStream};
use futures_util::future::BoxFuture;
use std::collections::HashMap;

/// [`S3StorageDriverClient`] backed by an [`aws_sdk_s3::Client`] from the official AWS SDK.
///
/// This API is experimental and may change in the future.
#[derive(Clone, Debug)]
pub struct AwsSdkS3StorageDriverClient {
    client: Client,
}

impl AwsSdkS3StorageDriverClient {
    /// Create a client that makes requests with the given AWS S3 client.
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl S3StorageDriverClient for AwsSdkS3StorageDriverClient {
    fn put_object<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        data: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move {
            self.client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(data))
                .send()
                .await?;
            Ok(())
        })
    }

    fn object_exists<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<bool, BoxError>> {
        Box::pin(async move {
            match self
                .client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
            {
                Ok(_) => Ok(true),
                Err(err) if err.as_service_error().is_some_and(|err| err.is_not_found()) => {
                    Ok(false)
                }
                Err(err) => Err(err.into()),
            }
        })
    }

    fn get_object<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
        Box::pin(async move {
            let output = self
                .client
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await?;
            Ok(output.body.collect().await?.into_bytes().to_vec())
        })
    }

    fn describe(&self) -> HashMap<String, String> {
        self.client
            .config()
            .region()
            .map(|region| HashMap::from([("client_region".to_owned(), region.to_string())]))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::{
        Config,
        config::{BehaviorVersion, Credentials, Region},
    };
    use aws_smithy_http_client::test_util::infallible_client_fn;
    use aws_smithy_types::body::SdkBody;
    use std::sync::{Arc, Mutex};

    fn client(
        respond: impl Fn(http::Request<SdkBody>) -> http::Response<SdkBody> + Send + Sync + 'static,
    ) -> AwsSdkS3StorageDriverClient {
        let config = Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-west-2"))
            .credentials_provider(Credentials::new("id", "secret", None, None, "test"))
            .force_path_style(true)
            .http_client(infallible_client_fn(respond))
            .build();
        AwsSdkS3StorageDriverClient::new(Client::from_conf(config))
    }

    fn status(code: u16) -> http::Response<SdkBody> {
        http::Response::builder()
            .status(code)
            .body(SdkBody::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn object_exists_returns_true_for_200_and_false_for_404() {
        assert!(
            client(|_| status(200))
                .object_exists("my-bucket", "my-key")
                .await
                .unwrap()
        );
        assert!(
            !client(|_| status(404))
                .object_exists("my-bucket", "my-key")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn object_exists_fails_for_errors_other_than_404() {
        client(|_| status(403))
            .object_exists("my-bucket", "my-key")
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn put_object_sends_data_to_bucket_and_key() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let client = client(move |request| {
            recorded.lock().unwrap().push((
                request.method().clone(),
                request.uri().path().to_owned(),
                request.body().bytes().map(<[u8]>::to_vec),
            ));
            status(200)
        });

        client
            .put_object("my-bucket", "my-key", vec![0, 1])
            .await
            .unwrap();

        assert_eq!(
            *requests.lock().unwrap(),
            [(
                http::Method::PUT,
                "/my-bucket/my-key".to_owned(),
                Some(vec![0, 1])
            )]
        );
    }

    #[tokio::test]
    async fn get_object_returns_the_response_body() {
        let client = client(|_| {
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(vec![0, 1]))
                .unwrap()
        });

        assert_eq!(
            client.get_object("my-bucket", "my-key").await.unwrap(),
            [0, 1]
        );
    }

    #[test]
    fn describe_returns_the_client_region() {
        assert_eq!(
            client(|_| status(200)).describe(),
            HashMap::from([("client_region".to_owned(), "us-west-2".to_owned())])
        );
    }
}
