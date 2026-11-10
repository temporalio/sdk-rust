//! An in-memory S3-compatible server, so the sample runs without an AWS account or Docker. It
//! implements only the three requests the S3 storage driver makes: `HeadObject`, `PutObject`, and
//! `GetObject`.

use bytes::Bytes;
use futures::TryStreamExt;
use hyper::{
    Request,
    body::Incoming,
    service::{Service, service_fn},
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
};
use s3s::{
    S3, S3Request, S3Response, S3Result,
    auth::SimpleAuth,
    dto::{
        GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput, PutObjectInput,
        PutObjectOutput, StreamingBlob,
    },
    s3_error,
    service::S3ServiceBuilder,
};
use std::{collections::HashMap, convert::Infallible, sync::Mutex, time::Instant};
use tokio::net::TcpListener;

const ADDRESS: &str = "localhost:5000";
const BUCKET: &str = "temporal-payloads";
const ACCESS_KEY: &str = "test";
const SECRET_KEY: &str = "test";

#[derive(Default)]
struct MemoryBucket(Mutex<HashMap<String, Bytes>>);

impl MemoryBucket {
    fn get(&self, bucket: &str, key: &str) -> S3Result<Bytes> {
        if bucket != BUCKET {
            return Err(s3_error!(NoSuchBucket));
        }
        self.0
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| s3_error!(NoSuchKey))
    }
}

#[async_trait::async_trait]
impl S3 for MemoryBucket {
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let data = self.get(&req.input.bucket, &req.input.key)?;
        Ok(S3Response::new(HeadObjectOutput {
            content_length: Some(data.len() as i64),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let data = self.get(&req.input.bucket, &req.input.key)?;
        Ok(S3Response::new(GetObjectOutput {
            content_length: Some(data.len() as i64),
            body: Some(StreamingBlob::wrap(futures::stream::iter([Ok::<
                _,
                Infallible,
            >(
                data
            )]))),
            ..Default::default()
        }))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let PutObjectInput {
            bucket, key, body, ..
        } = req.input;
        if bucket != BUCKET {
            return Err(s3_error!(NoSuchBucket));
        }
        let Some(body) = body else {
            return Err(s3_error!(IncompleteBody));
        };
        let chunks: Vec<Bytes> = body
            .try_collect()
            .await
            .map_err(|e| s3_error!(IncompleteBody, "{e}"))?;
        self.0.lock().unwrap().insert(key, chunks.concat().into());
        Ok(S3Response::new(PutObjectOutput::default()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = {
        let mut builder = S3ServiceBuilder::new(MemoryBucket::default());
        builder.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
        builder.build()
    };

    let listener = TcpListener::bind(ADDRESS).await?;
    println!("Mock S3 server running at http://{ADDRESS}");
    println!("Bucket {BUCKET:?} created. Press ctrl+c to stop.");

    let http = ConnBuilder::new(TokioExecutor::new());
    loop {
        let (socket, _) = tokio::select! {
            accepted = listener.accept() => accepted?,
            _ = tokio::signal::ctrl_c() => return Ok(()),
        };
        let service = service.clone();
        let logged = service_fn(move |request: Request<Incoming>| {
            let method = request.method().clone();
            let path = request.uri().path().to_owned();
            let started = Instant::now();
            // S3Service has an inherent `call` taking its own body type, which method syntax would
            // pick.
            let response = Service::call(&service, request);
            async move {
                let response = response.await;
                if let Ok(response) = &response {
                    println!(
                        "{method} {path} status={} duration={:?}",
                        response.status().as_u16(),
                        started.elapsed()
                    );
                }
                response
            }
        });
        let connection = http
            .serve_connection(TokioIo::new(socket), logged)
            .into_owned();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {e}");
            }
        });
    }
}
