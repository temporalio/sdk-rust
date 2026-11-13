//! An in-memory S3-compatible server, so the sample runs without an AWS account or Docker. It
//! implements only the three requests the S3 storage driver makes: `HeadObject`, `PutObject`, and
//! `GetObject`. It does not check request signatures.

use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header,
    service::service_fn,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::net::TcpListener;

const ADDRESS: &str = "localhost:9100";
const BUCKET: &str = "temporal-payloads";

type Objects = Arc<Mutex<HashMap<String, Bytes>>>;

async fn handle(
    objects: Objects,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let Some(key) = request
        .uri()
        .path()
        .strip_prefix(&format!("/{BUCKET}/"))
        .map(str::to_owned)
    else {
        return Ok(error(StatusCode::NOT_FOUND, "NoSuchBucket"));
    };
    let response = match *request.method() {
        Method::HEAD | Method::GET => {
            let data = objects.lock().unwrap().get(&key).cloned();
            match data {
                Some(data) if request.method() == Method::HEAD => Response::builder()
                    .header(header::CONTENT_LENGTH, data.len())
                    .body(Full::default())
                    .unwrap(),
                Some(data) => Response::new(Full::new(data)),
                None => error(StatusCode::NOT_FOUND, "NoSuchKey"),
            }
        }
        Method::PUT => {
            let data = request.into_body().collect().await?.to_bytes();
            objects.lock().unwrap().insert(key, data);
            Response::new(Full::default())
        }
        _ => error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed"),
    };
    Ok(response)
}

fn error(status: StatusCode, code: &str) -> Response<Full<Bytes>> {
    let body = format!("<Error><Code>{code}</Code></Error>");
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Full::new(body.into()))
        .unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let objects = Objects::default();
    let listener = TcpListener::bind(ADDRESS).await?;
    println!("Mock S3 server running at http://{ADDRESS}");
    println!("Bucket {BUCKET:?} created. Press ctrl+c to stop.");

    let http = ConnBuilder::new(TokioExecutor::new());
    loop {
        let (socket, _) = tokio::select! {
            accepted = listener.accept() => accepted?,
            _ = tokio::signal::ctrl_c() => return Ok(()),
        };
        let objects = objects.clone();
        let logged = service_fn(move |request: Request<Incoming>| {
            let method = request.method().clone();
            let path = request.uri().path().to_owned();
            let started = Instant::now();
            let response = handle(objects.clone(), request);
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
