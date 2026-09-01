use base64::prelude::*;
use http_body_util::Empty;
use hyper::{body::Bytes, header};
use hyper_util::{
    client::legacy::{
        Client,
        connect::{Connected, Connection},
    },
    rt::{TokioExecutor, TokioIo},
};
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tonic::transport::{Channel, Endpoint};
use tower::{Service, service_fn};

#[cfg(unix)]
use tokio::net::UnixStream;

/// Options for HTTP CONNECT proxy.
#[derive(Clone, Debug, bon::Builder)]
#[builder(start_fn = new, on(String, into))]
#[non_exhaustive]
pub struct HttpConnectProxyOptions {
    /// The host:port to proxy through for TCP, or unix:/path/to/unix.sock for
    /// Unix socket (which means it must start with "unix:/").
    #[builder(start_fn)]
    pub target_addr: String,
    /// Optional HTTP basic auth for the proxy as user/pass tuple.
    pub basic_auth: Option<(String, String)>,
}

impl HttpConnectProxyOptions {
    /// Create a channel from the given endpoint that uses the HTTP CONNECT proxy.
    pub async fn connect_endpoint(
        &self,
        endpoint: &Endpoint,
    ) -> Result<Channel, tonic::transport::Error> {
        let proxy_options = self.clone();
        let svc_fn = service_fn(move |uri: tonic::transport::Uri| {
            let proxy_options = proxy_options.clone();
            async move { proxy_options.connect(uri).await }
        });
        endpoint.connect_with_connector(svc_fn).await
    }

    async fn connect(
        &self,
        uri: tonic::transport::Uri,
    ) -> anyhow::Result<hyper::upgrade::Upgraded> {
        let uri = ensure_connect_authority_port(uri);
        debug!("Connecting to {} via proxy at {}", uri, self.target_addr);
        // Create CONNECT request
        let mut req_build = hyper::Request::builder().method("CONNECT").uri(uri);
        if let Some((user, pass)) = &self.basic_auth {
            let creds = BASE64_STANDARD.encode(format!("{user}:{pass}"));
            req_build = req_build.header(header::PROXY_AUTHORIZATION, format!("Basic {creds}"));
        }
        let req = req_build.body(Empty::<Bytes>::new())?;

        // We have to create a client with a specific connector because Hyper is
        // not letting us change the HTTP/2 authority
        let client = Client::builder(TokioExecutor::new())
            .build(OverrideAddrConnector(self.target_addr.clone()));

        // Send request
        let res = client.request(req).await?;
        if res.status().is_success() {
            Ok(hyper::upgrade::on(res).await?)
        } else {
            Err(anyhow::anyhow!(
                "CONNECT call failed with status: {}",
                res.status()
            ))
        }
    }
}

#[derive(Clone)]
struct OverrideAddrConnector(String);

impl Service<hyper::Uri> for OverrideAddrConnector {
    type Response = TokioIo<ProxyStream>;

    type Error = anyhow::Error;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _ctx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: hyper::Uri) -> Self::Future {
        let target_addr = self.0.clone();
        let fut = async move {
            Ok(TokioIo::new(
                ProxyStream::connect(target_addr.as_str()).await?,
            ))
        };
        Box::pin(fut)
    }
}

enum ProxyStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl ProxyStream {
    async fn connect(target_addr: &str) -> anyhow::Result<Self> {
        if target_addr.starts_with("unix:/") {
            #[cfg(unix)]
            {
                Ok(ProxyStream::Unix(
                    UnixStream::connect(&target_addr[5..]).await?,
                ))
            }
            #[cfg(not(unix))]
            {
                Err(anyhow::anyhow!(
                    "Unix sockets are not supported on this platform"
                ))
            }
        } else {
            Ok(ProxyStream::Tcp(TcpStream::connect(target_addr).await?))
        }
    }
}

impl AsyncRead for ProxyStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ProxyStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            ProxyStream::Tcp(s) => s.is_write_vectored(),
            #[cfg(unix)]
            ProxyStream::Unix(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl Connection for ProxyStream {
    fn connected(&self) -> Connected {
        match self {
            ProxyStream::Tcp(s) => s.connected(),
            // There is no special connected metadata for Unix sockets
            #[cfg(unix)]
            ProxyStream::Unix(_) => Connected::new(),
        }
    }
}

/// Ensure the URI authority includes an explicit port so that hyper emits a
/// RFC 9110-compliant CONNECT request-target (authority-form requires host:port).
fn ensure_connect_authority_port(uri: tonic::transport::Uri) -> tonic::transport::Uri {
    if uri.port().is_some() {
        return uri;
    }
    let port = match uri.scheme_str() {
        Some("https") => 443,
        Some("http") => 80,
        _ => return uri,
    };
    let mut parts = uri.into_parts();
    if let Some(ref authority) = parts.authority
        && let Ok(new_auth) = format!("{}:{}", authority.host(), port).parse()
    {
        parts.authority = Some(new_auth);
    }
    tonic::transport::Uri::from_parts(parts).expect("adding port to valid URI should not fail")
}

#[cfg(test)]
mod tests {
    use super::{HttpConnectProxyOptions, ProxyStream};
    use crate::{
        Client, ClientOptions, Connection as TemporalConnection, ConnectionOptions, RetryOptions,
        grpc::WorkflowService,
    };
    use base64::prelude::*;
    use futures_util::{FutureExt, future::BoxFuture};
    use http::{Request, Response};
    use http_body_util::Empty;
    use hyper::{
        body::{Bytes, Incoming},
        server::conn::http1,
        service::service_fn,
    };
    use hyper_util::rt::TokioIo;
    use std::{
        convert::Infallible,
        io,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };
    use temporalio_common::protos::temporal::api::workflowservice::v1::ListNamespacesRequest;
    #[cfg(unix)]
    use tokio::net::UnixListener;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
        sync::oneshot,
    };
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{IntoRequest, body::Body, server::NamedService, transport::Server};
    use tower::Service;
    use tracing::warn;
    use url::Url;

    #[derive(Clone)]
    struct FakeWorkflowService<F>(F);

    impl<F> Service<Request<Body>> for FakeWorkflowService<F>
    where
        F: FnMut(Request<Body>) -> BoxFuture<'static, Response<Body>>,
    {
        type Response = Response<Body>;
        type Error = Infallible;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request<Body>) -> Self::Future {
            let response = (self.0)(request);
            async move { Ok(response.await) }.boxed()
        }
    }

    impl<F> NamedService for FakeWorkflowService<F> {
        const NAME: &'static str = "temporal.api.workflowservice.v1.WorkflowService";
    }

    struct FakeServer {
        addr: std::net::SocketAddr,
        shutdown_tx: oneshot::Sender<()>,
    }

    async fn fake_server<F>(response_maker: F) -> FakeServer
    where
        F: FnMut(Request<Body>) -> BoxFuture<'static, Response<Body>>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let listener = TcpListener::bind("[::]:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(FakeWorkflowService(response_maker))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });
        FakeServer { addr, shutdown_tx }
    }

    struct HttpProxy {
        proxy_hits: Arc<AtomicUsize>,
        shutdown_tx: oneshot::Sender<()>,
    }

    impl HttpProxy {
        fn spawn_tcp(listener: TcpListener) -> Self {
            Self::spawn(ProxyListener::Tcp(listener))
        }

        #[cfg(unix)]
        fn spawn_unix(listener: UnixListener) -> Self {
            Self::spawn(ProxyListener::Unix(listener))
        }

        fn spawn(listener: ProxyListener) -> Self {
            let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
            let proxy_hits = Arc::new(AtomicUsize::new(0));
            let proxy_hits_for_task = proxy_hits.clone();
            tokio::spawn(async move {
                loop {
                    let proxy_hits = proxy_hits_for_task.clone();
                    tokio::select! {
                        _ = &mut shutdown_rx => break,
                        stream = listener.accept() => {
                            let stream = match stream {
                                Ok(stream) => stream,
                                Err(error) => {
                                    warn!(%error, "Proxy accept failed");
                                    continue;
                                }
                            };
                            tokio::spawn(async move {
                                if let Err(error) = http1::Builder::new()
                                    .serve_connection(
                                        TokioIo::new(stream),
                                        service_fn(move |request| {
                                            handle_connect(request, proxy_hits.clone())
                                        }),
                                    )
                                    .with_upgrades()
                                    .await
                                {
                                    warn!(%error, "Proxy connection failed");
                                }
                            });
                        }
                    }
                }
            });
            Self {
                proxy_hits,
                shutdown_tx,
            }
        }

        fn hit_count(&self) -> usize {
            self.proxy_hits.load(Ordering::SeqCst)
        }

        fn shutdown(self) {
            let _ = self.shutdown_tx.send(());
        }
    }

    async fn handle_connect(
        request: Request<Incoming>,
        counter: Arc<AtomicUsize>,
    ) -> Result<Response<Empty<Bytes>>, hyper::Error> {
        if request.method() != hyper::Method::CONNECT {
            return Ok(Response::builder()
                .status(hyper::StatusCode::METHOD_NOT_ALLOWED)
                .body(Empty::new())
                .unwrap());
        }

        counter.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            if let Some(addr) = request
                .uri()
                .authority()
                .map(|authority| authority.as_str())
                && let Ok(mut server_stream) = TcpStream::connect(addr).await
                && let Ok(upgraded) = hyper::upgrade::on(request).await
            {
                let mut upgraded = TokioIo::new(upgraded);
                let _ = tokio::io::copy_bidirectional(&mut upgraded, &mut server_stream).await;
            }
        });

        Ok(Response::builder()
            .status(hyper::StatusCode::OK)
            .body(Empty::new())
            .unwrap())
    }

    enum ProxyListener {
        Tcp(TcpListener),
        #[cfg(unix)]
        Unix(UnixListener),
    }

    impl ProxyListener {
        async fn accept(&self) -> io::Result<ProxyStream> {
            match self {
                ProxyListener::Tcp(listener) => listener
                    .accept()
                    .await
                    .map(|(stream, _)| ProxyStream::Tcp(stream)),
                #[cfg(unix)]
                ProxyListener::Unix(listener) => listener
                    .accept()
                    .await
                    .map(|(stream, _)| ProxyStream::Unix(stream)),
            }
        }
    }

    struct CapturedConnect {
        request_line: String,
        headers: Vec<String>,
    }

    async fn mock_proxy() -> (String, tokio::task::JoinHandle<CapturedConnect>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).await.unwrap();
            let mut headers = Vec::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                headers.push(line.trim_end().to_string());
            }
            reader
                .into_inner()
                .write_all(b"HTTP/1.1 200 OK\r\n\r\n")
                .await
                .unwrap();
            CapturedConnect {
                request_line,
                headers,
            }
        });
        (addr, handle)
    }

    #[rstest::rstest]
    #[case("https://example.com/some/path", "CONNECT example.com:443 HTTP/1.1")]
    #[case("http://example.com", "CONNECT example.com:80 HTTP/1.1")]
    #[case("https://example.com:7233", "CONNECT example.com:7233 HTTP/1.1")]
    #[tokio::test]
    async fn connect_request_line(#[case] uri: &str, #[case] expected: &str) {
        let (proxy_addr, handle) = mock_proxy().await;
        let opts = HttpConnectProxyOptions::new(proxy_addr).build();
        let uri: tonic::transport::Uri = uri.parse().unwrap();
        let _ = opts.connect(uri).await;

        let captured = handle.await.unwrap();
        assert_eq!(captured.request_line.trim(), expected);
    }

    #[tokio::test]
    async fn connect_includes_basic_auth() {
        let (proxy_addr, handle) = mock_proxy().await;
        let opts = HttpConnectProxyOptions::new(proxy_addr)
            .basic_auth(("user".to_string(), "pass".to_string()))
            .build();
        let uri: tonic::transport::Uri = "https://example.com:7233".parse().unwrap();
        let _ = opts.connect(uri).await;

        let captured = handle.await.unwrap();
        let creds = BASE64_STANDARD.encode("user:pass");
        let auth_header = captured
            .headers
            .iter()
            .find(|h| h.to_lowercase().starts_with("proxy-authorization:"))
            .expect("missing proxy-authorization header");
        assert_eq!(
            auth_header.trim(),
            format!("proxy-authorization: Basic {creds}")
        );
    }

    #[tokio::test]
    async fn connection_uses_http_connect_proxy() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_for_server = call_count.clone();
        let server = fake_server(move |_| {
            call_count_for_server.fetch_add(1, Ordering::SeqCst);
            async { Response::new(Body::empty()) }.boxed()
        })
        .await;

        let tcp_proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_proxy_addr = tcp_proxy_listener.local_addr().unwrap();
        let tcp_proxy = HttpProxy::spawn_tcp(tcp_proxy_listener);

        let mut options = ConnectionOptions::new(
            Url::parse(&format!("http://[::1]:{}", server.addr.port())).unwrap(),
        )
        .retry_options(RetryOptions::no_retries())
        .skip_get_system_info(true)
        .build();

        let connection = TemporalConnection::connect(options.clone()).await.unwrap();
        let client_options = ClientOptions::new("my-namespace").build();
        let client = Client::new(connection, client_options).unwrap();
        let _ = WorkflowService::list_namespaces(
            &mut client.clone(),
            ListNamespacesRequest::default().into_request(),
        )
        .await;
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert_eq!(tcp_proxy.hit_count(), 0);

        options.http_connect_proxy =
            Some(HttpConnectProxyOptions::new(tcp_proxy_addr.to_string()).build());
        options.dns_load_balancing = None;
        let connection = TemporalConnection::connect(options.clone()).await.unwrap();
        let client_options = ClientOptions::new("my-namespace").build();
        let proxied_client = Client::new(connection, client_options).unwrap();
        let _ = WorkflowService::list_namespaces(
            &mut proxied_client.clone(),
            ListNamespacesRequest::default().into_request(),
        )
        .await;
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        assert_eq!(tcp_proxy.hit_count(), 1);

        #[cfg(unix)]
        {
            let socket_dir = tempfile::tempdir().unwrap();
            let socket_path = socket_dir.path().join("http-proxy.sock");
            let unix_proxy = HttpProxy::spawn_unix(UnixListener::bind(&socket_path).unwrap());

            options.http_connect_proxy = Some(
                HttpConnectProxyOptions::new(format!("unix:{}", socket_path.display())).build(),
            );
            let connection = TemporalConnection::connect(options).await.unwrap();
            let client_options = ClientOptions::new("my-namespace").build();
            let proxied_client = Client::new(connection, client_options).unwrap();
            let _ = WorkflowService::list_namespaces(
                &mut proxied_client.clone(),
                ListNamespacesRequest::default().into_request(),
            )
            .await;
            assert_eq!(call_count.load(Ordering::SeqCst), 3);
            assert_eq!(unix_proxy.hit_count(), 1);

            unix_proxy.shutdown();
        }

        let _ = server.shutdown_tx.send(());
        tcp_proxy.shutdown();
    }
}
