use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    ops::Range,
    sync::Arc,
    time::Duration,
};

use arrow::{
    array::{ArrayRef, StringArray, TimestampNanosecondArray},
    record_batch::RecordBatch,
};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use datafusion_util::{unbounded_memory_pool, MemoryStream};
use hyper::{
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server,
};
use iox_time::Time;
use object_store::ObjectStore;
use parking_lot::Mutex;
use parquet_file::{metadata::IoxMetadata, serialize::to_parquet_bytes, ParquetFilePath};
use schema::{builder::SchemaBuilder, InfluxFieldType, TIME_DATA_TIMEZONE};
use std::net::SocketAddr;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

use crate::data_types::{
    KeyspaceResponseBody, ServiceNode, X_RANGE_END_HEADER, X_RANGE_START_HEADER,
};

pub async fn create_mock_raw_parquet_file(
    parquet_path: ParquetFilePath,
    creation_timestamp: DateTime<Utc>,
) -> (Vec<u8>, parquet::format::FileMetaData) {
    let meta = IoxMetadata {
        object_store_id: parquet_path.object_store_id(),
        creation_timestamp: Time::from_date_time(creation_timestamp)
            .checked_add(Duration::from_secs(30))
            .expect("should set L1 time as 30 seconds greater"),
        namespace_id: parquet_path.namespace_id(),
        namespace_name: "bananas".into(),
        table_id: parquet_path.table_id(),
        table_name: "platanos".into(),
        partition_key: "potato".into(),
        compaction_level: data_types::CompactionLevel::FileNonOverlapped,
        sort_key: None,
        max_l0_created_at: Time::from_date_time(creation_timestamp),
    };

    let array = StringArray::from_iter([Some("bananas")]);
    let data: ArrayRef = Arc::new(array);

    let timestamps = Arc::new(
        [1647695292000000000]
            .iter()
            .map(|v| Some(*v))
            .collect::<TimestampNanosecondArray>()
            .with_timezone_opt(TIME_DATA_TIMEZONE()),
    );

    // Build a schema that contains the IOx metadata, ensuring it is
    // correctly populated in the final parquet file's metadata to be read
    // back later in the test.
    let schema = SchemaBuilder::new()
        .influx_field("a", InfluxFieldType::String)
        .timestamp()
        .build()
        .expect("could not create schema")
        .as_arrow();

    let batch = RecordBatch::try_new(schema, vec![data, timestamps]).unwrap();
    let stream = Box::pin(MemoryStream::new(vec![batch.clone()]));

    to_parquet_bytes(stream, &meta, unbounded_memory_pool())
        .await
        .expect("should serialize")
}

#[allow(missing_debug_implementations)]
pub struct MockCacheServer {
    addr: SocketAddr,
    stop: oneshot::Sender<()>,
    join: JoinHandle<()>,
    req_handler: Arc<MockCacheServerRequestHandler>,
}

impl MockCacheServer {
    pub async fn create(addr: &str, _object_store: Arc<dyn ObjectStore>) -> Self {
        let listener = TcpListener::bind(addr)
            .await
            .expect("listener should have bound to addr");
        let addr = listener.local_addr().unwrap();

        let req_handler: Arc<MockCacheServerRequestHandler> =
            Arc::new(MockCacheServerRequestHandler::new(addr.to_string()));

        let handler = Arc::clone(&req_handler);
        let make_svc = make_service_fn(move |_socket: &AddrStream| {
            let handler = Arc::clone(&handler);
            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    let handler = Arc::clone(&handler);
                    async move { Arc::clone(&handler).handle(req) }
                }))
            }
        });

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let join = tokio::spawn(async {
            Server::builder(AddrIncoming::from_listener(listener).unwrap())
                .http2_only(true)
                .serve(make_svc)
                .with_graceful_shutdown(async {
                    rx.await.ok();
                })
                .await
                .unwrap()
        });

        Self {
            addr,
            stop: tx,
            join,
            req_handler,
        }
    }

    pub fn addr(&self) -> String {
        self.addr.to_string()
    }

    pub async fn close(self) {
        self.stop
            .send(())
            .expect("Error sending stop signal to server");
        self.join
            .await
            .expect("Error stopping parquet cache server");
    }

    pub fn was_called(&self, path_and_query: &String) -> bool {
        self.req_handler.called.lock().contains(path_and_query)
    }

    pub fn was_called_with_payload(&self, path_and_query: &String) -> bool {
        self.req_handler.called.lock().contains(path_and_query)
    }

    pub fn respond_with(&self, path_and_query: String, expected: ExpectedResponse) {
        self.req_handler
            .respond_with
            .lock()
            .insert(path_and_query, expected);
    }
}

#[derive(Clone)]
pub struct MockCacheServerRequestHandler {
    pub hostname: String,
    pub called: Arc<Mutex<HashSet<String>>>, // route_&_query
    pub respond_with: Arc<Mutex<HashMap<String, ExpectedResponse>>>, // route_&_query, reponse_payload_body
}

#[derive(Clone, Debug)]
pub struct ExpectedResponse {
    pub bytes: Bytes,
    pub range: Option<Range<usize>>,
}

impl MockCacheServerRequestHandler {
    fn new(hostname: String) -> Self {
        Self {
            hostname,
            called: Default::default(),
            respond_with: Default::default(),
        }
    }

    fn handle(&self, req: Request<Body>) -> Result<Response<hyper::body::Body>, Infallible> {
        let path_and_query = req.uri().path_and_query().unwrap().to_string();

        match (req.method(), req.uri().path()) {
            (&Method::GET, "/keyspace") => {
                self.insert_into_tracker(req);

                let body = KeyspaceResponseBody {
                    nodes: vec![ServiceNode {
                        id: 42,
                        hostname: self.hostname.clone(),
                    }],
                };

                Ok::<_, Infallible>(Response::new(Body::from(build_resp_body(&body))))
            }
            (&Method::GET, "/metadata") => {
                self.insert_into_tracker(req);
                Ok::<_, Infallible>(Response::new(self.get_resp_body(&path_and_query)))
            }
            (&Method::GET, "/object") => {
                // assert range header in mock server
                if let Some(range) = req.headers().get("range") {
                    // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range
                    // <unit>=<range-start>-<range-end>
                    let mut range = range.to_str().unwrap().to_string();
                    range = range
                        .strip_prefix("bytes=")
                        .expect("should start range header with `bytes=`")
                        .to_string();
                    let (start, end) = range.split_at(
                        range
                            .find('-')
                            .expect("should have dash delineating range `start-end`"),
                    );
                    assert!(start.parse::<usize>().is_ok());
                    assert!(end[1..].parse::<usize>().is_ok());
                };

                self.insert_into_tracker(req);

                let range = self
                    .get_size_range(&path_and_query)
                    .expect("should have used respond_with() for mocked response");

                let resp = Response::builder()
                    .header(X_RANGE_START_HEADER, range.start.to_string())
                    .header(X_RANGE_END_HEADER, range.end.to_string())
                    .body(self.get_resp_body(&path_and_query))
                    .expect("should be a valid response");

                Ok::<_, Infallible>(resp)
            }
            (&Method::POST, "/write-hint") => {
                self.insert_into_tracker(req);
                Ok::<_, Infallible>(Response::new(Body::empty()))
            }
            _ => unimplemented!(),
        }
    }

    fn insert_into_tracker(&self, req: Request<Body>) {
        self.called.lock().insert(
            req.uri()
                .path_and_query()
                .expect("should exist")
                .to_string(),
        );
    }

    fn get_resp_body(&self, path_and_query: &String) -> Body {
        match self.respond_with.lock().get(path_and_query) {
            None => Body::empty(),
            Some(expected) => Body::from(expected.clone().bytes),
        }
    }

    fn get_size_range(&self, path_and_query: &String) -> Option<Range<usize>> {
        self.respond_with
            .lock()
            .get(path_and_query)
            .map(|expected| expected.clone().range.unwrap())
    }
}

pub fn build_resp_body<T>(body: &T) -> Bytes
where
    T: Sized + serde::Serialize,
{
    let mut buf = BytesMut::new().writer();
    serde_json::to_writer(&mut buf, body).expect("should write response body");

    buf.into_inner().freeze()
}
