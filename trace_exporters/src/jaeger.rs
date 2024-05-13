use std::{
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    num::NonZeroU64,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;

use iox_time::TimeProvider;
use observability_deps::tracing::*;
use trace::span::Span;

use crate::thrift::agent::{AgentSyncClient, TAgentSyncClient};
use crate::thrift::jaeger;
use crate::{export::AsyncExport, rate_limiter::RateLimiter};
use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol};

mod span;

/// Defines the desired UDP socket send buffer size in bytes.
const WANT_SOCKET_SEND_BUFFER_SIZE: usize = 2 * 1024 * 1024; // 2MiB

/// A key=value pair for span annotations.
#[derive(Debug, Clone)]
pub struct JaegerTag {
    key: String,
    value: String,
}

impl JaegerTag {
    /// Create a new static tag for all jaeger spans.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Key.
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl From<JaegerTag> for jaeger::Tag {
    fn from(t: JaegerTag) -> Self {
        Self::new(
            t.key,
            jaeger::TagType::String,
            Some(t.value),
            None,
            None,
            None,
            None,
        )
    }
}

impl FromStr for JaegerTag {
    type Err = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('=').collect::<Vec<_>>();
        match *parts {
            [key, value] if !key.is_empty() && !value.is_empty() => Ok(Self::new(key, value)),
            _ => Err(format!("invalid key=value pair ({s})").into()),
        }
    }
}

/// `JaegerAgentExporter` receives span data and writes it over UDP to a local jaeger agent
///
/// Note: will drop data if the UDP socket would block
pub struct JaegerAgentExporter {
    /// The name of the service
    service_name: String,

    /// The agent client that encodes messages
    client:
        AgentSyncClient<TCompactInputProtocol<NoopReader>, TCompactOutputProtocol<MessageWriter>>,

    /// Spans should be assigned a sequential sequence number
    /// to allow jaeger to better detect dropped spans
    next_sequence: i64,

    /// Optional static tags to annotate every span with.
    tags: Option<Vec<jaeger::Tag>>,

    /// Rate limiter
    rate_limiter: RateLimiter,
}

impl JaegerAgentExporter {
    pub fn new<E: ToSocketAddrs + std::fmt::Display>(
        service_name: String,
        agent_endpoint: E,
        time_provider: Arc<dyn TimeProvider>,
        max_msgs_per_second: NonZeroU64,
    ) -> super::Result<Self> {
        info!(%agent_endpoint, %service_name, "Creating jaeger tracing exporter");
        let remote_addr = agent_endpoint.to_socket_addrs()?.next().ok_or_else(|| {
            super::Error::ResolutionError {
                address: agent_endpoint.to_string(),
            }
        })?;

        let local_addr: SocketAddr = if remote_addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()
        .unwrap();

        let socket = UdpSocket::bind(local_addr)?;
        socket.set_nonblocking(true)?;
        socket.connect(remote_addr)?;

        // Attempt to grow the socket send buffer.
        let socket = set_send_buffer_size(socket, WANT_SOCKET_SEND_BUFFER_SIZE);

        let client = AgentSyncClient::new(
            TCompactInputProtocol::new(NoopReader::default()),
            TCompactOutputProtocol::new(MessageWriter::new(socket)),
        );

        Ok(Self {
            service_name,
            client,
            next_sequence: 0,
            tags: None,
            rate_limiter: RateLimiter::new(max_msgs_per_second, time_provider),
        })
    }

    /// Annotate all spans emitted by this exporter with the specified static
    /// tags.
    pub fn with_tags(self, tags: &[JaegerTag]) -> Self {
        debug!(?tags, "setting static jaeger span tags");
        let tags = Some(tags.iter().cloned().map(Into::into).collect());
        Self { tags, ..self }
    }

    fn make_batch(&mut self, spans: Vec<Span>) -> jaeger::Batch {
        let seq_no = Some(self.next_sequence);
        self.next_sequence += 1;
        jaeger::Batch {
            process: jaeger::Process {
                service_name: self.service_name.clone(),
                tags: self.tags.clone(),
            },
            spans: spans
                .into_iter()
                .filter_map(|span| match jaeger::Span::try_from(span) {
                    Ok(span) => Some(span),
                    Err(e) => {
                        warn!(
                            %e,
                            "cannot convert span to jaeger format",
                        );
                        None
                    }
                })
                .collect(),
            seq_no,
            stats: None,
        }
    }
}

/// Attempt to increase the socket send buffer size to `want`.
///
/// If the buffer size cannot be set, a lower value is attempted until the
/// buffer is expanded (to this lower value) or the initial buffer size is
/// reached.
///
/// This method never shrinks the buffer size.
fn set_send_buffer_size(socket: UdpSocket, mut want: usize) -> UdpSocket {
    let sock2_sock = socket2::Socket::from(socket);

    // The starting socket size.
    //
    // If this isn't known, try to grow the buffer until the target reduces to
    // 1024 bytes. If none of the grow attempts are successful before hitting
    // the current size (or 1024 bytes if unknown) then leave the default value
    // as-is.
    let starting_size = sock2_sock.send_buffer_size().ok().unwrap_or(1024);

    // Attempt to set the desired buffer size.
    //
    // If setting the desired size fails, incrementally reduce the requested
    // buffer size until it either succeeds, or reaches the starting value
    // (never shrink the buffer smaller than the starting value).
    while want > starting_size {
        match sock2_sock.set_send_buffer_size(want) {
            Ok(_) => break,
            Err(e) => {
                warn!(
                    starting_size,
                    want,
                    error=%e,
                    "failed to grow udp socket buffer size"
                );
                // Halve the desired buffer size for the next attempt.
                want /= 2;
            }
        };
    }

    // The actual value set is the greater of the last tried value, or the
    // starting value if all attempts failed.
    let got = want.max(starting_size);

    debug!(buffer_size=%got, "resized udp socket send buffer");

    UdpSocket::from(sock2_sock)
}

#[async_trait]
impl AsyncExport for JaegerAgentExporter {
    async fn export(&mut self, spans: Vec<Span>) {
        let batch = self.make_batch(spans);

        // The Jaeger UDP protocol provides no backchannel and therefore no way for us to know about backpressure or
        // dropped messages. To make dropped messages (by the OS, network, or Jaeger itself) less likely, we employ a
        // simple rate limit.
        self.rate_limiter.send().await;

        if let Err(e) = self.client.emit_batch(batch) {
            let e = NiceThriftError::from(e);

            // not a user-visible error but only a monitoring outage, print on info level
            // Ref: https://github.com/influxdata/influxdb_iox/issues/9726
            info!(%e, "error writing batch to jaeger agent")
        }
    }
}

/// Thrift error formatting is messy, try better.
///
/// See <https://github.com/influxdata/influxdb_iox/issues/9726>.
#[derive(Debug)]
struct NiceThriftError(thrift::Error);

impl From<thrift::Error> for NiceThriftError {
    fn from(e: thrift::Error) -> Self {
        Self(e)
    }
}

impl std::fmt::Display for NiceThriftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            thrift::Error::Transport(e) => {
                let kind = match e.kind {
                    thrift::TransportErrorKind::Unknown => "unknown",
                    thrift::TransportErrorKind::NotOpen => "not open",
                    thrift::TransportErrorKind::AlreadyOpen => "already open",
                    thrift::TransportErrorKind::TimedOut => "timed out",
                    thrift::TransportErrorKind::EndOfFile => "end of file",
                    thrift::TransportErrorKind::NegativeSize => "negative size message",
                    thrift::TransportErrorKind::SizeLimit => "message too long",
                    _ => "unknown variant",
                };

                write!(f, "transport: {}: {}", kind, e.message)
            }
            thrift::Error::Protocol(e) => {
                let kind = match e.kind {
                    thrift::ProtocolErrorKind::Unknown => "unknown",
                    thrift::ProtocolErrorKind::InvalidData => "bad data",
                    thrift::ProtocolErrorKind::NegativeSize => "negative message size",
                    thrift::ProtocolErrorKind::SizeLimit => "message too long",
                    thrift::ProtocolErrorKind::BadVersion => "invalid thrift version",
                    thrift::ProtocolErrorKind::NotImplemented => "not implemented",
                    thrift::ProtocolErrorKind::DepthLimit => "maximum skip depth reached",
                    _ => "unknown variant",
                };

                write!(f, "protocol: {}: {}", kind, e.message)
            }
            thrift::Error::Application(e) => {
                let kind = match e.kind {
                    thrift::ApplicationErrorKind::Unknown => "unknown",
                    thrift::ApplicationErrorKind::UnknownMethod => "unknown service method",
                    thrift::ApplicationErrorKind::InvalidMessageType => {
                        "wrong message type received"
                    }
                    thrift::ApplicationErrorKind::WrongMethodName => {
                        "unknown method reply received"
                    }
                    thrift::ApplicationErrorKind::BadSequenceId => "out of order sequence id",
                    thrift::ApplicationErrorKind::MissingResult => "missing method result",
                    thrift::ApplicationErrorKind::InternalError => "remote service threw exception",
                    thrift::ApplicationErrorKind::ProtocolError => "protocol error",
                    thrift::ApplicationErrorKind::InvalidTransform => "invalid transform",
                    thrift::ApplicationErrorKind::InvalidProtocol => "invalid protocol requested",
                    thrift::ApplicationErrorKind::UnsupportedClientType => {
                        "unsupported protocol client"
                    }
                    _ => "unknown variant",
                };

                write!(f, "application: {}: {}", kind, e.message)
            }
            thrift::Error::User(e) => write!(f, "user: {e}"),
        }
    }
}

/// `NoopReader` is a `std::io::Read` that never returns any data
#[derive(Debug, Default)]
struct NoopReader {}

impl std::io::Read for NoopReader {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(0)
    }
}

/// A `MessageWriter` only writes entire message payloads to the provided UDP socket
///
/// If the UDP socket would block, drops the packet
struct MessageWriter {
    buf: Vec<u8>,
    socket: UdpSocket,
}

impl MessageWriter {
    fn new(socket: UdpSocket) -> Self {
        Self {
            buf: vec![],
            socket,
        }
    }
}

impl std::io::Write for MessageWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let message_len = self.buf.len();
        let r = self.socket.send(&self.buf);
        self.buf.clear();
        match r {
            Ok(written) => {
                if written != message_len {
                    // In the event a message is truncated, there isn't an obvious way to recover
                    //
                    // The Thrift protocol is normally used on top of a reliable stream,
                    // e.g. TCP, and it is a bit of a hack to send it over UDP
                    //
                    // Jaeger requires that each thrift Message is encoded in exactly one UDP
                    // packet, as this ensures it either arrives in its entirety or not at all
                    //
                    // If for whatever reason the packet is truncated, the agent will fail to
                    // to decode it, likely due to a missing stop-field, and discard it
                    error!(%written, %message_len, "jaeger agent exporter failed to write message as single UDP packet");
                }
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                error!("jaeger agent exporter would have blocked - dropping message");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::thrift::agent::{AgentSyncHandler, AgentSyncProcessor};
    use chrono::{TimeZone, Utc};
    use iox_time::SystemProvider;
    use std::borrow::Cow;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use thrift::server::TProcessor;
    use thrift::transport::TBufferChannel;
    use trace::ctx::{SpanContext, SpanId, TraceId};
    use trace::span::{MetaValue, SpanEvent, SpanStatus};

    struct TestHandler {
        batches: Arc<Mutex<Vec<jaeger::Batch>>>,
    }

    impl AgentSyncHandler for TestHandler {
        fn handle_emit_zipkin_batch(
            &self,
            _spans: Vec<crate::thrift::zipkincore::Span>,
        ) -> thrift::Result<()> {
            unimplemented!()
        }

        fn handle_emit_batch(&self, batch: jaeger::Batch) -> thrift::Result<()> {
            self.batches.lock().unwrap().push(batch);
            Ok(())
        }
    }

    /// Wraps a UdpSocket and a buffer the size of the max UDP datagram and provides
    /// `std::io::Read` on this buffer's contents, ensuring that reads are not truncated
    struct Reader {
        socket: UdpSocket,
        buffer: Box<[u8; 65535]>,
        idx: usize,
        len: usize,
    }

    impl Reader {
        pub fn new(socket: UdpSocket) -> Self {
            Self {
                socket,
                buffer: Box::new([0; 65535]),
                idx: 0,
                len: 0,
            }
        }
    }

    impl std::io::Read for Reader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.idx == self.len {
                self.idx = 0;
                self.len = self.socket.recv(self.buffer.as_mut())?;
            }
            let to_read = buf.len().min(self.len - self.idx);
            buf.copy_from_slice(&self.buffer[self.idx..(self.idx + to_read)]);
            self.idx += to_read;
            Ok(to_read)
        }
    }

    #[test]
    fn test_jaeger_tag_from_str() {
        "".parse::<JaegerTag>().expect_err("empty tag should fail");
        "key"
            .parse::<JaegerTag>()
            .expect_err("no value should fail");
        "key="
            .parse::<JaegerTag>()
            .expect_err("no value should fail");
        "key=="
            .parse::<JaegerTag>()
            .expect_err("no value should fail");
        "=value"
            .parse::<JaegerTag>()
            .expect_err("no key should fail");
        "==value"
            .parse::<JaegerTag>()
            .expect_err("empty key should fail");
        "key==value"
            .parse::<JaegerTag>()
            .expect_err("too many = should fail");
        "=".parse::<JaegerTag>()
            .expect_err("empty key value should fail");
        "key=value"
            .parse::<JaegerTag>()
            .expect("valid form should succeed");
    }

    #[tokio::test]
    async fn test_jaeger() {
        let server = UdpSocket::bind("0.0.0.0:0").unwrap();
        server
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();

        let tags = [JaegerTag::new("bananas", "great")];
        let address = server.local_addr().unwrap();
        let mut exporter = JaegerAgentExporter::new(
            "service_name".to_string(),
            address,
            Arc::new(SystemProvider::new()),
            NonZeroU64::new(1_000).unwrap(),
        )
        .unwrap()
        .with_tags(&tags);

        // Encoded form of tags.
        let want_tags = [jaeger::Tag {
            key: "bananas".into(),
            v_str: Some("great".into()),
            v_type: jaeger::TagType::String,
            v_double: None,
            v_bool: None,
            v_long: None,
            v_binary: None,
        }];

        let batches = Arc::new(Mutex::new(vec![]));

        let mut processor_input = TCompactInputProtocol::new(Reader::new(server));
        let mut processor_output = TCompactOutputProtocol::new(TBufferChannel::with_capacity(0, 0));
        let processor = AgentSyncProcessor::new(TestHandler {
            batches: Arc::clone(&batches),
        });

        let ctx = SpanContext {
            trace_id: TraceId::new(43434).unwrap(),
            parent_span_id: None,
            span_id: SpanId::new(3495993).unwrap(),
            links: vec![],
            collector: None,
            sampled: true,
        };
        let mut span = ctx.child("foo");
        span.ctx.links = vec![
            (TraceId::new(12).unwrap(), SpanId::new(123).unwrap()),
            (TraceId::new(45).unwrap(), SpanId::new(456).unwrap()),
        ];
        span.status = SpanStatus::Ok;
        span.events = vec![SpanEvent {
            time: Utc.timestamp_nanos(200000),
            msg: "hello".into(),
            metadata: HashMap::from([(Cow::from("evt_md"), MetaValue::Int(42))]),
        }];
        span.start = Some(Utc.timestamp_nanos(100000));
        span.end = Some(Utc.timestamp_nanos(300000));
        span.metadata = HashMap::from([(Cow::from("span_md"), MetaValue::Int(1337))]);

        exporter.export(vec![span.clone(), span.clone()]).await;
        exporter.export(vec![span.clone()]).await;

        processor
            .process(&mut processor_input, &mut processor_output)
            .unwrap();

        processor
            .process(&mut processor_input, &mut processor_output)
            .unwrap();

        let batches = batches.lock().unwrap();
        assert_eq!(batches.len(), 2);

        let b1 = &batches[0];

        assert_eq!(b1.spans.len(), 2);
        assert_eq!(b1.process.service_name.as_str(), "service_name");
        assert_eq!(b1.seq_no.unwrap(), 0);
        let got_tags = b1
            .process
            .tags
            .as_ref()
            .expect("expected static process tags");
        assert_eq!(got_tags, &want_tags);

        let b2 = &batches[1];
        assert_eq!(b2.spans.len(), 1);
        assert_eq!(b2.process.service_name.as_str(), "service_name");
        assert_eq!(b2.seq_no.unwrap(), 1);
        let got_tags = b2
            .process
            .tags
            .as_ref()
            .expect("expected static process tags");
        assert_eq!(got_tags, &want_tags);

        // Span tags should be constant
        assert_eq!(b1.process.tags, b2.process.tags);

        let b1_s0 = &b1.spans[0];

        assert_eq!(b1_s0, &b1.spans[1]);
        assert_eq!(b1_s0, &b2.spans[0]);

        assert_eq!(b1_s0.span_id, span.ctx.span_id.get() as i64);
        assert_eq!(
            b1_s0.parent_span_id,
            span.ctx.parent_span_id.unwrap().get() as i64
        );

        // test links
        let b1_s0_refs = b1_s0.references.as_ref().unwrap();
        assert_eq!(b1_s0_refs.len(), 2);
        let b1_s0_r0 = &b1_s0_refs[0];
        let b1_s0_r1 = &b1_s0_refs[1];
        assert_eq!(b1_s0_r0.span_id, span.ctx.links[0].1.get() as i64);
        assert_eq!(b1_s0_r1.span_id, span.ctx.links[1].1.get() as i64);

        // microseconds not nanoseconds
        assert_eq!(b1_s0.start_time, 100);
        assert_eq!(b1_s0.duration, 200);

        let logs = b1_s0.logs.as_ref().unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].timestamp, 200);
        assert_eq!(logs[0].fields.len(), 2);
        assert_eq!(logs[0].fields[0].key.as_str(), "event");
        assert_eq!(logs[0].fields[0].v_str.as_ref().unwrap().as_str(), "hello");
        assert_eq!(logs[0].fields[1].key.as_str(), "evt_md");
        assert_eq!(logs[0].fields[1].v_long.unwrap(), 42);

        let tags = b1_s0.tags.as_ref().unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].key.as_str(), "ok");
        assert!(tags[0].v_bool.unwrap());
        assert_eq!(tags[1].key.as_str(), "span_md");
        assert_eq!(tags[1].v_long.unwrap(), 1337);
    }

    #[test]
    fn test_resolve() {
        JaegerAgentExporter::new(
            "service_name".to_string(),
            "localhost:8082",
            Arc::new(SystemProvider::new()),
            NonZeroU64::new(1_000).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn test_send_buffer_size() {
        const EXCESSIVE_BUFFER_SIZE: usize = 100 * 1024 * 1024;

        let socket = UdpSocket::bind("0.0.0.0:0").expect("must bind random UDP socket");
        let socket2 = socket2::Socket::from(socket);

        // Shrink the socket to a tiny buffer size, and then try and grow it to
        // something bigger.
        socket2.set_send_buffer_size(1).unwrap();

        let socket = UdpSocket::from(socket2);
        let socket = set_send_buffer_size(socket, EXCESSIVE_BUFFER_SIZE);

        // Convert it back into a socket2 socket to inspect.
        let socket2 = socket2::Socket::from(socket);

        // The system running the test almost certainly does not allow 100MiB
        // socket buffers, but it should still have been expanded to something
        // bigger than 1 as a result of the backoff logic.
        assert!(socket2.send_buffer_size().unwrap() > 1);

        // But it shouldn't have gone full bananas and set something more than
        // what was asked for.
        assert!(socket2.send_buffer_size().unwrap() <= EXCESSIVE_BUFFER_SIZE);
    }
}
