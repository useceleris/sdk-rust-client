use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{Bytes, Message};
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream, client_async_tls_with_config};

use crate::channel::{ChannelInner, ChannelShared};
use crate::command_queue::QueuedPublish;
use crate::constants::{
    MAXIMUM_BATCH_BYTES, MAXIMUM_BATCH_COMMANDS, MAXIMUM_BUFFERED_BYTES, MAXIMUM_PENDING_COMMANDS,
};
use crate::decode::decode_server_message;
use crate::errors::{CelerisError, protocol_error};
use crate::heartbeat::heartbeat;
use crate::messages::ServerMessage;

/// The byte stream under a WebSocket: TCP in production, an in-memory pipe in
/// tests.
pub(crate) trait Io: AsyncRead + AsyncWrite + Unpin + Send {}

impl<Stream: AsyncRead + AsyncWrite + Unpin + Send> Io for Stream {}

pub(crate) type WebSocket = WebSocketStream<MaybeTlsStream<Box<dyn Io>>>;

pub(crate) type SharedSink = Arc<tokio::sync::Mutex<SplitSink<WebSocket, Message>>>;

/// The one transport seam: opens a WebSocket to a URL, which carries the
/// credentials, so tests can substitute an in-memory peer. The error carries
/// nothing, because a dial error quotes the URL.
pub(crate) type Dialer = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<WebSocket, ()>> + Send>> + Send + Sync,
>;

pub(crate) fn web_socket_dialer() -> Dialer {
    Arc::new(|url| Box::pin(dial_web_socket(url)))
} // end fn web_socket_dialer

async fn dial_web_socket(url: String) -> Result<WebSocket, ()> {
    let request = url.as_str().into_client_request().map_err(drop)?;
    let uri = request.uri();
    let secure = uri.scheme_str() == Some("wss");
    let host = uri
        .host()
        .ok_or(())?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let port = uri.port_u16().unwrap_or(if secure { 443 } else { 80 });

    let tcp = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(drop)?;
    tcp.set_nodelay(true).map_err(drop)?;

    // Received messages are never size-checked (LIMIT-01).
    let config = WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None);
    let stream: Box<dyn Io> = Box::new(tcp);
    let connector = Connector::Rustls(tls_configuration());
    let (socket, _response) =
        client_async_tls_with_config(request, stream, Some(config), Some(connector))
            .await
            .map_err(drop)?;

    Ok(socket)
} // end fn dial_web_socket

/// TLS verified against the system's roots, with the crate's own crypto
/// provider, so an application's TLS settings cannot weaken a connection
/// (SEC-02).
fn tls_configuration() -> Arc<ClientConfig> {
    static CONFIGURATION: OnceLock<Arc<ClientConfig>> = OnceLock::new();

    let configuration = CONFIGURATION.get_or_init(|| {
        let mut roots = rustls::RootCertStore::empty();
        roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);

        let provider = Arc::new(rustls::crypto::ring::default_provider());

        let configuration = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("the ring provider supports the default protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();

        Arc::new(configuration)
    });

    Arc::clone(configuration)
} // end fn tls_configuration

/// One command handed to the writer.
pub(crate) struct OutboundFrame {
    data: Bytes,

    /// Set for a publish, which settles once written.
    publish: Option<Arc<QueuedPublish>>,
} // end struct OutboundFrame

/// One socket and the tasks that drive it: a writer that writes handed-off
/// frames in order, packed into batches (BATCH-01), a reader that routes what arrives, and a heartbeat that
/// pings the server and finds a dead path. Guarded by the channel's lock.
pub(crate) struct ConnectionState {
    pub(crate) id: u64,

    /// Frames handed to the writer and not yet written, the batch being
    /// written first: its size is `writing`. They are bounded by count and
    /// bytes, so a maximum-size command always fits an empty writer.
    outbound: VecDeque<OutboundFrame>,
    outbound_bytes: usize,
    writing: usize,

    /// Wakes the writer: a frame was handed off, or the connection closes or
    /// breaks.
    ready: Arc<Notify>,

    /// Asks the writer to flush, then to perform the closing handshake.
    closing: bool,

    /// Asks the writer to stop at once.
    broken: bool,

    pub(crate) heartbeat: HeartbeatState,

    tasks: Vec<AbortHandle>,
    pub(crate) writer: Option<tokio::task::JoinHandle<()>>,
} // end struct ConnectionState

/// What the heartbeat knows about the connection (HEARTBEAT-01).
pub(crate) struct HeartbeatState {
    /// Reading time: how long the reader has waited for a message, the only
    /// time a pong or a message can arrive. It is the finished reads' total,
    /// plus the current read's time since `reading_since`.
    reading_time: Duration,
    reading_since: Option<Instant>,

    /// The reading time when the oldest unanswered ping was sent, if a ping
    /// awaits its pong.
    pub(crate) unanswered_since: Option<Duration>,

    /// When the server was last heard from: the socket opening, a pong or a
    /// received message.
    pub(crate) last_heard: Instant,
} // end struct HeartbeatState

impl HeartbeatState {
    fn start_reading(&mut self) {
        self.reading_since = Some(Instant::now());
    } // end method start_reading

    fn stop_reading(&mut self, received: bool) {
        if let Some(since) = self.reading_since.take() {
            self.reading_time += since.elapsed();
        }

        if received {
            self.heard();
        }
    } // end method stop_reading

    /// Records a pong or a received message, which clears the unanswered
    /// ping.
    pub(crate) fn heard(&mut self) {
        self.last_heard = Instant::now();
        self.unanswered_since = None;
    } // end method heard

    /// Includes the read in progress.
    pub(crate) fn total_reading_time(&self) -> Duration {
        self.reading_time
            + self
                .reading_since
                .map_or(Duration::ZERO, |since| since.elapsed())
    } // end method total_reading_time
} // end impl HeartbeatState

impl ConnectionState {
    pub(crate) fn has_room(&self, size: usize) -> bool {
        self.outbound.len() < MAXIMUM_PENDING_COMMANDS
            && self.outbound_bytes + size <= MAXIMUM_BUFFERED_BYTES
    } // end method has_room

    pub(crate) fn hand_off(&mut self, data: Bytes, publish: Option<Arc<QueuedPublish>>) {
        self.outbound_bytes += data.len();
        self.outbound.push_back(OutboundFrame { data, publish });
        self.ready.notify_one();
    } // end method hand_off

    pub(crate) fn begin_closing(&mut self) {
        self.closing = true;
        self.ready.notify_one();
    } // end method begin_closing

    fn is_writing(&self, publish: &Arc<QueuedPublish>) -> bool {
        self.outbound
            .iter()
            .take(self.writing)
            .filter_map(|frame| frame.publish.as_ref())
            .any(|written| Arc::ptr_eq(written, publish))
    } // end method is_writing

    /// Marks the frames at the front of the writer that go out together, in
    /// order: at most `MAXIMUM_BATCH_COMMANDS` of them, within
    /// `MAXIMUM_BATCH_BYTES` with the array header. A first frame larger than
    /// that goes alone. Returns the bytes to write: a single command as it is,
    /// several in a `*N` array.
    fn pack_batch(&mut self) -> Bytes {
        let mut size = self.outbound[0].data.len();
        let mut count = 1;

        while let Some(frame) = self.outbound.get(count) {
            let next = size + frame.data.len();
            // The array header is "*", the count and LF.
            let frame_length = 2 + (count + 1).to_string().len() + next;

            if count == MAXIMUM_BATCH_COMMANDS || frame_length > MAXIMUM_BATCH_BYTES {
                break;
            }

            size = next;
            count += 1;
        }

        self.writing = count;

        if count == 1 {
            return self.outbound[0].data.clone();
        }

        let header = format!("*{count}\n");
        let mut data = Vec::with_capacity(header.len() + size);
        data.extend_from_slice(header.as_bytes());

        for frame in self.outbound.iter().take(count) {
            data.extend_from_slice(&frame.data);
        }

        Bytes::from(data)
    } // end method pack_batch

    /// Takes back every frame of `publish` that the writer has not started, and
    /// reports if there was one. A rate limit can requeue a publish still in
    /// the writer, so it may have been handed over twice.
    pub(crate) fn remove_publish(&mut self, publish: &Arc<QueuedPublish>) -> (bool, bool) {
        let writing = self.is_writing(publish);
        let skip = self.writing;
        let before = self.outbound.len();
        let mut index = 0;

        self.outbound.retain(|frame| {
            index += 1;
            let unwritten = index > skip;
            let matches = frame
                .publish
                .as_ref()
                .is_some_and(|queued| Arc::ptr_eq(queued, publish));

            !(unwritten && matches)
        });

        self.outbound_bytes = self.outbound.iter().map(|frame| frame.data.len()).sum();

        (self.outbound.len() < before, writing)
    } // end method remove_publish

    /// Fails every publish handed to this connection that the socket never
    /// started to write.
    pub(crate) fn fail_unwritten(&mut self, error: &CelerisError) {
        for frame in self.outbound.iter().skip(self.writing) {
            if let Some(publish) = &frame.publish {
                publish.settle(Err(error.clone()));
            }
        }

        self.drop_unwritten();
    } // end method fail_unwritten

    /// Takes back, unsettled and in order, every publish handed to this
    /// connection that the socket never started to write, so recovery can
    /// queue it for the next socket (QUEUE-01). A rate limit can requeue a
    /// publish still in the writer, so it may appear twice; it is taken once.
    /// A publish already written, or being written, keeps that outcome: only a
    /// rate limit resends.
    pub(crate) fn take_unwritten(&mut self) -> Vec<Arc<QueuedPublish>> {
        let mut publishes: Vec<Arc<QueuedPublish>> = Vec::new();

        for frame in self.outbound.iter().skip(self.writing) {
            let Some(publish) = &frame.publish else {
                continue;
            };

            let taken = publishes
                .iter()
                .any(|existing| Arc::ptr_eq(existing, publish));

            if publish.is_settled() || self.is_writing(publish) || taken {
                continue;
            }

            publish.set_connection(None);
            publishes.push(Arc::clone(publish));
        }

        self.drop_unwritten();

        publishes
    } // end method take_unwritten

    /// Empties the writer except for the batch that the socket is writing.
    fn drop_unwritten(&mut self) {
        self.outbound.truncate(self.writing);
        self.outbound_bytes = self.outbound.iter().map(|frame| frame.data.len()).sum();
    } // end method drop_unwritten

    /// Stops the connection without a closing handshake. A publish whose write
    /// was in progress may or may not have been sent, and is never resent.
    fn abandon(&mut self) {
        self.broken = true;
        self.ready.notify_one();

        for publish in self
            .outbound
            .iter()
            .take(self.writing)
            .filter_map(|frame| frame.publish.as_ref())
        {
            publish.settle(Err(CelerisError::DeliveryUnknown(
                "The connection dropped while the socket was writing the publish, so it may or may not have been sent."
                    .to_owned(),
            )));
        }

        for task in &self.tasks {
            task.abort();
        }

        if let Some(writer) = &self.writer {
            writer.abort();
        }
    } // end method abandon
} // end impl ConnectionState

impl Drop for ConnectionState {
    /// A connection that the channel lets go of, or that goes with a dropped
    /// channel, takes its tasks and its socket with it.
    fn drop(&mut self) {
        self.abandon();
    } // end method drop
} // end impl Drop for ConnectionState

impl ChannelInner {
    pub(crate) fn connection(&self, id: u64) -> Option<&ConnectionState> {
        self.connections
            .iter()
            .find(|connection| connection.id == id)
    } // end method connection

    pub(crate) fn connection_mut(&mut self, id: u64) -> Option<&mut ConnectionState> {
        self.connections
            .iter_mut()
            .find(|connection| connection.id == id)
    } // end method connection_mut

    /// Installs a socket as the channel's connection and starts its tasks.
    pub(crate) fn install_connection(&mut self, socket: WebSocket) -> u64 {
        self.last_connection_id += 1;
        let id = self.last_connection_id;
        let (sink, stream) = socket.split();
        let sink: SharedSink = Arc::new(tokio::sync::Mutex::new(sink));
        let runtime = self.runtime();

        let reader = runtime.spawn(read_loop(self.this.clone(), id, stream));
        let pinger = runtime.spawn(heartbeat(self.this.clone(), id, Arc::clone(&sink)));
        let writer = runtime.spawn(write_loop(self.this.clone(), id, sink));

        self.connections.push(ConnectionState {
            id,
            outbound: VecDeque::new(),
            outbound_bytes: 0,
            writing: 0,
            ready: Arc::new(Notify::new()),
            closing: false,
            broken: false,
            heartbeat: HeartbeatState {
                reading_time: Duration::ZERO,
                reading_since: None,
                unanswered_since: None,
                last_heard: Instant::now(),
            },
            tasks: vec![reader.abort_handle(), pinger.abort_handle()],
            writer: Some(writer),
        });

        self.current_connection = Some(id);

        id
    } // end method install_connection

    /// Lets go of a connection: its unwritten publishes fail with `error`, and
    /// its tasks and socket stop.
    pub(crate) fn discard_connection(&mut self, id: u64, error: &CelerisError) {
        if let Some(position) = self
            .connections
            .iter()
            .position(|connection| connection.id == id)
        {
            let mut connection = self.connections.remove(position);
            connection.fail_unwritten(error);
        }

        if self.current_connection == Some(id) {
            self.current_connection = None;
        }
    } // end method discard_connection
} // end impl ChannelInner

enum NextWrite {
    Frame(Bytes),
    Close,
    Wait(Arc<Notify>),
    Stop,
} // end enum NextWrite

/// Writes handed-off frames in order until the connection breaks, or until it
/// closes and every frame is flushed.
async fn write_loop(channel: Weak<ChannelShared>, id: u64, sink: SharedSink) {
    loop {
        let next = {
            let Some(channel) = channel.upgrade() else {
                return;
            };

            let mut inner = channel.lock();

            match inner.connection_mut(id) {
                None => NextWrite::Stop,
                Some(connection) if connection.broken => NextWrite::Stop,
                Some(connection) if !connection.outbound.is_empty() => {
                    NextWrite::Frame(connection.pack_batch())
                }
                Some(connection) if connection.closing => NextWrite::Close,
                Some(connection) => NextWrite::Wait(Arc::clone(&connection.ready)),
            }
        };

        match next {
            NextWrite::Stop => return,
            NextWrite::Wait(ready) => ready.notified().await,
            NextWrite::Close => {
                let _ = sink.lock().await.close().await;

                return;
            }
            NextWrite::Frame(data) => {
                let written = sink.lock().await.send(Message::Binary(data)).await.is_ok();

                let Some(channel) = channel.upgrade() else {
                    return;
                };

                if !channel.finish_write(id, written) {
                    return;
                }
            }
        }
    }
} // end fn write_loop

impl ChannelShared {
    /// Settles the batch that the writer finished. Reports `false` when the
    /// writer must stop.
    fn finish_write(self: &Arc<Self>, id: u64, written: bool) -> bool {
        let mut inner = self.lock();

        let Some(connection) = inner.connection_mut(id) else {
            return false;
        };

        let batch: Vec<OutboundFrame> = connection.outbound.drain(..connection.writing).collect();
        connection.writing = 0;
        connection.outbound_bytes -= batch.iter().map(|frame| frame.data.len()).sum::<usize>();

        let publishes = batch.into_iter().filter_map(|frame| frame.publish);

        if !written {
            for publish in publishes {
                publish.settle(Err(CelerisError::DeliveryUnknown(
                    "The socket failed while writing the publish, so it may or may not have been sent.".to_owned(),
                )));
            }

            // A write that the socket refuses leaves the server's view of this
            // connection unknown, so the socket is replaced and reconnecting
            // restores every subscription (RESEND-01). Otherwise Close let go
            // of it while it flushed, so what it never wrote is cancelled.
            let queued = inner.receive_socket_failure(id);
            inner.discard_connection(id, &closed_before_sent());
            drop(inner);

            if queued {
                self.dispatch_events();
            }

            return false;
        }

        for publish in publishes {
            publish.settle(Ok(()));
        }

        if inner.current_connection == Some(id) {
            inner.drain();
        }

        true
    } // end method finish_write
} // end impl ChannelShared

pub(crate) fn closed_before_sent() -> CelerisError {
    CelerisError::Cancelled("Channel closed before the publish was sent.".to_owned())
} // end fn closed_before_sent

/// What one read produced.
enum Received {
    Message(Result<ServerMessage, CelerisError>),
    Heard,
    Ended,
} // end enum Received

/// Routes what arrives on one socket until it fails or the channel lets go of
/// it.
async fn read_loop(channel: Weak<ChannelShared>, id: u64, mut stream: SplitStream<WebSocket>) {
    loop {
        if !with_heartbeat(&channel, id, HeartbeatState::start_reading) {
            return;
        }

        let next = stream.next().await;
        let received = matches!(next, Some(Ok(_)));

        if !with_heartbeat(&channel, id, |heartbeat| heartbeat.stop_reading(received)) {
            return;
        }

        // A message that cannot be decoded costs exactly that message: decoding
        // never spans messages, so the next one is unaffected and the
        // connection stays up (DECODE-01).
        let outcome = match next {
            Some(Ok(Message::Binary(data))) => Received::Message(decode_server_message(&data)),
            Some(Ok(Message::Text(_))) => Received::Message(Err(protocol_error(
                "Expected a binary WebSocket message.",
                "Message",
                0,
            ))),
            Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => Received::Heard,
            Some(Ok(Message::Close(_)) | Err(_)) | None => Received::Ended,
        };

        let Some(channel) = channel.upgrade() else {
            return;
        };

        match outcome {
            Received::Heard => {}
            Received::Ended => {
                let queued = channel.lock().receive_socket_failure(id);

                if queued {
                    channel.dispatch_events();
                }

                return;
            }
            Received::Message(message) => {
                if !channel.route(id, message).await {
                    return;
                }
            }
        }
    }
} // end fn read_loop

/// Runs `update` on the connection's heartbeat state. Reports `false` when the
/// channel or the connection is gone.
fn with_heartbeat(
    channel: &Weak<ChannelShared>,
    id: u64,
    update: impl FnOnce(&mut HeartbeatState),
) -> bool {
    let Some(channel) = channel.upgrade() else {
        return false;
    };

    let mut inner = channel.lock();

    match inner.connection_mut(id) {
        Some(connection) => {
            update(&mut connection.heartbeat);

            true
        }
        None => false,
    }
} // end fn with_heartbeat
