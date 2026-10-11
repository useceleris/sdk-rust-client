//! Shared fixtures for the unit tests: an in-memory server peer, a recording
//! credential provider, event recorders and server frame builders. Nothing
//! here touches the network.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Bytes, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::channel::Channel;
use crate::client::{Client, ClientConfiguration, ClientOptions};
use crate::connection::{Dialer, Io, WebSocket};
use crate::constants::MAXIMUM_COMMAND_BYTES;
use crate::credentials::{CredentialProvider, CredentialRequest, Credentials};
use crate::errors::CelerisError;
use crate::segment::Segment;
use crate::values::ChannelState;

/// The base URL that every test client uses. Nothing listens there: the
/// dialer below replaces the network.
pub(crate) const TEST_BASE_URL: &str = "ws://localhost:9";

/// Room for this many bytes in each direction of an in-memory socket.
const PIPE_CAPACITY: usize = 4 * 1024 * 1024;

/// A thread-safe list that tests read after the events arrive.
pub(crate) struct Recorder<Item> {
    items: Arc<Mutex<Vec<Item>>>,
} // end struct Recorder

impl<Item> Clone for Recorder<Item> {
    fn clone(&self) -> Recorder<Item> {
        Recorder {
            items: Arc::clone(&self.items),
        }
    } // end method clone
} // end impl Clone for Recorder

impl<Item: Clone> Recorder<Item> {
    pub(crate) fn new() -> Recorder<Item> {
        Recorder {
            items: Arc::new(Mutex::new(Vec::new())),
        }
    } // end fn new

    pub(crate) fn push(&self, item: Item) {
        self.items.lock().unwrap().push(item);
    } // end method push

    pub(crate) fn all(&self) -> Vec<Item> {
        self.items.lock().unwrap().clone()
    } // end method all

    pub(crate) fn len(&self) -> usize {
        self.items.lock().unwrap().len()
    } // end method len
} // end impl Recorder

/// What the fake dialer does with the next dial.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DialBehaviour {
    Accept,
    Refuse,
    Hang,
} // end enum DialBehaviour

/// Stands in for the server: every dial it accepts produces a [`Peer`].
pub(crate) struct FakeServer {
    peers: tokio::sync::Mutex<mpsc::UnboundedReceiver<Peer>>,
    behaviour: Arc<Mutex<DialBehaviour>>,

    /// How many of the next dials are refused before the behaviour applies.
    refusals: Arc<Mutex<u32>>,

    pub(crate) urls: Recorder<String>,
} // end struct FakeServer

impl FakeServer {
    /// Waits for the next connection that the client opens.
    pub(crate) async fn accept(&self) -> Peer {
        let next = timeout(Duration::from_secs(600), async {
            self.peers.lock().await.recv().await
        });

        next.await
            .expect("the client opened no connection")
            .expect("the dialer is gone")
    } // end method accept

    /// Reports if a connection was opened that no test accepted yet.
    pub(crate) fn has_pending_connection(&self) -> bool {
        self.peers.try_lock().is_ok_and(|peers| !peers.is_empty())
    } // end method has_pending_connection

    pub(crate) fn set_behaviour(&self, behaviour: DialBehaviour) {
        *self.behaviour.lock().unwrap() = behaviour;
    } // end method set_behaviour

    /// Refuses the next `count` dials, then applies the behaviour again.
    pub(crate) fn refuse_dials(&self, count: u32) {
        *self.refusals.lock().unwrap() = count;
    } // end method refuse_dials
} // end impl FakeServer

/// Records every credential request and answers with fixed credentials.
#[derive(Clone)]
pub(crate) struct ProviderLog {
    pub(crate) requests: Recorder<CredentialRequest>,

    /// When each request arrived, on the test clock.
    pub(crate) times: Recorder<Instant>,
} // end struct ProviderLog

pub(crate) fn test_credentials() -> Credentials {
    Credentials {
        payload: "test-payload".to_owned(),
        signature: "test-signature".to_owned(),
    }
} // end fn test_credentials

/// A provider that records each request and returns [`test_credentials`].
pub(crate) fn recording_provider() -> (CredentialProvider, ProviderLog) {
    let log = ProviderLog {
        requests: Recorder::new(),
        times: Recorder::new(),
    };

    let requests = log.requests.clone();
    let times = log.times.clone();

    let provider = CredentialProvider::new(move |request| {
        requests.push(request);
        times.push(Instant::now());

        async { Ok(test_credentials()) }
    });

    (provider, log)
} // end fn recording_provider

/// Options for a test client: the recording provider, the test URL and the
/// loopback opt-in.
pub(crate) fn test_options() -> (ClientOptions, ProviderLog) {
    let (provider, log) = recording_provider();

    (options_with_provider(provider), log)
} // end fn test_options

/// Records every state change of `channel` from now, without first letting
/// the queued events reach the listeners registered before.
pub(crate) fn state_recorder(channel: &Channel) -> Recorder<ChannelState> {
    let states = Recorder::new();
    let collector = states.clone();
    channel
        .events()
        .on_state_change(move |state| collector.push(state));

    states
} // end fn state_recorder

/// Records every error event of `channel` from now, without first letting
/// the queued events reach the listeners registered before.
pub(crate) fn error_recorder(channel: &Channel) -> Recorder<CelerisError> {
    let errors = Recorder::new();
    let collector = errors.clone();
    channel
        .events()
        .on_error(move |error| collector.push(error.clone()));

    errors
} // end fn error_recorder

/// The payload of a publish to the default segment, with a message id of seven
/// characters, whose command is exactly the largest command.
const STALL_PAYLOAD_BYTES: usize = MAXIMUM_COMMAND_BYTES - 37;

/// Fills the writer of a connected channel whose peer does not read. The
/// first publish of the largest command fits the pipe and is sent. The socket
/// blocks during the second, which then holds all of the writer's room, so
/// every later publish waits in the publish queue. The second publish stays
/// pending until the peer reads; the peer then receives "stall-1" and
/// "stall-2", each [`MAXIMUM_COMMAND_BYTES`] long, before anything else.
pub(crate) async fn stall_writer(channel: &Channel) -> JoinHandle<Result<(), CelerisError>> {
    let segment = channel.default_segment();
    let payload = vec![b'x'; STALL_PAYLOAD_BYTES];

    segment
        .publish_with_message_id(&payload, "stall-1")
        .await
        .expect("the pipe has room for the first command");

    let stuck =
        tokio::spawn(async move { segment.publish_with_message_id(&payload, "stall-2").await });

    tokio::time::sleep(Duration::from_millis(1)).await;
    assert!(!stuck.is_finished(), "the second command did not block");

    stuck
} // end fn stall_writer

/// Reads every message on `peer` from a task of its own, so the peer answers
/// pings, and forwards each binary frame as text. Abort the task to drop the
/// connection.
pub(crate) fn keep_reading(mut peer: Peer) -> (JoinHandle<()>, mpsc::UnboundedReceiver<String>) {
    let (sender, receiver) = mpsc::unbounded_channel();

    let reader = tokio::spawn(async move {
        while let Some(message) = peer.next_message().await {
            if let Message::Binary(data) = message {
                let _ = sender.send(String::from_utf8_lossy(&data).into_owned());
            }
        }
    });

    (reader, receiver)
} // end fn keep_reading

/// Options for a test client with the given provider.
pub(crate) fn options_with_provider(provider: CredentialProvider) -> ClientOptions {
    ClientOptions {
        base_url: Some(TEST_BASE_URL.to_owned()),
        allow_insecure_loopback: true,
        ..ClientOptions::new(provider)
    }
} // end fn options_with_provider

/// Options whose provider records when each request came, on the test clock.
pub(crate) fn timed_test_options() -> (ClientOptions, Recorder<(Instant, CredentialRequest)>) {
    let requests = Recorder::new();
    let collector = requests.clone();

    let provider = CredentialProvider::new(move |request| {
        collector.push((Instant::now(), request));

        async { Ok(test_credentials()) }
    });

    (options_with_provider(provider), requests)
} // end fn timed_test_options

/// Connects a new channel of `client` and returns it with its peer.
pub(crate) async fn connect(client: &Client, server: &FakeServer) -> (Channel, Peer) {
    let channel = client.channel("test-channel").unwrap();
    let (connected, peer) = tokio::join!(channel.connect(), server.accept());
    connected.expect("the test channel connects");
    settle(&channel).await;

    (channel, peer)
} // end fn connect

/// Builds a client from `options` through the public constructor, then puts
/// the fake server in place of the network and a fixed value in place of the
/// jitter.
pub(crate) fn client_with_server(options: ClientOptions, random: f64) -> (Client, Arc<FakeServer>) {
    let client = Client::new(options).expect("the test options are valid");
    let (sender, receiver) = mpsc::unbounded_channel();
    let behaviour = Arc::new(Mutex::new(DialBehaviour::Accept));
    let refusals = Arc::new(Mutex::new(0));
    let urls = Recorder::new();

    let server = Arc::new(FakeServer {
        peers: tokio::sync::Mutex::new(receiver),
        behaviour: Arc::clone(&behaviour),
        refusals: Arc::clone(&refusals),
        urls: urls.clone(),
    });

    let dial: Dialer = Arc::new(move |url| {
        urls.push(url);
        let mut behaviour = *behaviour.lock().unwrap();
        let mut refusals = refusals.lock().unwrap();

        if *refusals > 0 {
            *refusals -= 1;
            behaviour = DialBehaviour::Refuse;
        }

        drop(refusals);
        let sender = sender.clone();

        Box::pin(async move {
            match behaviour {
                DialBehaviour::Refuse => Err(()),
                DialBehaviour::Hang => std::future::pending().await,
                DialBehaviour::Accept => {
                    let (socket, peer) = socket_pair().await;
                    let _ = sender.send(peer);

                    Ok(socket)
                }
            }
        })
    });

    let configuration = ClientConfiguration {
        dial,
        random: Arc::new(move || random),
        ..(*client.configuration).clone()
    };

    let client = Client {
        configuration: Arc::new(configuration),
    };

    (client, server)
} // end fn client_with_server

/// A connected test channel and its peer: the default options, zero jitter.
pub(crate) async fn connected_channel() -> (Channel, Peer, Arc<FakeServer>, ProviderLog) {
    let (options, log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();

    let (connected, peer) = tokio::join!(channel.connect(), server.accept());
    connected.expect("the test channel connects");

    (channel, peer, server, log)
} // end fn connected_channel

async fn socket_pair() -> (WebSocket, Peer) {
    let (client_side, server_side) = tokio::io::duplex(PIPE_CAPACITY);
    let unlimited = WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None);
    let pipe = Arc::new(PipeControl::default());

    let client_side: Box<dyn Io> = Box::new(ControlledStream {
        inner: client_side,
        pipe: Arc::clone(&pipe),
    });

    let socket = WebSocketStream::from_raw_socket(
        MaybeTlsStream::Plain(client_side),
        Role::Client,
        Some(unlimited),
    )
    .await;
    let server = WebSocketStream::from_raw_socket(server_side, Role::Server, Some(unlimited)).await;

    (
        socket,
        Peer {
            socket: server,
            pipe,
            commands: VecDeque::new(),
        },
    )
} // end fn socket_pair

/// What the test asks of the client's writes on one in-memory socket.
#[derive(Default)]
struct PipeState {
    held: bool,
    failing: bool,
    waiting_write: Option<Waker>,
    ping_times: Vec<Instant>,
} // end struct PipeState

/// Lets a test stall or break the client's writes, and records when the
/// client wrote each ping.
#[derive(Default)]
struct PipeControl {
    state: Mutex<PipeState>,
} // end struct PipeControl

impl PipeControl {
    fn state(&self) -> MutexGuard<'_, PipeState> {
        self.state.lock().unwrap()
    } // end method state
} // end impl PipeControl

/// The client's end of an in-memory socket, under the test's control.
struct ControlledStream {
    inner: DuplexStream,
    pipe: Arc<PipeControl>,
} // end struct ControlledStream

/// The first byte of a masked client ping frame: FIN and opcode 9.
const PING_FRAME_START: u8 = 0x89;

impl AsyncRead for ControlledStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(context, buffer)
    } // end method poll_read
} // end impl AsyncRead for ControlledStream

impl AsyncWrite for ControlledStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        {
            let mut state = this.pipe.state();

            if state.failing {
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
            }

            if state.held {
                state.waiting_write = Some(context.waker().clone());

                return Poll::Pending;
            }
        }

        let result = Pin::new(&mut this.inner).poll_write(context, data);

        if matches!(result, Poll::Ready(Ok(written)) if written > 0)
            && data.first() == Some(&PING_FRAME_START)
        {
            this.pipe.state().ping_times.push(Instant::now());
        }

        result
    } // end method poll_write

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        if this.pipe.state().failing {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
        }

        Pin::new(&mut this.inner).poll_flush(context)
    } // end method poll_flush

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    } // end method poll_shutdown
} // end impl AsyncWrite for ControlledStream

/// The server's end of one in-memory connection.
pub(crate) struct Peer {
    socket: WebSocketStream<DuplexStream>,
    pipe: Arc<PipeControl>,

    /// Commands of a `*N` frame that the test has not read yet.
    commands: VecDeque<String>,
} // end struct Peer

impl Peer {
    /// The next binary frame that the client sent. Pings are answered and
    /// skipped. Fails the test after two hours of test time, since the
    /// client's pings keep the paused clock moving.
    pub(crate) async fn next_frame(&mut self) -> Vec<u8> {
        let next = timeout(Duration::from_secs(2 * 60 * 60), async {
            loop {
                match self.next_message().await {
                    Some(Message::Binary(data)) => return data.to_vec(),
                    Some(Message::Ping(_) | Message::Pong(_)) => {}
                    other => panic!("expected a binary frame, got {other:?}"),
                }
            }
        });

        next.await.expect("the client sent no frame")
    } // end method next_frame

    /// The next command as text, for readable assertions; a `*N` frame gives
    /// its commands one at a time.
    pub(crate) async fn next_command(&mut self) -> String {
        if let Some(command) = self.commands.pop_front() {
            return command;
        }

        let frame =
            String::from_utf8(self.next_frame().await).expect("commands in these tests are text");
        self.commands = split_frame(&frame).into();

        self.commands.pop_front().expect("a frame holds a command")
    } // end method next_command

    /// The next message of any kind, or `None` when the client closed.
    pub(crate) async fn next_message(&mut self) -> Option<Message> {
        match self.socket.next().await {
            Some(Ok(message)) => Some(message),
            Some(Err(_)) | None => None,
        }
    } // end method next_message

    /// Reports the commands that the client sent within `wait`, each `*N`
    /// frame split into its commands, answering pings.
    pub(crate) async fn commands_within(&mut self, wait: Duration) -> Vec<String> {
        let mut commands: Vec<String> = self.commands.drain(..).collect();
        let deadline = tokio::time::Instant::now() + wait;

        while let Ok(message) = tokio::time::timeout_at(deadline, self.socket.next()).await {
            match message {
                Some(Ok(Message::Binary(data))) => {
                    commands.extend(split_frame(&String::from_utf8_lossy(&data)));
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }

        commands
    } // end method commands_within

    pub(crate) async fn send(&mut self, frame: impl Into<Vec<u8>>) {
        self.socket
            .send(Message::Binary(Bytes::from(frame.into())))
            .await
            .expect("the client side is open");
    } // end method send

    pub(crate) async fn send_text(&mut self, text: &str) {
        self.socket
            .send(Message::text(text))
            .await
            .expect("the client side is open");
    } // end method send_text

    pub(crate) async fn send_ping(&mut self) {
        self.socket
            .send(Message::Ping(Bytes::new()))
            .await
            .expect("the client side is open");
    } // end method send_ping

    /// Sends a frame and reports `false` when the client side is gone.
    pub(crate) async fn try_send(&mut self, frame: impl Into<Vec<u8>>) -> bool {
        self.socket
            .send(Message::Binary(Bytes::from(frame.into())))
            .await
            .is_ok()
    } // end method try_send

    /// Closes with a close frame, as a server does.
    pub(crate) async fn close(mut self) {
        let _ = self.socket.close(None).await;
    } // end method close

    /// Sends bytes that are not a valid WebSocket frame, then drops the
    /// socket: the client reads a transport error, then the end of the stream.
    pub(crate) async fn break_with_garbage(mut self) {
        // FIN with the reserved opcode 3.
        self.socket
            .get_mut()
            .write_all(&[0x83, 0x00])
            .await
            .expect("the client side is open");
    } // end method break_with_garbage

    /// Stalls the client's writes on this socket until `release_writes`.
    pub(crate) fn hold_writes(&self) {
        self.pipe.state().held = true;
    } // end method hold_writes

    pub(crate) fn release_writes(&self) {
        let mut state = self.pipe.state();
        state.held = false;

        if let Some(waker) = state.waiting_write.take() {
            waker.wake();
        }
    } // end method release_writes

    /// Reports if a client write waits because writes are held.
    pub(crate) fn has_held_write(&self) -> bool {
        self.pipe.state().waiting_write.is_some()
    } // end method has_held_write

    /// Makes every later client write on this socket fail.
    pub(crate) fn fail_writes(&self) {
        self.pipe.state().failing = true;
    } // end method fail_writes

    /// When the client wrote each ping on this socket, on the test clock.
    pub(crate) fn ping_times(&self) -> Vec<Instant> {
        self.pipe.state().ping_times.clone()
    } // end method ping_times

    /// Reads everything in a background task, which answers pings. Dropping
    /// the result drops the socket.
    pub(crate) fn answer_in_background(self) -> AnsweringPeer {
        let mut peer = self;

        AnsweringPeer {
            task: tokio::spawn(async move { while peer.next_message().await.is_some() {} }),
        }
    } // end method answer_in_background
} // end impl Peer

/// The commands of a frame: one, or each of a `*N` array.
pub(crate) fn split_frame(frame: &str) -> Vec<String> {
    let Some(body) = frame.strip_prefix('*') else {
        return vec![frame.to_owned()];
    };

    let mut commands = Vec::new();
    let mut offset = body.find('\n').expect("an array header ends its line") + 1;

    while offset < body.len() {
        let start = offset;
        offset += body[offset..]
            .find('\n')
            .expect("a command name ends its line")
            + 1;

        // Fields follow the command name until the next command starts.
        while offset < body.len() && !body[offset..].starts_with('@') {
            let line_end = offset + body[offset..].find('\n').expect("a field ends its line");
            let field = &body[offset..line_end];
            offset = line_end + 1;

            if let Some(length) = field
                .strip_prefix('$')
                .and_then(|length| length.parse::<usize>().ok())
            {
                offset += length + 1;
            }
        }

        commands.push(body[start..offset].to_owned());
    }

    commands
} // end fn split_frame

/// A peer that a background task reads; dropping it drops the socket at once.
pub(crate) struct AnsweringPeer {
    task: JoinHandle<()>,
} // end struct AnsweringPeer

impl Drop for AnsweringPeer {
    fn drop(&mut self) {
        self.task.abort();
    } // end method drop
} // end impl Drop for AnsweringPeer

/// `@MSG` as the server writes it.
pub(crate) fn delivery_frame(
    token_reference: &str,
    segment_id: &str,
    message_id: &str,
    timestamp: i64,
    payload: &[u8],
) -> Vec<u8> {
    let mut frame = format!(
        "@MSG\n+{token_reference}\n+{segment_id}\n+{message_id}\n:{timestamp}\n${}\n",
        payload.len()
    )
    .into_bytes();
    frame.extend_from_slice(payload);
    frame.push(b'\n');

    frame
} // end fn delivery_frame

/// `@SERVER_MSG` as the server writes it.
pub(crate) fn notice_frame(timestamp: i64, text: &str) -> Vec<u8> {
    format!("@SERVER_MSG\n:{timestamp}\n${}\n{text}\n", text.len()).into_bytes()
} // end fn notice_frame

/// `@PRES_NOTIFY` as the server writes it.
pub(crate) fn presence_frame(
    segment_id: &str,
    token_reference: &str,
    connection_id: &str,
    joined: bool,
    timestamp: i64,
) -> Vec<u8> {
    let event = i32::from(joined);

    format!("@PRES_NOTIFY\n+{segment_id}\n+{token_reference}\n+{connection_id}\n;{event}\n:{timestamp}\n").into_bytes()
} // end fn presence_frame

/// `@PRES_LIST_RESPONSE` with one entry per `(token reference, connection id,
/// timestamp)`.
pub(crate) fn presence_list_frame(
    segment_id: &str,
    request_id: &str,
    figures: [i32; 5],
    connections: &[(&str, &str, i64)],
) -> Vec<u8> {
    let [total, per_page, current_page, from, to] = figures;

    let mut frame = format!(
        "@PRES_LIST_RESPONSE\n+{segment_id}\n${}\n{request_id}\n;{total}\n;{per_page}\n;{current_page}\n;{from}\n;{to}\n*{}\n",
        request_id.len(),
        connections.len()
    );

    for (token_reference, connection_id, timestamp) in connections {
        frame.push_str(&format!(
            "*3\n+{token_reference}\n+{connection_id}\n:{timestamp}\n"
        ));
    }

    frame.into_bytes()
} // end fn presence_list_frame

/// `-Err` with a simple-string sub type (or null) and a bulk resource (or
/// null).
pub(crate) fn error_frame(
    error_type: &str,
    sub_type: Option<&str>,
    message: &str,
    resource: Option<&str>,
) -> Vec<u8> {
    let sub_type = sub_type.map_or("$-1\n".to_owned(), |sub_type| format!("+{sub_type}\n"));
    let resource = resource.map_or("$-1\n".to_owned(), |resource| {
        format!("${}\n{resource}\n", resource.len())
    });

    format!(
        "-Err\n+{error_type}\n{sub_type}${}\n{message}\n{resource}",
        message.len()
    )
    .into_bytes()
} // end fn error_frame

/// `@PUB` as the client writes it.
pub(crate) fn publish_command(segment_id: &str, message_id: &str, payload: &str) -> String {
    format!(
        "@PUB\n${}\n{segment_id}\n${}\n{message_id}\n${}\n{payload}\n",
        segment_id.len(),
        message_id.len(),
        payload.len()
    )
} // end fn publish_command

/// The `-Err` frame of a rate limit.
pub(crate) fn rate_limit_frame() -> Vec<u8> {
    error_frame("RateLimitError", None, "Rate limit exceeded", None)
} // end fn rate_limit_frame

/// The command a client sends for a segment.
pub(crate) fn segment_command(name: &str, segment_id: &str) -> String {
    format!("@{name}\n${}\n{segment_id}\n", segment_id.len())
} // end fn segment_command

/// Sends a rate limit and waits, without moving the clock, until the client
/// paused sending. Returns when the limit arrived.
pub(crate) async fn receive_rate_limit(channel: &Channel, peer: &mut Peer) -> Instant {
    peer.send(rate_limit_frame()).await;
    let shared = &channel.shared;
    yield_until("the rate limit pause", || shared.lock().queue.is_paused()).await;

    Instant::now()
} // end fn receive_rate_limit

/// Publishes and reads the frame that the peer receives.
pub(crate) async fn publish_now(
    segment: &Segment,
    peer: &mut Peer,
    message_id: &str,
    payload: &str,
) {
    let (published, frame) = tokio::join!(
        segment.publish_with_message_id(payload, message_id),
        peer.next_command()
    );
    published.expect("the publish is sent");
    assert_eq!(frame, publish_command(segment.id(), message_id, payload));
} // end fn publish_now

/// Records every later state change of `channel`. It first lets the events
/// already queued reach the listeners registered before.
pub(crate) async fn record_states(channel: &Channel) -> Recorder<ChannelState> {
    settle(channel).await;
    let states = Recorder::new();
    let collector = states.clone();
    channel
        .events()
        .on_state_change(move |state| collector.push(state));

    states
} // end fn record_states

/// Records every later error that `channel` reports. It first lets the
/// events already queued reach the listeners registered before.
pub(crate) async fn record_errors(channel: &Channel) -> Recorder<CelerisError> {
    settle(channel).await;
    let errors = Recorder::new();
    let collector = errors.clone();
    channel
        .events()
        .on_error(move |error| collector.push(error.clone()));

    errors
} // end fn record_errors

/// Lets queued listener deliveries finish.
pub(crate) async fn settle(channel: &Channel) {
    channel.shared.quiesce().await;
} // end fn settle

/// Waits until `condition` holds, letting the paused clock run. Fails the test
/// after ten minutes of test time.
pub(crate) async fn wait_until(description: &str, condition: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(600);

    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
} // end fn wait_until

/// Waits until `condition` holds without letting the paused clock move: it
/// only yields to the other tasks.
pub(crate) async fn yield_until(description: &str, condition: impl Fn() -> bool) {
    for _ in 0..1_000_000 {
        if condition() {
            return;
        }

        tokio::task::yield_now().await;
    }

    panic!("gave up waiting for {description}");
} // end fn yield_until

/// A publish in progress, which the test awaits or drops later.
pub(crate) type PendingPublish<'call> =
    Pin<Box<dyn Future<Output = Result<(), CelerisError>> + Send + 'call>>;

/// Polls `future` once and returns its output if it is ready.
pub(crate) async fn poll_once<Pending: Future + Unpin>(
    future: &mut Pending,
) -> Option<Pending::Output> {
    std::future::poll_fn(|context| match Pin::new(&mut *future).poll(context) {
        Poll::Ready(output) => Poll::Ready(Some(output)),
        Poll::Pending => Poll::Ready(None),
    })
    .await
} // end fn poll_once

/// Starts a publish and polls it once, which queues it, and checks that it
/// is still waiting.
pub(crate) async fn start_publish<'call>(
    segment: &'call Segment,
    message_id: &'call str,
    payload: &'call str,
) -> PendingPublish<'call> {
    let mut publish: PendingPublish<'call> =
        Box::pin(segment.publish_with_message_id(payload, message_id));
    let first = poll_once(&mut publish).await;
    assert!(first.is_none(), "the publish settled at once: {first:?}");

    publish
} // end fn start_publish

/// Records the message id of every delivery to `segment`.
pub(crate) fn record_message_ids(segment: &Segment) -> Recorder<String> {
    let identifiers = Recorder::new();
    let collector = identifiers.clone();
    segment.on_message(move |_payload, metadata| collector.push(metadata.message_id.clone()));

    identifiers
} // end fn record_message_ids

/// Fills the in-memory pipe from the client to the peer exactly, so the next
/// frame that the client writes waits until the peer reads. Call it when the
/// peer has read every earlier frame. The peer then reads the two commands
/// that it sends to segment "s" first: 2 097 152 and 2 097 124 bytes.
pub(crate) async fn fill_pipe(channel: &Channel) {
    // Each command is "@PUB\n$1\ns\n$6\nfill-N\n$" with a 7-digit length and
    // LF, the payload and LF: 30 bytes and the payload. Each WebSocket frame
    // adds a 14-byte header (an 8-byte length and a 4-byte mask), so the two
    // frames are 2 097 166 and 2 097 138 bytes: 4 194 304 together.
    const _: () = assert!(PIPE_CAPACITY == 2_097_166 + 2_097_138);

    let segment = channel.segment("s").expect("the segment ID is valid");

    segment
        .publish_with_message_id(vec![0_u8; 2_097_122], "fill-1")
        .await
        .expect("the first fill command goes out");

    segment
        .publish_with_message_id(vec![0_u8; 2_097_094], "fill-2")
        .await
        .expect("the second fill command goes out");
} // end fn fill_pipe
