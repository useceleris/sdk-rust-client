# Celeris Rust client

[![Crates.io](https://img.shields.io/crates/v/useceleris-client.svg)](https://crates.io/crates/useceleris-client)
[![Documentation](https://docs.rs/useceleris-client/badge.svg)](https://docs.rs/useceleris-client)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

The realtime client for Celeris channels: the connection lifecycle with automatic recovery, segment messages, and presence.

**The model in three sentences.** A `Channel` is one WebSocket connection: another `Channel`, also for the same reference, opens another connection. All segments of a channel share that one connection, and when you connect, the connection becomes a member of the `"default"` segment. `Segment` handles are light: make as many as you need, and they share the connection and one interest count.

## Install

```sh
cargo add useceleris-client
```

The client is async and runs on [Tokio](https://tokio.rs). It needs Rust 1.88 or newer. It uses `tokio-tungstenite` for the WebSocket and `rustls` with the system's root certificates for TLS.

## Quickstart

```rust,no_run
use useceleris_client::{Client, ClientOptions, CredentialProvider, Credentials, read_text, text_payload};

async fn fetch_credentials(channel_reference: String) -> Result<Credentials, Box<dyn std::error::Error + Send + Sync>> {
    // Get the credentials from your own credential endpoint.
    todo!("sign credentials for {channel_reference} on your server")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = CredentialProvider::new(|request| fetch_credentials(request.channel_reference));
    let client = Client::new(ClientOptions::new(provider))?;
    let channel = client.channel("room-42")?;
    let chat = channel.segment("chat")?;

    chat.on_message(|payload, metadata| {
        println!("{}: {:?}", metadata.message_id, read_text(payload));
    });

    let _subscription = chat.subscribe()?;
    channel.connect().await?;
    chat.publish(text_payload("hello")).await?;
    channel.close().await;

    Ok(())
}
```

[examples/quickstart.rs](examples/quickstart.rs) is the same flow as a complete program.

## Credentials

The client never signs. A `CredentialProvider` gets fresh credentials from your own authenticated endpoint, which signs them with [useceleris-server](https://crates.io/crates/useceleris-server). Never put a signing secret into an application.

```rust,no_run
use useceleris_client::{CredentialProvider, CredentialReason, Credentials};

fn provider() -> CredentialProvider {
    CredentialProvider::new(|request| async move {
        let lookback = match request.reason {
            CredentialReason::Initial => None,
            CredentialReason::Reconnect { replay_lookback, .. } => Some(replay_lookback),
        };

        // Send request.channel_reference and lookback to your endpoint, and
        // return the payload and signature that it signed.
        let _ = lookback;

        Ok(Credentials {
            payload: String::from("from your endpoint"),
            signature: String::from("from your endpoint"),
        })
    })
}
```

The provider runs one time for each connection attempt, reconnects included. On a reconnect, `CredentialReason::Reconnect` gives the time of the drop and a replay lookback: the outage so far plus five seconds. Your endpoint decides if it grants the replay. When the attempt's deadline passes or the channel closes, the SDK drops the provider's future. An error or a panic from the provider is reported as `CelerisError::Transport`, and the SDK never passes on its text. `Debug` shows `Credentials` as redacted.

## Channels and connection

`connect` returns when the socket is open and your subscriptions are queued for restoration. A first connect that fails is not retried: the channel becomes `Failed`, and you can call `connect` again. Only a connection that was up recovers automatically. `close` is terminal and idempotent, returns in about five seconds at most, and fails the publishes and presence queries that still wait with `CelerisError::Cancelled`.

The states go from `Idle` to `Connecting` to `Connected`. A drop gives `Reconnecting`, then `Connected` again on recovery, or `Failed` when recovery stops. `close` gives `Closing`, then `Closed`.

```rust,no_run
use useceleris_client::{Channel, ChannelState};

fn watch_state(channel: &Channel) {
    channel.events().on_state_change(|state| match state {
        ChannelState::Reconnecting => println!("The connection dropped. Recovery started."),
        ChannelState::Failed => println!("Recovery stopped. Call connect to try again."),
        _ => {}
    });
}
```

A channel reference is 1 to 255 ASCII letters, digits, `-` or `_`. A segment id is any text that is not empty and has no CR or LF.

## Subscribing and receiving

`subscribe` registers interest in a segment. The first interest subscribes the segment, the last `cancel` unsubscribes it, and the SDK restores the subscriptions after each reconnect. A subscription needs read access to the segment, and a publish needs write access. The protocol has no acknowledgement, so a denial comes later through `on_error`. The connection is a member of the default segment from the connect, so that segment needs no subscription.

```rust,no_run
use useceleris_client::{CelerisError, Segment};

fn listen(chat: &Segment) -> Result<(), CelerisError> {
    let listener = chat.on_message(|payload, metadata| {
        println!("{} sent {} bytes at {}", metadata.token_reference, payload.len(), metadata.timestamp);
    });

    let subscription = chat.subscribe()?;

    // Later:
    subscription.cancel();
    listener.remove();

    Ok(())
}
```

Each message id is delivered one time in the deduplication window (`deduplication_window_size`, 1024 ids by default). A registration stays until you call `remove`. When you drop the `Registration`, the listener stays.

The server, not a listener, controls the messages that you receive from a segment:

- When you hold a subscription to a segment, the connection receives the messages of that segment. A listener alone receives no messages.
- A publish does not make the connection a member of the segment. To receive the messages of a segment, call `subscribe`. This is also necessary for your own publishes, which the connection receives only when its credentials allow echo.
- With write access only, the connection can publish, and the server refuses `subscribe` with a `PermissionDeniedError`. With read access only, the connection can subscribe, and the server refuses the publish.
- When you cancel the last subscription of a segment, the server removes the connection from the segment. A presence subscription does not make or keep a membership.
- After a reconnect, the SDK subscribes again to the segments that you hold a subscription for.
- The connection always receives the messages of `"default"`.
- Listeners and subscriptions are different. When you remove one, the other does not change.

To receive all messages that the connection gets, from all segments, add a channel listener. The segment listeners get each message first, then the channel listeners:

```rust,no_run
use useceleris_client::Channel;

fn listen_to_all(channel: &Channel) {
    let all_messages = channel.events().on_message(|payload, metadata| {
        println!("{}: {} bytes", metadata.segment_id, payload.len());
    });

    // remove removes only this channel listener. The other listeners and the
    // subscriptions do not change, and the SDK sends nothing to the server.
    all_messages.remove();
}
```

## Publishing

`publish` sends with a generated message id, and `publish_with_message_id` sends with your id. `Ok` means that the local socket accepted the bytes, not that a receiver got them.

A generated id is `msg__rust_v`, the crate version, two underscores, then 32 random lowercase hex digits, for example `msg__rust_v0.1.0__4f1c9a0b7d2e43f6a8b5c1d0e9f27364`. It holds only letters, digits, dots and underscores, and the double underscores keep it apart from the ids that the server generates (`msg_{node}_…`). Your own id must have at least one character and no CR or LF. The SDK sends it exactly as you give it.

```rust,no_run
use useceleris_client::{CelerisError, Segment, text_payload};

async fn ship(orders: &Segment) {
    match orders
        .publish_with_message_id(text_payload("Order 1042 shipped"), "order-1042-shipped")
        .await
    {
        Ok(()) => println!("The socket accepted the bytes."),
        Err(CelerisError::Backpressure(_)) => println!("The publish queue is full. Try again later."),
        Err(CelerisError::DeliveryUnknown(_)) => println!("It is not known if this went out. Do not send it again blindly."),
        Err(error) => println!("Not sent: {error}"),
    }
}
```

While the channel reconnects, `publish` waits in the publish queue. The SDK sends it after the reconnect, when your subscriptions are restored. If recovery stops in `Failed`, the publish fails with the terminal error, and if you close the channel, it fails with `CelerisError::Cancelled`. Before the first connect, during the first connect, and after `Failed` or `close`, `publish` fails with `CelerisError::NotConnected`. A publish does not join the segment: call `subscribe` to receive from it. Your plan's limit counts the whole encoded command, not only the payload. A publish over that limit still returns `Ok`, and the server refuses it later with a `MessageSizeLimitError` through `on_error`. When you drop the future of `publish` before the socket starts to write it, the SDK takes the publish back.

## Payloads

Payloads are opaque bytes. `text_payload` and `read_text` handle UTF-8 text. For JSON or any other format, give your own encoder and decoder to `PayloadCodec`. The crate itself depends on no serializer:

```rust,no_run
use useceleris_client::PayloadCodec;

#[derive(Debug, PartialEq)]
struct Position {
    x: i32,
    y: i32,
}

fn position_codec() -> PayloadCodec<Position, String> {
    // With serde_json:
    // PayloadCodec::new(serde_json::to_vec, |payload| serde_json::from_slice(payload))
    PayloadCodec::new(
        |position: &Position| Ok(format!("{},{}", position.x, position.y).into_bytes()),
        |payload: &[u8]| {
            let text = String::from_utf8_lossy(payload);
            let (x, y) = text.split_once(',').ok_or("missing comma")?;
            let x = x.parse().map_err(|_| "bad x")?;
            let y = y.parse().map_err(|_| "bad y")?;

            Ok(Position { x, y })
        },
    )
}
```

The codec returns the errors of your encoder and decoder unchanged. A decoder only reads a type. Check payloads from peers that you do not control.

## Presence

A presence subscription gives the joins and leaves of a segment. It watches without a membership: it gives no messages, and the server does not announce or list the watcher. The events and `presence_list` include all server nodes of the channel.

```rust,no_run
use useceleris_client::{CelerisError, Segment};

async fn who_is_here(chat: &Segment) -> Result<(), CelerisError> {
    chat.on_presence(|event| {
        let change = if event.joined { "joined" } else { "left" };
        println!("{} {change} on connection {}", event.token_reference, event.connection_id);
    });

    let _presence = chat.subscribe_presence()?;

    let mut page = 1;

    loop {
        let result = chat.presence_list(page, 100).await?;

        for connection in &result.connections {
            println!("{} {}", connection.token_reference, connection.connection_id);
        }

        if result.connections.is_empty() || result.to >= result.total {
            return Ok(());
        }

        page += 1;
    }
}
```

One query can be in flight on a channel. A second query fails with `CelerisError::OperationInProgress`. A query times out after `presence_query_timeout` (10 seconds by default), and a timeout has no effect on the connection.

## Errors

All failures are one `CelerisError`. Match on the variant, never on the message. The SDK's own messages name what failed and the rule that it broke. They never contain your input values, credentials or server bytes.

| Variant | Meaning |
| --- | --- |
| `Configuration` | Options, identifiers or payloads that are not valid, or credentials from a provider that are not valid |
| `Timeout` | A connection attempt or a presence query ran past its deadline |
| `Cancelled` | The channel closed or a newer attempt started |
| `Transport` | The provider failed, the server refused the handshake, or the socket broke |
| `NotConnected` | The operation needs a connection that the channel does not have |
| `Backpressure` | The publish queue (`publish_queue_size`, 64 by default) is full, or sending stopped for a short time after a rate limit |
| `OperationInProgress` | A second `connect` or a second presence query while the first runs |
| `DeliveryUnknown` | A publish that possibly left the socket; the SDK never sends it again |
| `Protocol` | A server message that could not be decoded; the SDK drops it and the connection stays up |
| `Server` | An error frame from the server |

A refused handshake is `Transport`: the SDK never reports an authentication failure. Errors from the server come through `on_error` as `CelerisError::Server`, with the type (`PermissionDeniedError`, `RateLimitError`, `MessageSizeLimitError`, `ParserError`, `SendError`, `InternalError`, or `Other` for a type that a newer server adds), the sub type (the command that it answers), the message and the resource:

```rust,no_run
use useceleris_client::{CelerisError, Channel};

fn report_errors(channel: &Channel) {
    channel.events().on_error(|error| match error {
        CelerisError::Server(server_error) => println!(
            "server: {} {:?} {:?}",
            server_error.error_type.as_str(),
            server_error.sub_type,
            server_error.resource
        ),
        other => println!("sdk: {other}"),
    });
}
```

`on_notice` gives the server's raw notices, such as greetings, as text. Never make a decision from their text.

## Reconnection and recovery

The SDK recovers a connection that drops automatically:

- It makes at most `maximum_reconnect_attempts` failed attempts (10 by default, from 1 to 100), with full jitter up to 30 seconds between them.
- Each attempt gets fresh credentials and has the deadline `reconnect_timeout` (the connect timeout by default).
- The credential request suggests a replay lookback that covers the outage.
- The count of attempts starts again after a connection stays up for 60 seconds.

The client sends a ping every 20 seconds, so the connection stays open while a listener runs. When a ping gets no answer for 15 seconds of reading time, the SDK recovers the connection. Time in listeners does not count, so a slow listener never looks like a dead connection. When recovery stops, `on_error` reports the cause and the channel becomes `Failed`.

```rust,no_run
use useceleris_client::Channel;

fn watch_recovery(channel: &Channel) {
    channel.events().on_recovery(|event| {
        // Replay is a window with a limit: gaps and duplicates are always possible.
        println!("Recovered on retry {}.", event.retry_index);
    });
}
```

Recovery restores all subscriptions and always reports **possible gaps and duplicates**. The deduplication window (`deduplication_window_size`, 1024 ids by default) drops replayed messages that you already received. Duplicates older than the window get to your listeners.

## Delivery, as it is

- The protocol has **no server receipt or acknowledgement**. When `publish` returns `Ok`, the local socket accepted the bytes.
- There is no offline queue past a reconnect, no durable history and no global order. Publishes that wait in the publish queue when the connection drops go out after the reconnect, in call order, after the restored subscriptions. Publishes that the socket did not start to write go back to the front of the queue in their order. The publish that the socket was writing when the connection dropped reports `DeliveryUnknown`, and the SDK never sends it again.
- A `RateLimitError` never names the command that it dropped. Thus the client stops sending for a short time, then sends again what it sent in the last two seconds: the subscriptions as their current state, then at most 64 publishes, each one time and with its original id, so receivers drop a copy that they already got. After eight limits in a row, the SDK treats the limit as a used quota: it stops sending again, and it tries the dropped subscriptions on a slow schedule, after one minute and then doubled up to one hour. The repeated sends count toward your usage.
- When the writer is full, subscriptions and publishes wait for space. A subscription change goes before publishes, but never before a publish to its own segment that was queued before it.
- A publish does not join a segment, and a presence subscription does not. Only `subscribe` joins.

## Limits and defaults

| What | Value |
| --- | --- |
| Outbound command | 2 MiB encoded, refused before a write |
| Plan command limit | 64 KiB free, 128 KiB standard, 512 KiB pro, 1024 KiB prime, for the whole encoded command; the server enforces it |
| Writer limits | 64 commands or 2 MiB not yet written; `publish_queue_size` queued publishes (64 by default), plus repeated sends |
| Pause after a rate limit | 1 second plus full jitter that grows with consecutive limits, at most 31 seconds |
| Repeated sends | the commands of the last 2 seconds, at most 64 publishes, each one time |
| Quota probe | after 8 limits in a row: dropped subscriptions tried after 1 minute, doubled up to 1 hour |
| Connect deadline | `connect_timeout`, 15 seconds by default, for the credentials and the handshake together |
| Timeout options | each from 1 ms to 15 minutes |
| Reconnect deadline | `reconnect_timeout` for each reconnect attempt; the connect timeout by default |
| Presence query | one in flight on a channel; `presence_query_timeout`, 10 seconds by default; a timeout never drops the connection |
| Reconnect | `maximum_reconnect_attempts` failed attempts, 10 by default, from 1 to 100; full jitter up to 30 seconds; the count starts again after 60 seconds connected |
| Heartbeat | a WebSocket ping every 20 seconds |
| Dead connection | a ping without an answer for 15 seconds of reading time, without the time in listeners: found in about 35 seconds, then recovered |
| Deduplication window | `deduplication_window_size` message ids on a channel, 1024 by default |
| Close | about 5 seconds at most, also when the server does not answer |

The SDK never checks the size of received messages: they are already in memory when they arrive. A publish that the server refuses for your plan's limit still counts toward your usage.

## Tasks, listeners and cancellation

A `Client`, its channels and their segments are cheap to clone and safe to use from many tasks. Listeners run on Tokio's blocking thread pool, one at a time for each channel, in the order of the events, and never under an SDK lock:

- A slow listener makes delivery slower. It does not make a queue grow, and it never stops the runtime's worker threads.
- A listener can call `subscribe`, `cancel` and the listener methods directly. For the async methods (`publish`, `connect`, `close`, `presence_list`), it can start a task with `tokio::spawn`, or it can wait with `tokio::runtime::Handle::current().block_on(...)`.

Rust cancels an operation when you drop its future:

- When you drop the future of `connect`, the SDK stops the attempt, and the channel becomes `Failed`.
- When you drop the future of `publish`, the SDK takes back a publish that the socket did not start to write.
- When you drop the future of `presence_list`, the query slot becomes free.

Close each channel when you do not need it. When you drop all handles of a channel without `close`, its connection stops without the closing handshake.

## Local development and self-hosting

The production endpoint is built in. Set `base_url` only for a local stack or another deployment. The SDK accepts `ws://` only for a loopback host, and only when you set `allow_insecure_loopback`:

```rust,no_run
use useceleris_client::{Client, ClientOptions, CelerisError, CredentialProvider};

fn local_client(provider: CredentialProvider) -> Result<Client, CelerisError> {
    Client::new(ClientOptions {
        // A local ws:// stack. Production uses the built-in wss:// endpoint.
        base_url: Some(String::from("ws://localhost:19002")),
        allow_insecure_loopback: true,
        ..ClientOptions::new(provider)
    })
}
```

The SDK always verifies TLS against the system's root certificates, with its own `rustls` configuration. The TLS settings of your application have no effect on it.

## Versioning

Releases follow semantic versioning. Before 1.0.0, a minor release can change the API.

## More documentation

- [API reference](https://docs.rs/useceleris-client)
- [Rust guide](https://useceleris.com/docs/sdks/rust)
- [useceleris-server](https://crates.io/crates/useceleris-server), to sign credentials on your server

## Development

`make check` runs rustfmt, clippy, the unit suites, the layout and surface tests, every code block of this README, and the documentation build. `make live` runs the acceptance suites against a real Celeris stack. They read `CELERIS_WS_URL`, `CELERIS_CLIENT_ID` and `CELERIS_SIGNING_SECRET` from the environment or from a `.env` file that git ignores.

Read [CONVENTIONS.md](CONVENTIONS.md) before you contribute, and [SECURITY.md](SECURITY.md) before you report a vulnerability.

## License

[Apache 2.0](LICENSE).
