//! Every fixed value the crate uses, in one place. None of these is part of
//! the public surface.

use std::time::Duration;

// Outgoing size (LIMIT-01). Size is only ever checked on commands the client
// sends. Received messages are already fully buffered when they arrive, so
// checking them bounds no memory and only discards data.
//
// The command bound is the server's transport ceiling: above it no plan can
// accept a command. Each plan's own, smaller cap on the whole encoded command
// is enforced by the server and surfaces as a MessageSizeLimitError server
// error.
pub(crate) const MAXIMUM_COMMAND_BYTES: usize = 2 * 1024 * 1024;

// Equal to the command bound, so a maximum-size command always fits an empty
// writer and anything behind it waits for room.
pub(crate) const MAXIMUM_BUFFERED_BYTES: usize = MAXIMUM_COMMAND_BYTES;

// Bounds the writer and the publishes kept for a rate-limit resend, and is the
// default bound on the publishes that wait for the writer (publish queue size).
pub(crate) const MAXIMUM_PENDING_COMMANDS: usize = 64;

// Batching (BATCH-01). The writer sends the frames waiting for it in one `*N`
// frame of at most this many commands and bytes, header included. A command
// larger than the byte bound goes alone. The server refuses an array of more
// than 16 commands.
pub(crate) const MAXIMUM_BATCH_COMMANDS: usize = 16;

pub(crate) const MAXIMUM_BATCH_BYTES: usize = 64 * 1024;

// Outbound recovery (RESEND-01). A rate limit is reported without saying which
// frame it dropped, so whatever went out recently is resent.
//
// The server reports drops at most once a second, and a second more covers the
// round trip, so a report concerns only commands sent within this window.
pub(crate) const RATE_LIMIT_SUSPECT_WINDOW: Duration = Duration::from_secs(2);

// Resending waits at least this long, past the per-second window the dropped
// frames were counted in.
pub(crate) const RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(1);

// Bounds the extra load, and the extra usage, a rate limit can cause.
pub(crate) const MAXIMUM_PUBLISH_RESENDS: u32 = 1;

// After this many rate limits in a row the limit is treated as a used-up quota
// (per hour or per month): recent publishes are no longer resent, and recent
// subscriptions wait for a quota probe.
pub(crate) const MAXIMUM_CONSECUTIVE_RATE_LIMITS: u32 = 8;

// A used-up quota refuses every frame, so the subscriptions it dropped are
// re-sent rarely rather than abandoned: first after a minute, then doubling.
pub(crate) const QUOTA_PROBE_FIRST_DELAY: Duration = Duration::from_secs(60);

pub(crate) const QUOTA_PROBE_MAXIMUM_DELAY: Duration = Duration::from_secs(60 * 60);

// Generated message ids share every receiver's deduplication window with ids
// from other publishers, so they are random and long enough never to collide.
pub(crate) const MESSAGE_ID_RANDOM_BYTES: usize = 16;

// Every generated message id names the SDK that made it (RESEND-01): this
// crate's language and its version from Cargo.toml.
pub(crate) const SDK_LANGUAGE: &str = "rust";

pub(crate) const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

// Decoder bounds. These limit parsing work and recursion, not message size; no
// legitimate server message approaches them.
pub(crate) const MAXIMUM_FRAGMENTS: usize = 4096;

pub(crate) const MAXIMUM_DEPTH: usize = 32;

// "PRES_LIST_RESPONSE", the longest command the server sends.
pub(crate) const MAXIMUM_COMMAND_NAME_BYTES: usize = 18;

pub(crate) const MAXIMUM_ERROR_NAME_BYTES: usize = 64;

// Integer64 (`:`) carries timestamps; it is also the range of bulk and array
// lengths. "-9223372036854775808" is its widest value.
pub(crate) const MAXIMUM_INTEGER64_LINE_BYTES: usize = 20;

// Integer32 (`;`) carries every other integer. "-2147483648" is its widest
// value.
pub(crate) const MAXIMUM_INTEGER32_LINE_BYTES: usize = 11;

// The largest page a presence query may request.
pub(crate) const MAXIMUM_PRESENCE_PER_PAGE: i32 = 100;

// Connection defaults. Consumers do not configure where Celeris lives;
// overriding the base URL is for local stacks and other deployments
// (ENDPOINT-01).
pub(crate) const DEFAULT_BASE_URL: &str = "wss://realtime.useceleris.com";

pub(crate) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) const DEFAULT_PRESENCE_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

// The shortest and the longest timeout an option accepts (CONFIG-01). Every SDK
// shares them, and the longest stays below the largest delay a JavaScript timer
// can count.
pub(crate) const MINIMUM_TIMEOUT: Duration = Duration::from_millis(1);

pub(crate) const MAXIMUM_TIMEOUT: Duration = Duration::from_secs(15 * 60);

pub(crate) const DEFAULT_SEGMENT_ID: &str = "default";

// The longest channel reference the server accepts.
pub(crate) const MAXIMUM_CHANNEL_REFERENCE_LENGTH: usize = 255;

// The command a presence query error names as its sub type (QUERY-01).
pub(crate) const PRESENCE_LIST_COMMAND: &str = "PRES_LIST";

// Recovery. The default maximum reconnect attempts.
pub(crate) const DEFAULT_MAXIMUM_RECONNECT_ATTEMPTS: u32 = 10;

// The largest maximum reconnect attempts an option accepts (CONFIG-01), so a
// channel that cannot reconnect reaches failed in a known time.
pub(crate) const MAXIMUM_RECONNECT_ATTEMPTS_CEILING: u32 = 100;

pub(crate) const RETRY_BUDGET_RESET: Duration = Duration::from_secs(60);

pub(crate) const RETRY_BASE_DELAY: Duration = Duration::from_millis(500);

pub(crate) const RETRY_DELAY_CAP: Duration = Duration::from_secs(30);

// A rate limit already in hand disproves recovery only if commands flowed
// unrefused for longer than the longest pause plus the report window; any
// sooner, it may be a late report of the frames that just went out.
pub(crate) const QUOTA_RETURN_CONFIRMATION: Duration =
    RETRY_DELAY_CAP.saturating_add(RATE_LIMIT_SUSPECT_WINDOW);

pub(crate) const REPLAY_OVERLAP: Duration = Duration::from_secs(5);

// The server's largest replay lookback: an unsigned 32-bit millisecond count.
pub(crate) const REPLAY_LOOKBACK_CAP: Duration = Duration::from_millis(4_294_967_295);

pub(crate) const CLOSE_BUDGET: Duration = Duration::from_secs(5);

// The server pings every 30 seconds and closes a connection from which it has
// received no ping or pong for 60 seconds; data frames do not count
// (HEARTBEAT-01). The client answers its pings only while the channel reads,
// so it pings on its own this often: every 60-second window holds at least two
// pings, so one can fail to go out.
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

// A ping unanswered for this much reading time means the path is dead
// (HEARTBEAT-01), so a dead path is noticed within about 35 seconds. Pongs
// arrive only while the channel reads, so time a listener holds the reader
// never counts.
pub(crate) const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);

// The default deduplication window (deduplication window size).
pub(crate) const DEFAULT_DEDUPLICATION_WINDOW_SIZE: usize = 1024;
