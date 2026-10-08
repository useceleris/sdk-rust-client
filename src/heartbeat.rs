use std::sync::Weak;

use futures_util::SinkExt;
use tokio::time::{Instant, interval_at, sleep_until, timeout};
use tokio_tungstenite::tungstenite::{Bytes, Message};

use crate::channel::ChannelShared;
use crate::connection::SharedSink;
use crate::constants::{HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT};

/// What the heartbeat does after a wake.
enum Beat {
    Wait(Instant),
    Dead,
    Stop,
} // end enum Beat

/// Pings the server every HEARTBEAT_INTERVAL until the connection ends, so the
/// server's client timeout never fires while a listener holds the reader, and
/// fails the connection once a ping has gone unanswered for HEARTBEAT_TIMEOUT
/// of reading time (HEARTBEAT-01). Pongs are read only while the reader reads,
/// so time a listener holds it never counts.
pub(crate) async fn heartbeat(channel: Weak<ChannelShared>, id: u64, sink: SharedSink) {
    let mut pings = interval_at(Instant::now() + HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);

    // When the oldest unanswered ping may have used up its reading time; with
    // none unanswered, a wake does nothing.
    let mut check_at = Instant::now() + HEARTBEAT_TIMEOUT;

    loop {
        tokio::select! {
            _ = pings.tick() => {
                if !start_ping(&channel, id) {
                    return;
                }

                // Sent from its own task: a writer stuck on a dead path must not
                // stop this loop from noticing it. The timeout bounds how long a
                // stuck ping keeps the socket.
                let sink = SharedSink::clone(&sink);

                tokio::spawn(async move {
                    let _ = timeout(HEARTBEAT_INTERVAL, async {
                        sink.lock().await.send(Message::Ping(Bytes::new())).await
                    })
                    .await;
                });
            }
            () = sleep_until(check_at) => match check(&channel, id) {
                Beat::Wait(next) => check_at = next,
                Beat::Stop => return,
                Beat::Dead => {
                    if let Some(channel) = channel.upgrade() {
                        let queued = channel.lock().heartbeat_failure(id);

                        if queued {
                            channel.dispatch_events();
                        }
                    }

                    return;
                }
            },
        }
    }
} // end fn heartbeat

/// Records a ping that is about to go out, unless an older one still awaits
/// its pong. Reports `false` when the connection is gone.
fn start_ping(channel: &Weak<ChannelShared>, id: u64) -> bool {
    let Some(channel) = channel.upgrade() else {
        return false;
    };

    let mut inner = channel.lock();

    let Some(connection) = inner.connection_mut(id) else {
        return false;
    };

    let state = &mut connection.heartbeat;

    if state.unanswered_since.is_none() {
        state.unanswered_since = Some(state.total_reading_time());
    }

    true
} // end fn start_ping

fn check(channel: &Weak<ChannelShared>, id: u64) -> Beat {
    let Some(channel) = channel.upgrade() else {
        return Beat::Stop;
    };

    let inner = channel.lock();

    let Some(connection) = inner.connection(id) else {
        return Beat::Stop;
    };

    let state = &connection.heartbeat;

    let Some(unanswered_since) = state.unanswered_since else {
        return Beat::Wait(Instant::now() + HEARTBEAT_TIMEOUT);
    };

    let used = state.total_reading_time().saturating_sub(unanswered_since);

    match HEARTBEAT_TIMEOUT.checked_sub(used) {
        Some(left) if !left.is_zero() => Beat::Wait(Instant::now() + left),
        _ => Beat::Dead,
    }
} // end fn check
