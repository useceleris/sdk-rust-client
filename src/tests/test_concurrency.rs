//! Concurrency and listener re-entry (catalogue section 14, LIFE-04, DEV-01).

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::time::{sleep, timeout};

use crate::events::Registration;
use crate::tests::support::{
    Recorder, connected_channel, delivery_frame, notice_frame, publish_command, segment_command,
    settle, wait_until,
};
use crate::values::{ChannelState, MessageMetadata};

/// Listeners never run under an SDK lock, so they may call back into the
/// channel. Calls they make never wait for their own events: those follow
/// once the listener returns.
#[tokio::test(start_paused = true)]
async fn listeners_may_call_back_into_the_channel() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let outcomes = Recorder::new();
    let own_registration: Arc<Mutex<Option<Registration>>> = Arc::new(Mutex::new(None));

    let listener = {
        let handle = Handle::current();
        let channel = channel.clone();
        let chat = chat.clone();
        let outcomes = outcomes.clone();
        let own_registration = Arc::clone(&own_registration);

        move |_payload: &[u8], metadata: &MessageMetadata| {
            let echo = format!("echo-{}", metadata.message_id);

            if let Err(error) = handle.block_on(chat.publish_with_message_id("echo", &echo)) {
                outcomes.push(format!("publish failed: {error}"));
            }

            match chat.subscribe() {
                Ok(subscription) => subscription.cancel(),
                Err(error) => outcomes.push(format!("subscribe failed: {error}")),
            }

            channel.events().on_notice(|_notice| {}).remove();

            if let Some(registration) = own_registration.lock().unwrap().as_ref() {
                registration.remove();
            }

            outcomes.push(format!("done {}", metadata.message_id));
        }
    };

    *own_registration.lock().unwrap() = Some(chat.on_message(listener));

    let delivered = Recorder::new();
    let collector = delivered.clone();
    channel
        .events()
        .on_message(move |_payload, metadata| collector.push(metadata.message_id.clone()));

    peer.send(delivery_frame("alice", "chat", "id-1", 1, b"x"))
        .await;
    peer.send(delivery_frame("alice", "chat", "id-2", 2, b"x"))
        .await;

    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "echo-id-1", "echo")
    );
    assert_eq!(peer.next_command().await, segment_command("SUB", "chat"));
    assert_eq!(peer.next_command().await, segment_command("UNSUB", "chat"));

    // The second message is the control: the removed listener misses it.
    wait_until("both deliveries", || delivered.len() == 2).await;
    settle(&channel).await;
    assert_eq!(delivered.all(), ["id-1", "id-2"]);
    assert_eq!(outcomes.all(), ["done id-1"]);
} // end fn listeners_may_call_back_into_the_channel

#[tokio::test(start_paused = true)]
async fn close_from_inside_a_listener() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    settle(&channel).await;
    let log = Recorder::new();
    let states = log.clone();
    channel
        .events()
        .on_state_change(move |state| states.push(state.to_string()));

    let closer = log.clone();
    let handle = Handle::current();
    let listener_channel = channel.clone();
    channel.events().on_notice(move |_notice| {
        handle.block_on(listener_channel.close());
        closer.push("closed in listener".to_owned());
    });

    let messages = log.clone();
    channel
        .segment("chat")
        .unwrap()
        .on_message(move |_payload, _metadata| messages.push("message".to_owned()));

    let mut batch = b"*2\n".to_vec();
    batch.extend(notice_frame(1, ""));
    batch.extend(delivery_frame("alice", "chat", "id-1", 1, b"x"));
    peer.send(batch).await;

    wait_until("the closed state", || log.len() == 3).await;
    sleep(Duration::from_secs(5)).await;
    settle(&channel).await;

    // The batch's later entry is never routed once the channel closed.
    assert_eq!(log.all(), ["closed in listener", "closing", "closed"]);
    assert_eq!(channel.state(), ChannelState::Closed);
} // end fn close_from_inside_a_listener

/// A listener that calls close while close delivers events returns, and so
/// does the outer close.
#[tokio::test(start_paused = true)]
async fn close_from_a_listener_during_close_returns() {
    let (channel, _peer, _server, _log) = connected_channel().await;
    settle(&channel).await;
    let log = Recorder::new();
    let collector = log.clone();
    let handle = Handle::current();
    let listener_channel = channel.clone();

    channel.events().on_state_change(move |state| {
        if state == ChannelState::Closing {
            handle.block_on(listener_channel.close());
            collector.push("inner close returned".to_owned());
        }
    });

    timeout(Duration::from_secs(30), channel.close())
        .await
        .expect("close returns");
    wait_until("the inner close", || log.len() == 1).await;
    settle(&channel).await;

    assert_eq!(log.all(), ["inner close returned"]);
    assert_eq!(channel.state(), ChannelState::Closed);
} // end fn close_from_a_listener_during_close_returns

/// Counts listeners that run at once, and records any overlap.
#[derive(Clone, Default)]
struct OverlapCheck {
    running: Arc<AtomicUsize>,
    overlapped: Arc<AtomicBool>,
} // end struct OverlapCheck

impl OverlapCheck {
    fn run(&self) {
        if self.running.fetch_add(1, Ordering::SeqCst) != 0 {
            self.overlapped.store(true, Ordering::SeqCst);
        }

        // Widens the window in which another listener could start.
        std::thread::sleep(Duration::from_micros(200));
        self.running.fetch_sub(1, Ordering::SeqCst);
    } // end method run
} // end impl OverlapCheck

/// LIFE-04: concurrent use from many tasks on many threads is safe, and
/// listeners run one at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_use_is_safe() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let check = OverlapCheck::default();
    let delivered = Recorder::new();

    {
        let check = check.clone();
        let delivered = delivered.clone();
        channel.events().on_message(move |_payload, metadata| {
            check.run();
            delivered.push(metadata.message_id.clone());
        });
    }

    let mut workers = Vec::new();

    for worker in 0..8 {
        let channel = channel.clone();
        let check = check.clone();

        workers.push(tokio::spawn(async move {
            let chat = channel.segment(&format!("chat-{}", worker % 3)).unwrap();
            let mut subscriptions = Vec::new();

            for index in 0..20 {
                let check = check.clone();
                let registration = chat.on_message(move |_payload, _metadata| check.run());
                subscriptions.push(chat.subscribe().unwrap());
                chat.publish_with_message_id("x", &format!("m-{worker}-{index}"))
                    .await
                    .unwrap();
                registration.remove();
            }

            for subscription in subscriptions {
                subscription.cancel();
            }
        }));
    }

    let sender = tokio::spawn(async move {
        for index in 0..50_i64 {
            let segment_id = format!("chat-{}", index % 3);
            let message_id = format!("id-{index}");
            peer.send(delivery_frame(
                "alice",
                &segment_id,
                &message_id,
                index,
                b"x",
            ))
            .await;
        }

        peer
    });

    for worker in workers {
        worker.await.unwrap();
    }

    let _peer = sender.await.unwrap();
    wait_until("every delivery", || delivered.len() == 50).await;
    settle(&channel).await;

    let expected: HashSet<String> = (0..50).map(|index| format!("id-{index}")).collect();
    let received: HashSet<String> = delivered.all().into_iter().collect();
    assert_eq!(received, expected);
    assert_eq!(delivered.len(), 50);
    assert!(!check.overlapped.load(Ordering::SeqCst));
    assert_eq!(channel.state(), ChannelState::Connected);

    channel.close().await;
    assert_eq!(channel.state(), ChannelState::Closed);
} // end fn concurrent_use_is_safe

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_connect_and_close_under_load() {
    let rounds = async {
        for _ in 0..50 {
            let (channel, mut peer, _server, _log) = connected_channel().await;
            let chat = channel.segment("chat").unwrap();
            let _subscription = chat.subscribe().unwrap();
            chat.on_message(|_payload, _metadata| {});

            let load = tokio::spawn(async move {
                for index in 0..5_i64 {
                    let message_id = format!("id-{index}");

                    if !peer
                        .try_send(delivery_frame("alice", "chat", &message_id, index, b"x"))
                        .await
                    {
                        break;
                    }
                }

                peer
            });

            channel.close().await;
            assert_eq!(channel.state(), ChannelState::Closed);

            // The client let go of the socket.
            let mut peer = load.await.unwrap();
            while peer.next_message().await.is_some() {}
        }
    };

    timeout(Duration::from_secs(60), rounds)
        .await
        .expect("connect and close never hang");
} // end fn repeated_connect_and_close_under_load
