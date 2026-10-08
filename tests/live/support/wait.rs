//! Waits. Each `await_*` helper registers its listener at once, before the
//! action that causes the event, and returns a [`Waiter`] for the first value
//! that matches.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::{Instant, sleep, timeout};
use useceleris_client::{
    CelerisError, Channel, ChannelState, PresenceEvent, RecoveryEvent, Registration, Segment,
    ServerError, ServerErrorResource, ServerErrorType,
};

use crate::support::collect::Delivery;

/// The usual time to wait for a delivery or an event.
pub const EVENT_WAIT: Duration = Duration::from_secs(15);

/// The time to wait for a body, which can come by replay.
const BODY_WAIT: Duration = Duration::from_secs(25);

/// The first value that matched, once it arrives.
pub struct Waiter<Value> {
    arrived: oneshot::Receiver<Value>,
    registration: Registration,
    description: String,
    timeout: Duration,
} // end struct Waiter

impl<Value> Waiter<Value> {
    fn new(
        arrived: oneshot::Receiver<Value>,
        registration: Registration,
        description: &str,
        timeout: Duration,
    ) -> Waiter<Value> {
        Waiter {
            arrived,
            registration,
            description: description.to_owned(),
            timeout,
        }
    } // end fn new

    /// Waits for the value, and removes the listener.
    #[track_caller]
    pub fn wait(self) -> impl Future<Output = Value> {
        let location = std::panic::Location::caller();

        async move {
            let outcome = timeout(self.timeout, self.arrived).await;
            self.registration.remove();

            match outcome {
                Ok(Ok(value)) => value,
                _ => panic!("{location}: timed out waiting for {}", self.description),
            }
        }
    } // end method wait
} // end impl Waiter

/// A listener that sends the first value that matches `predicate`.
fn first_match<Value: Send + 'static>(
    predicate: impl Fn(&Value) -> bool + Send + Sync + 'static,
) -> (
    impl Fn(Value) + Send + Sync + 'static,
    oneshot::Receiver<Value>,
) {
    let (sender, receiver) = oneshot::channel();
    let sender = Mutex::new(Some(sender));

    let deliver = move |value: Value| {
        if !predicate(&value) {
            return;
        }

        let taken = sender.lock().unwrap_or_else(PoisonError::into_inner).take();

        if let Some(sender) = taken {
            let _ = sender.send(value);
        }
    };

    (deliver, receiver)
} // end fn first_match

pub fn await_message(
    segment: &Segment,
    predicate: impl Fn(&Delivery) -> bool + Send + Sync + 'static,
    description: &str,
    timeout: Duration,
) -> Waiter<Delivery> {
    let (deliver, arrived) = first_match(predicate);

    let registration =
        segment.on_message(move |payload, metadata| deliver(Delivery::new(payload, metadata)));

    Waiter::new(arrived, registration, description, timeout)
} // end fn await_message

/// Waits for the first delivery of `body` on the segment.
pub fn await_body(segment: &Segment, body: &str) -> Waiter<Delivery> {
    let wanted = body.to_owned();

    await_message(
        segment,
        move |delivery| delivery.payload == wanted.as_bytes(),
        &format!("{body:?} on {}", segment.id()),
        BODY_WAIT,
    )
} // end fn await_body

/// Waits for the first delivery of `body` to the channel listener.
pub fn await_channel_body(channel: &Channel, body: &str) -> Waiter<Delivery> {
    let wanted = body.to_owned();
    let (deliver, arrived) =
        first_match(move |delivery: &Delivery| delivery.payload == wanted.as_bytes());

    let registration = channel
        .events()
        .on_message(move |payload, metadata| deliver(Delivery::new(payload, metadata)));

    Waiter::new(
        arrived,
        registration,
        &format!("{body:?} on the channel listener"),
        BODY_WAIT,
    )
} // end fn await_channel_body

pub fn await_presence(
    segment: &Segment,
    predicate: impl Fn(&PresenceEvent) -> bool + Send + Sync + 'static,
    description: &str,
    timeout: Duration,
) -> Waiter<PresenceEvent> {
    let (deliver, arrived) = first_match(predicate);
    let registration = segment.on_presence(move |event| deliver(event.clone()));

    Waiter::new(arrived, registration, description, timeout)
} // end fn await_presence

pub fn await_error(
    channel: &Channel,
    predicate: impl Fn(&CelerisError) -> bool + Send + Sync + 'static,
    description: &str,
    timeout: Duration,
) -> Waiter<CelerisError> {
    let (deliver, arrived) = first_match(predicate);
    let registration = channel
        .events()
        .on_error(move |error| deliver(error.clone()));

    Waiter::new(arrived, registration, description, timeout)
} // end fn await_error

pub fn await_state(
    channel: &Channel,
    state: ChannelState,
    timeout: Duration,
) -> Waiter<ChannelState> {
    let (deliver, arrived) = first_match(move |changed: &ChannelState| *changed == state);
    let registration = channel.events().on_state_change(deliver);

    Waiter::new(
        arrived,
        registration,
        &format!("the {state} state"),
        timeout,
    )
} // end fn await_state

pub fn await_recovery(channel: &Channel, timeout: Duration) -> Waiter<RecoveryEvent> {
    let (deliver, arrived) = first_match(|_: &RecoveryEvent| true);
    let registration = channel
        .events()
        .on_recovery(move |event| deliver(event.clone()));

    Waiter::new(arrived, registration, "the recovery event", timeout)
} // end fn await_recovery

/// Waits for a permission denial that names this command and segment.
pub fn await_denial(channel: &Channel, sub_type: &str, segment_id: &str) -> Waiter<CelerisError> {
    let (wanted_sub_type, wanted_segment) = (sub_type.to_owned(), segment_id.to_owned());

    await_error(
        channel,
        move |error| is_denial(error, &wanted_sub_type, &wanted_segment),
        &format!("a {sub_type} denial for {segment_id}"),
        EVENT_WAIT,
    )
} // end fn await_denial

/// Polls `condition` every 100 ms until it holds.
#[track_caller]
pub fn eventually<Condition: Fn() -> bool>(
    condition: Condition,
    description: &str,
    timeout: Duration,
) -> impl Future<Output = ()> + use<Condition> {
    let location = std::panic::Location::caller();
    let description = description.to_owned();

    async move {
        let deadline = Instant::now() + timeout;

        while !condition() {
            if Instant::now() >= deadline {
                panic!("{location}: timed out waiting for {description}");
            }

            sleep(Duration::from_millis(100)).await;
        }
    }
} // end fn eventually

#[track_caller]
pub fn until_state(
    channel: &Channel,
    state: ChannelState,
    timeout: Duration,
) -> impl Future<Output = ()> {
    let channel = channel.clone();

    eventually(
        move || channel.state() == state,
        &format!("the {state} state"),
        timeout,
    )
} // end fn until_state

/// Time for subscriptions and joins to take effect on the server.
pub async fn settle() {
    sleep(Duration::from_millis(1500)).await;
} // end fn settle

pub fn server_error(error: &CelerisError) -> Option<&ServerError> {
    match error {
        CelerisError::Server(server_error) => Some(server_error),
        _ => None,
    }
} // end fn server_error

pub fn is_server_error(error: &CelerisError, error_type: &ServerErrorType) -> bool {
    server_error(error).is_some_and(|server_error| server_error.error_type == *error_type)
} // end fn is_server_error

pub fn is_denial(error: &CelerisError, sub_type: &str, segment_id: &str) -> bool {
    server_error(error).is_some_and(|denial| {
        denial.error_type == ServerErrorType::PermissionDeniedError
            && denial.sub_type.as_deref() == Some(sub_type)
            && denial.resource == ServerErrorResource::Text(segment_id.to_owned())
    })
} // end fn is_denial
