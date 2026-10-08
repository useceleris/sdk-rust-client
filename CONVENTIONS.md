# Conventions

Simplicity and maintainability are paramount. These rules apply to every change. The surface contract is in the specifications repository, and the JavaScript client is the reference implementation that this crate mirrors.

## Names

Use full domain words: `release_interest`, `queue_interest_sync`, `segment_id`, `credential_provider`. Do not use abbreviations or single-letter names, except in tight loops. A name says what the thing is. A comment states only a constraint that the code cannot show.

## Simplicity over abstraction

Solve the problem with the simplest structure that stays readable. Do not add registries, factories, event frameworks or wrapper layers. A helper type must remove real duplication that exists now (`ListenerSet` does; a "Manager" does not). Prefer a function to a type, and a method on an existing type to a new type.

Two seams exist, and no others: `ClientConfiguration::dial`, so that tests put an in-memory peer in place of the network, and `ClientConfiguration::random`, so that tests fix the jitter. Time needs no seam, because the unit tests run on Tokio's paused clock.

## Concurrency

- One lock, `ChannelShared::lock`, guards all of a channel's state. Never hold it across an `.await`, and never hold it while user code runs.
- Listeners run on Tokio's blocking thread pool, one at a time per channel, in the order that the state changed. Queue every event with `ChannelInner::queue_event` under the lock, then call `ChannelShared::dispatch_events` after the lock is released. Never call a listener directly.
- A task holds a `Weak<ChannelShared>`, so that dropping every handle stops the channel's tasks and its socket.
- Every future that the public surface returns is cancel-safe: dropping it takes back what it started (a queued publish, a query slot, a connection attempt).

## Maintainability

- Every fixed value is in `src/constants.rs`, as a `pub(crate)` constant in `SCREAMING_SNAKE_CASE`. Validation rules stay beside the code that uses them.
- Only `src/lib.rs` re-exports. Every other module imports from the module that defines the item.
- Delete code in the same change that makes it unnecessary.
- Every public item traces to a requirement or a recorded decision in the specifications repository (SEG-01, DEV-01, REV-01, ...). The public surface is pinned in one test, `tests/surface.rs`.
- Errors carry a stable variant and a message that names what failed and the rule or limit that it broke. Never put received values, input values, credentials or server text into an SDK message, and never wrap a cause: a dial error quotes the URL, which carries the credentials.

## Tests

- Unit tests of crate-private behaviour are in `src/tests/test_<topic>.rs`, with shared fixtures in `src/tests/support.rs`. No source file contains tests.
- Integration tests are in `tests/`: `layout.rs`, `surface.rs`, and the live suites in `tests/live/` (one binary, one module per topic). The live suites run only with `make live`.
- Unit tests are deterministic: `#[tokio::test(start_paused = true)]`, a fixed jitter, and the in-memory peer. They catch defects of this crate only. Golden vectors are written by hand, never produced by the code under test.
- An assertion must fail when nothing happens. Before a test asserts that nothing arrives, it first receives a control message.

## Layout

rustfmt owns formatting, with its defaults. Two more rules apply, and `tests/layout.rs` enforces them on every Rust file of the crate:

- A statement or item that ends with a closing brace on a line of its own (a block, a closure, a struct literal) is followed by a blank line before the next statement or item. A `use` declaration is exempt.
- Every function, method, struct, enum, trait and impl block whose body spans several lines ends with a marker that names it: `} // end fn name`, `} // end method name` (a function with a `self` receiver), `} // end struct Name`, `} // end enum Name`, `} // end trait Name`, `} // end impl Name` or `} // end impl Trait for Name`.

`make check` must pass.

## Writing

Write documentation and comments in ASD-STE100 Simplified Technical English: short sentences, the active voice, one instruction per sentence, and approved words. Technical names such as `subscribe` and `publish` are permitted.
