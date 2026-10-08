//! The acceptance suites, run against a real Celeris stack with `make live`.
//!
//! Put CELERIS_WS_URL, CELERIS_CLIENT_ID and CELERIS_SIGNING_SECRET in the
//! crate's `.env` (gitignored) or in the environment. Real environment
//! variables take precedence. The optional CELERIS_WS_URL_PEER is a gateway
//! that routes to a different server node. The cross-node tests use it for
//! their second connection, and skip without it.

mod support;

mod authentication;
mod crossnode;
mod examples;
mod heartbeat;
mod lifecycle;
mod messaging;
mod options;
mod payload_formats;
mod permissions;
mod presence;
mod reconnect;
mod replay;
mod segments;
