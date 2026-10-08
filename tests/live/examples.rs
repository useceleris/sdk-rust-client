//! The quickstart example (catalogue section 15). The example's flow runs
//! with a provider that signs with this suite's test-only signer, in place of
//! the example's credential endpoint.

use useceleris_client::CredentialProvider;

use crate::support::environment::websocket_url;
use crate::support::signer::{Claims, sign};

// The example's `main` and its placeholder endpoint are not used here.
#[allow(dead_code)]
#[path = "../../examples/quickstart.rs"]
mod quickstart;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_the_quickstart() {
    // The quickstart publishes and receives on one connection.
    let provider = CredentialProvider::new(|request| {
        let claims = Claims {
            allow_echo: Some(true),
            ..Claims::channels(&[&request.channel_reference])
        };

        let credentials = sign(&claims);

        async move { Ok(credentials) }
    });

    let summary = quickstart::run(websocket_url(), provider)
        .await
        .expect("the quickstart runs");

    assert_eq!((summary.delivered, summary.present), (1, 1));
} // end fn runs_the_quickstart
