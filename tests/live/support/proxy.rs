//! A TCP proxy in front of the realtime service that a test can break, as a
//! network would:
//!
//! - `set_refusing`: new connections close at once.
//! - `set_blackhole`: new connections stay open but carry nothing.
//! - `blackhole_all`: the open connections silently stop carrying anything,
//!   and neither side's socket closes.
//! - `stall_upstream`: the proxy stops reading what clients send, so their
//!   socket buffers fill.
//! - `drop_all`: every open connection is reset (RST).
//! - `close_all`: every open connection is closed (FIN).

use std::sync::{Arc, Mutex, PoisonError};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::support::environment::{target_authority, target_path};

/// What one link does next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LinkCommand {
    Carry,
    Blackhole,
    Reset,
    Close,
} // end enum LinkCommand

#[derive(Default)]
struct ProxyFlags {
    refusing: bool,
    blackhole: bool,
    links: Vec<watch::Sender<LinkCommand>>,
} // end struct ProxyFlags

struct ProxyShared {
    flags: Mutex<ProxyFlags>,
    stalled: watch::Sender<bool>,
} // end struct ProxyShared

impl ProxyShared {
    fn flags(&self) -> std::sync::MutexGuard<'_, ProxyFlags> {
        self.flags.lock().unwrap_or_else(PoisonError::into_inner)
    } // end method flags

    /// Sends `command` to every open link and forgets them.
    fn end_links(&self, command: LinkCommand) {
        let links = std::mem::take(&mut self.flags().links);

        for link in links {
            link.send_replace(command);
        }
    } // end method end_links
} // end impl ProxyShared

pub struct DroppingProxy {
    url: String,
    shared: Arc<ProxyShared>,
    accepting: JoinHandle<()>,
} // end struct DroppingProxy

impl DroppingProxy {
    pub async fn start() -> DroppingProxy {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the proxy binds a loopback port");
        let address = listener.local_addr().expect("the proxy has an address");

        let shared = Arc::new(ProxyShared {
            flags: Mutex::new(ProxyFlags::default()),
            stalled: watch::Sender::new(false),
        });

        let accepting = tokio::spawn(accept(listener, Arc::clone(&shared)));

        DroppingProxy {
            url: format!("ws://{address}{}", target_path()),
            shared,
            accepting,
        }
    } // end fn start

    pub fn url(&self) -> &str {
        &self.url
    } // end method url

    pub fn set_refusing(&self, refusing: bool) {
        self.shared.flags().refusing = refusing;
    } // end method set_refusing

    pub fn set_blackhole(&self, blackhole: bool) {
        self.shared.flags().blackhole = blackhole;
    } // end method set_blackhole

    pub fn blackhole_all(&self) {
        for link in &self.shared.flags().links {
            link.send_replace(LinkCommand::Blackhole);
        }
    } // end method blackhole_all

    pub fn stall_upstream(&self, stalled: bool) {
        self.shared.stalled.send_replace(stalled);
    } // end method stall_upstream

    pub fn drop_all(&self) {
        self.shared.end_links(LinkCommand::Reset);
    } // end method drop_all

    pub fn close_all(&self) {
        self.shared.end_links(LinkCommand::Close);
    } // end method close_all
} // end impl DroppingProxy

impl Drop for DroppingProxy {
    fn drop(&mut self) {
        self.accepting.abort();
        self.shared.stalled.send_replace(false);
        self.shared.end_links(LinkCommand::Reset);
    } // end method drop
} // end impl Drop for DroppingProxy

async fn accept(listener: TcpListener, shared: Arc<ProxyShared>) {
    loop {
        let Ok((client, _)) = listener.accept().await else {
            return;
        };

        let (refusing, blackhole, commands) = {
            let mut flags = shared.flags();
            let (sender, receiver) = watch::channel(LinkCommand::Carry);

            if !flags.refusing {
                flags.links.push(sender);
            }

            (flags.refusing, flags.blackhole, receiver)
        };

        if refusing {
            drop(client);

            continue;
        }

        tokio::spawn(carry(
            client,
            blackhole,
            commands,
            shared.stalled.subscribe(),
        ));
    }
} // end fn accept

/// Carries one connection until either side ends or a command ends it.
async fn carry(
    mut client: TcpStream,
    blackhole: bool,
    mut commands: watch::Receiver<LinkCommand>,
    mut stalled: watch::Receiver<bool>,
) {
    let mut upstream = if blackhole {
        None
    } else {
        match TcpStream::connect(target_authority()).await {
            Ok(upstream) => Some(upstream),
            Err(_) => return,
        }
    };

    let mut from_client = vec![0_u8; 32 * 1024];
    let mut from_upstream = vec![0_u8; 32 * 1024];

    loop {
        let command = *commands.borrow_and_update();

        match command {
            LinkCommand::Reset => {
                reset(&client);

                if let Some(upstream) = &upstream {
                    reset(upstream);
                }

                return;
            }
            LinkCommand::Close => {
                let _ = client.shutdown().await;

                if let Some(upstream) = upstream.as_mut() {
                    let _ = upstream.shutdown().await;
                }

                return;
            }
            LinkCommand::Carry | LinkCommand::Blackhole => {}
        }

        let carrying = command == LinkCommand::Carry && upstream.is_some();
        let client_paused = carrying && *stalled.borrow_and_update();

        tokio::select! {
            changed = commands.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = stalled.changed() => {}
            read = client.read(&mut from_client), if !client_paused => {
                let Ok(count @ 1..) = read else {
                    return;
                };

                if carrying
                    && let Some(upstream) = upstream.as_mut()
                    && upstream.write_all(&from_client[..count]).await.is_err()
                {
                    return;
                }
            }
            read = read_upstream(&mut upstream, &mut from_upstream) => {
                let Ok(count @ 1..) = read else {
                    return;
                };

                if carrying && client.write_all(&from_upstream[..count]).await.is_err() {
                    return;
                }
            }
        }
    }
} // end fn carry

/// Reads from the upstream socket, or waits forever when there is none.
async fn read_upstream(
    upstream: &mut Option<TcpStream>,
    buffer: &mut [u8],
) -> std::io::Result<usize> {
    match upstream {
        Some(upstream) => upstream.read(buffer).await,
        None => std::future::pending().await,
    }
} // end fn read_upstream

/// Makes the coming drop send RST instead of FIN.
fn reset(stream: &TcpStream) {
    let _ = stream.set_zero_linger();
} // end fn reset
