//! Venue doubles and helpers for the WebSocket clients' tests.

use std::future::Future;
use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

/// A venue that closes the first connection once it is up and keeps the others, answering their
/// heartbeat. Returns its endpoint.
pub(crate) async fn close_the_first() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let mut number = 0;
        while let Ok((stream, _)) = listener.accept().await {
            let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await else {
                continue;
            };
            number += 1;
            if number == 1 {
                drop(socket.close(None).await);
                continue;
            }
            tokio::spawn(async move {
                while let Some(Ok(message)) = socket.next().await {
                    if message == Message::Text("PING".into()) {
                        drop(socket.send(Message::Text("PONG".into())).await);
                    }
                }
            });
        }
    });
    endpoint
}

/// Runs `future` on `runtime` for at most 5 s; past that, panics with `what`.
pub(crate) fn within<F: Future>(runtime: &Runtime, what: &str, future: F) -> F::Output {
    runtime.block_on(async {
        timeout(Duration::from_secs(5), future)
            .await
            .unwrap_or_else(|_| panic!("{what}"))
    })
}
