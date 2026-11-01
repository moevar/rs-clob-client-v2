//! Venue doubles and helpers for the WebSocket clients' tests.

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use futures::{SinkExt as _, Stream, StreamExt as _};
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

/// A venue for one connection: it answers each request but the heartbeat with `answer`, and
/// sends `marker` each time the returned sender is given `()`. Returns its endpoint and that
/// sender.
pub(crate) async fn answering(
    answer: String,
    marker: String,
) -> (String, mpsc::UnboundedSender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (send_marker, mut markers) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        loop {
            let reply = tokio::select! {
                message = socket.next() => match message {
                    Some(Ok(Message::Text(text))) if text == "PING" => "PONG".to_owned(),
                    Some(Ok(Message::Text(_))) => answer.clone(),
                    Some(Ok(_)) => continue,
                    _ => break,
                },
                Some(()) = markers.recv() => marker.clone(),
            };
            if socket.send(Message::Text(reply.into())).await.is_err() {
                break;
            }
        }
    });
    (endpoint, send_marker)
}

/// The stream's first item, read for at most 5 s on a runtime of the calling thread.
pub(crate) fn first<T, E: std::fmt::Debug>(stream: impl Stream<Item = Result<T, E>>) -> T {
    let mut stream = pin!(stream);
    Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async { timeout(Duration::from_secs(5), stream.next()).await })
        .expect("the stream should yield within 5 s")
        .expect("the stream should go on")
        .unwrap()
}

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
