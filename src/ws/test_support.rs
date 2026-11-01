//! Venue doubles and helpers for the WebSocket clients' tests.

use std::cell::RefCell;
use std::future::Future;
use std::pin::pin;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::time::{Duration, Instant};

use futures::{SinkExt as _, Stream, StreamExt as _};
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(5);

thread_local! {
    /// A writer's pause between its read of the connection's state and its report
    /// (`ConnectionManager::emit_reconnect_write_unavailable`), set on the writer's thread.
    static AFTER_STATE_READ: RefCell<Option<Box<dyn FnMut()>>> = const { RefCell::new(None) };
}

/// Called by a writer that read `Reconnecting`, before its report.
pub(crate) fn after_state_read() {
    AFTER_STATE_READ.with_borrow_mut(|pause| {
        if let Some(pause) = pause {
            pause();
        }
    });
}

/// A write on a thread of its own, held after its read of the state until it is released.
pub(crate) struct HeldWrite {
    resume: Sender<()>,
    done: Receiver<()>,
}

impl HeldWrite {
    /// Starts `write` and returns once it is held: it must read `Reconnecting`.
    pub(crate) fn start(write: impl FnOnce() + Send + 'static) -> Self {
        let (paused, is_paused) = channel();
        let (resume, resumed) = channel::<()>();
        let (finished, done) = channel();
        thread::spawn(move || {
            AFTER_STATE_READ.set(Some(Box::new(move || {
                _ = paused.send(());
                _ = resumed.recv();
            })));
            write();
            _ = finished.send(());
        });
        is_paused
            .recv_timeout(WAIT)
            .expect("the held write should read `Reconnecting`");
        Self { resume, done }
    }

    /// Lets the write go on. True once it has returned, within 5 s.
    pub(crate) fn release(&self) -> bool {
        _ = self.resume.send(());
        self.done.recv_timeout(WAIT).is_ok()
    }
}

/// A venue that closes the first connection once it is up. It answers the second connection's
/// handshake only once the returned sender is given `()`, then keeps it, answering its heartbeat.
/// Returns its endpoint and that sender.
pub(crate) async fn gate_the_second() -> (String, mpsc::UnboundedSender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (proceed, mut proceeds) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        let mut first = tokio_tungstenite::accept_async(first).await.unwrap();
        drop(first.close(None).await);
        drop(first);
        let (second, _) = listener.accept().await.unwrap();
        proceeds.recv().await;
        let mut second = tokio_tungstenite::accept_async(second).await.unwrap();
        while let Some(Ok(message)) = second.next().await {
            if message == Message::Text("PING".into()) {
                drop(second.send(Message::Text("PONG".into())).await);
            }
        }
    });
    (endpoint, proceed)
}

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

/// Runs `future` on `runtime` for at most 5 s; past that, panics with `what`.
pub(crate) fn within<F: Future>(runtime: &Runtime, what: &str, future: F) -> F::Output {
    runtime.block_on(async {
        timeout(WAIT, future)
            .await
            .unwrap_or_else(|_| panic!("{what}"))
    })
}

/// Checks `condition` every millisecond for at most 5 s. True once it holds.
pub(crate) fn wait_until(condition: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + WAIT;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(1));
    }
    false
}

/// The stream's first item, read for at most 5 s on a runtime of the calling thread.
pub(crate) fn first<T, E: std::fmt::Debug>(stream: impl Stream<Item = Result<T, E>>) -> T {
    let mut stream = pin!(stream);
    Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async { timeout(WAIT, stream.next()).await })
        .expect("the stream should yield within 5 s")
        .expect("the stream should go on")
        .unwrap()
}
