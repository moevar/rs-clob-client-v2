//! The market channel's first connections, in the order each of their races needs: the reconnect
//! handler's task first runs after a `Connected`, and the raw stream is subscribed after one.
#![cfg(all(feature = "clob", feature = "ws"))]
#![allow(
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    reason = "Do not need additional syntax for setting up tests"
)]

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt as _, Stream, StreamExt as _};
use polymarket_client_sdk_v2::Result as SdkResult;
use polymarket_client_sdk_v2::clob::ws::interest::InterestTracker;
use polymarket_client_sdk_v2::clob::ws::subscription::SubscriptionManager;
use polymarket_client_sdk_v2::clob::ws::{
    Client, MarketRawStreamEvent, MarketStreamContinuity, SubscriptionRequest, WsMessage,
};
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::config::Config;
use polymarket_client_sdk_v2::ws::{
    ConnectionDiagnosticKind, ConnectionManager, RawConnectionEvent, RawFrameProtocol,
};
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Notify, broadcast, mpsc};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

type Connection = ConnectionManager<WsMessage, Arc<InterestTracker>>;

/// A market channel double that completes no handshake until it is opened, and passes on every
/// text frame a client sends but its heartbeat, in order, with the number of the connection it came
/// on. It answers each heartbeat and sends its connections the frames it is given. If told to, it
/// closes the first connection after its first request.
struct Server {
    addr: SocketAddr,
    gate: Arc<Notify>,
    requests: mpsc::UnboundedReceiver<(usize, String)>,
    frames: broadcast::Sender<Message>,
}

impl Server {
    async fn start(close_first: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let gate = Arc::new(Notify::new());
        let (sent, requests) = mpsc::unbounded_channel();
        let (frames, _) = broadcast::channel(4096);
        tokio::spawn({
            let gate = Arc::clone(&gate);
            let frames = frames.clone();
            async move {
                gate.notified().await;
                let mut number = 0;
                while let Ok((stream, _)) = listener.accept().await {
                    let Ok(socket) = tokio_tungstenite::accept_async(stream).await else {
                        continue;
                    };
                    number += 1;
                    let sent = sent.clone();
                    let (mut write, mut read) = socket.split();
                    let mut given = frames.subscribe();
                    let (pong, mut pongs) = mpsc::unbounded_channel();
                    let writer = tokio::spawn(async move {
                        loop {
                            let frame = tokio::select! {
                                Ok(frame) = given.recv() => frame,
                                Some(()) = pongs.recv() => Message::Text("PONG".into()),
                                else => break,
                            };
                            if write.send(frame).await.is_err() {
                                break;
                            }
                        }
                    });
                    tokio::spawn(async move {
                        while let Some(Ok(message)) = read.next().await {
                            match message {
                                Message::Text(text) if text == "PING" => drop(pong.send(())),
                                Message::Text(text) => {
                                    drop(sent.send((number, text.to_string())));
                                    if close_first && number == 1 {
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }
                        // Both halves go, so the connection closes.
                        writer.abort();
                    });
                }
            }
        });
        Self {
            addr,
            gate,
            requests,
            frames,
        }
    }

    fn endpoint(&self) -> String {
        format!("ws://{}/ws/market", self.addr)
    }

    /// Lets the handshakes complete.
    fn open(&self) {
        self.gate.notify_one();
    }

    fn send(&self, frame: Message) {
        self.frames.send(frame).unwrap();
    }

    async fn request(&mut self) -> (usize, String) {
        timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .expect("the server should receive a request")
            .expect("the server should still run")
    }
}

fn subscribe(asset: u8) -> String {
    serde_json::to_string(&SubscriptionRequest::market(vec![U256::from(asset)])).unwrap()
}

/// The connection on its own runtime, and the handler's runtime, a current-thread one that runs
/// nothing until the test drives it.
fn runtimes() -> (Runtime, Runtime) {
    let connection = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let handler = Builder::new_current_thread().enable_all().build().unwrap();
    (connection, handler)
}

/// Runs the handler's task for the first time.
fn drive(handler: &Runtime) {
    handler.block_on(async {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    });
}

/// The handler's task first runs once the first connection is up. That connection sends the
/// subscribe queued before it, and nothing sends it again.
#[test]
fn a_handler_that_first_runs_after_the_first_connect_sends_no_subscribe_again() {
    let (connection_runtime, handler_runtime) = runtimes();
    let mut server = connection_runtime.block_on(Server::start(false));
    let interest = Arc::new(InterestTracker::default());
    let connection: Connection = {
        let _entered = connection_runtime.enter();
        ConnectionManager::new(server.endpoint(), Config::default(), Arc::clone(&interest)).unwrap()
    };
    let subscriptions = Arc::new(SubscriptionManager::new(connection.clone(), interest));
    let _first = subscriptions
        .subscribe_market(vec![U256::from(1_u8)])
        .unwrap();
    {
        let _entered = handler_runtime.enter();
        subscriptions.start_reconnection_handler();
    }

    // The handler has its state receiver; the first connection comes up and sends the queued
    // subscribe.
    server.open();
    let first = connection_runtime.block_on(server.request());
    assert_eq!(first, (1, subscribe(1)));
    assert!(connection.state().is_connected());

    // The handler's task runs for the first time, then the next subscribe is sent.
    drive(&handler_runtime);
    let _second = subscriptions
        .subscribe_market(vec![U256::from(2_u8)])
        .unwrap();
    let next = connection_runtime.block_on(server.request());
    assert_eq!(
        next,
        (1, subscribe(2)),
        "the handler sent the first connection's subscriptions again"
    );
}

/// The handler's task first runs once a reconnect has brought up the second connection. That
/// connection still gets the subscriptions.
#[test]
fn a_handler_that_first_runs_after_a_reconnect_resubscribes() {
    let (connection_runtime, handler_runtime) = runtimes();
    let mut server = connection_runtime.block_on(Server::start(true));
    let mut config = Config::default();
    config.reconnect.initial_backoff = Duration::from_millis(50);
    config.reconnect.max_backoff = Duration::from_millis(50);
    let interest = Arc::new(InterestTracker::default());
    let connection: Connection = {
        let _entered = connection_runtime.enter();
        ConnectionManager::new(server.endpoint(), config, Arc::clone(&interest)).unwrap()
    };
    let subscriptions = Arc::new(SubscriptionManager::new(connection.clone(), interest));
    let _first = subscriptions
        .subscribe_market(vec![U256::from(1_u8)])
        .unwrap();
    {
        let _entered = handler_runtime.enter();
        subscriptions.start_reconnection_handler();
    }

    // The first connection sends the subscribe and is closed; the second comes up.
    server.open();
    let first = connection_runtime.block_on(server.request());
    assert_eq!(first, (1, subscribe(1)));
    connection_runtime.block_on(async {
        let mut generation = connection.generation_receiver();
        let mut state = connection.state_receiver();
        timeout(Duration::from_secs(5), async {
            generation
                .wait_for(|generation| generation.as_u64() == 2)
                .await
                .unwrap();
            state.wait_for(|state| state.is_connected()).await.unwrap();
        })
        .await
        .expect("the second connection should come up");
    });

    // The handler's task runs for the first time, then the next subscribe is sent.
    drive(&handler_runtime);
    let _second = subscriptions
        .subscribe_market(vec![U256::from(2_u8)])
        .unwrap();
    let next = connection_runtime.block_on(server.request());
    assert_eq!(
        next,
        (2, subscribe(1)),
        "the second connection didn't get the subscriptions again first"
    );
}

async fn next_raw<S>(stream: &mut Pin<Box<S>>) -> MarketRawStreamEvent
where
    S: Stream<Item = SdkResult<MarketRawStreamEvent>> + ?Sized,
{
    timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the raw stream should yield")
        .expect("the raw stream should not end")
        .unwrap()
}

fn connected(event: &MarketRawStreamEvent, number: u64) -> bool {
    matches!(
        event,
        MarketRawStreamEvent::Continuity {
            generation,
            reason: MarketStreamContinuity::Connected,
        } if generation.as_u64() == number
    )
}

/// The raw stream is subscribed once the first connection is up: it still starts with that
/// connection's `Connected`.
#[tokio::test]
async fn a_raw_stream_subscribed_after_the_first_connect_starts_with_it() {
    let server = Server::start(false).await;
    server.open();
    let interest = Arc::new(InterestTracker::default());
    let connection: Connection =
        ConnectionManager::new(server.endpoint(), Config::default(), Arc::clone(&interest))
            .unwrap();
    let mut state = connection.state_receiver();
    timeout(
        Duration::from_secs(5),
        state.wait_for(|state| state.is_connected()),
    )
    .await
    .expect("the first connection should come up")
    .unwrap();

    let subscriptions = SubscriptionManager::new(connection.clone(), interest);
    let raw = subscriptions
        .subscribe_market_raw_events(vec![U256::from(1_u8)])
        .unwrap();
    let mut raw = Box::pin(raw);
    let first = next_raw(&mut raw).await;
    assert!(
        connected(&first, 1),
        "the raw stream started with {first:?}"
    );
}

/// The raw stream is subscribed before the first handshake completes, as it usually is: it
/// announces the first connection once, before its subscribe goes out.
#[tokio::test]
async fn a_raw_stream_subscribed_before_the_first_connect_announces_it_once() {
    let server = Server::start(false).await;
    let client = Client::new(&server.endpoint(), Config::default()).unwrap();
    let raw = client
        .subscribe_market_raw_events(vec![U256::from(1_u8)])
        .unwrap();
    let mut raw = Box::pin(raw);
    server.open();

    let mut announced = 0;
    loop {
        let event = next_raw(&mut raw).await;
        if connected(&event, 1) {
            announced += 1;
        } else if matches!(event, MarketRawStreamEvent::Outbound { .. }) {
            break;
        }
    }
    assert_eq!(
        announced, 1,
        "the first connection was announced {announced} times"
    );
}

/// The raw stream lags past the first connection's `Connected` and a frame, and what it still holds
/// starts with that frame's parser diagnostic: it announces the connection before the diagnostic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lagged_raw_stream_announces_a_connection_before_its_diagnostic() {
    // The raw tap holds 1024 events. The `Connected`, the subscribe, then 520 malformed frames
    // (each an inbound frame and its diagnostic), then a binary frame (no diagnostic): 1043
    // events, so the unread stream lost the first 19 and holds the 9th frame's diagnostic first.
    const CAPACITY: usize = 1024;
    const MALFORMED: usize = 520;
    let mut server = Server::start(false).await;
    let mut config = Config::default();
    config.heartbeat_interval = Duration::from_secs(60);
    let interest = Arc::new(InterestTracker::default());
    let connection: Connection =
        ConnectionManager::new(server.endpoint(), config, Arc::clone(&interest)).unwrap();
    // Both see every event from before the handshake: `all` as they come, `raw` only at the end.
    let mut all = connection.subscribe_raw_events();
    let subscriptions = SubscriptionManager::new(connection.clone(), interest);
    let raw = subscriptions
        .subscribe_market_raw_events(vec![U256::from(1_u8)])
        .unwrap();
    let mut raw = Box::pin(raw);
    server.open();
    assert_eq!(server.request().await.0, 1);

    let mut events = Vec::new();
    let mut take = async |until: usize, events: &mut Vec<RawConnectionEvent>| {
        while events.len() < until {
            let event = timeout(Duration::from_secs(5), all.recv()).await;
            events.push(event.expect("an event").expect("no lag"));
        }
    };
    // In batches, so `all` never lags.
    for _ in 0..4 {
        for _ in 0..MALFORMED / 4 {
            server.send(Message::Text("not json".into()));
        }
        let until = events.len().max(2) + MALFORMED / 2;
        take(until, &mut events).await;
    }
    server.send(Message::Binary(vec![0].into()));
    take(2 + 2 * MALFORMED + 1, &mut events).await;
    assert!(matches!(
        events.last(),
        Some(RawConnectionEvent::Inbound(frame)) if frame.protocol == RawFrameProtocol::Binary
    ));
    let lost = events.len() - CAPACITY;
    assert!(
        matches!(
            &events[lost],
            RawConnectionEvent::Diagnostic(diagnostic)
                if matches!(diagnostic.kind, ConnectionDiagnosticKind::ParserFailure(_))
        ),
        "the setup didn't leave a diagnostic first: {:?}",
        events[lost]
    );

    let lagged = next_raw(&mut raw).await;
    assert!(
        matches!(
            lagged,
            MarketRawStreamEvent::Continuity {
                reason: MarketStreamContinuity::Lagged { missed },
                ..
            } if missed == lost as u64
        ),
        "{lagged:?}"
    );
    let announced = next_raw(&mut raw).await;
    assert!(
        connected(&announced, 1),
        "the diagnostic came before the connection's announcement: {announced:?}"
    );
    let diagnostic = next_raw(&mut raw).await;
    assert!(
        matches!(
            diagnostic,
            MarketRawStreamEvent::Continuity {
                reason: MarketStreamContinuity::ParserDiagnostic(_),
                ..
            }
        ),
        "{diagnostic:?}"
    );
}
