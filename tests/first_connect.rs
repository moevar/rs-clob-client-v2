//! The market channel's first connection, in the order each of its races needs (PumpAmp V3 bug
//! 0010): the reconnect handler's task first runs after the first `Connected`, and the raw stream
//! is subscribed after it.
#![cfg(all(feature = "clob", feature = "ws"))]
#![allow(
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    reason = "Do not need additional syntax for setting up tests"
)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use polymarket_client_sdk_v2::clob::ws::interest::InterestTracker;
use polymarket_client_sdk_v2::clob::ws::subscription::SubscriptionManager;
use polymarket_client_sdk_v2::clob::ws::{
    MarketRawStreamEvent, MarketStreamContinuity, SubscriptionRequest, WsMessage,
};
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::ConnectionManager;
use polymarket_client_sdk_v2::ws::config::Config;
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::sync::{Notify, mpsc};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

type Connection = ConnectionManager<WsMessage, Arc<InterestTracker>>;

/// A market channel double that completes no handshake until it is opened, and passes on every
/// text frame a client sends but its heartbeat, in order.
struct Server {
    addr: SocketAddr,
    gate: Arc<Notify>,
    requests: mpsc::UnboundedReceiver<String>,
}

impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let gate = Arc::new(Notify::new());
        let (sent, requests) = mpsc::unbounded_channel();
        tokio::spawn({
            let gate = Arc::clone(&gate);
            async move {
                gate.notified().await;
                while let Ok((stream, _)) = listener.accept().await {
                    let Ok(socket) = tokio_tungstenite::accept_async(stream).await else {
                        continue;
                    };
                    let sent = sent.clone();
                    tokio::spawn(async move {
                        let (_write, mut read) = socket.split();
                        while let Some(Ok(message)) = read.next().await {
                            if let Message::Text(text) = message
                                && text != "PING"
                            {
                                drop(sent.send(text.to_string()));
                            }
                        }
                    });
                }
            }
        });
        Self {
            addr,
            gate,
            requests,
        }
    }

    fn endpoint(&self) -> String {
        format!("ws://{}/ws/market", self.addr)
    }

    /// Lets the handshakes complete.
    fn open(&self) {
        self.gate.notify_one();
    }

    async fn request(&mut self) -> String {
        timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .expect("the server should receive a request")
            .expect("the server should still run")
    }
}

fn subscribe(asset: u8) -> String {
    serde_json::to_string(&SubscriptionRequest::market(vec![U256::from(asset)])).unwrap()
}

/// The handler's task first runs once the first connection is up. That connection sends the
/// subscribe queued before it, and nothing sends it again.
#[test]
fn a_handler_that_first_runs_after_the_first_connect_sends_no_subscribe_again() {
    // The connection runs on its own runtime; the handler's, a current-thread one, runs nothing
    // until the test drives it.
    let connection_runtime = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let handler_runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let mut server = connection_runtime.block_on(Server::start());
    let interest = Arc::new(InterestTracker::default());
    let connection: Connection = {
        let _entered = connection_runtime.enter();
        ConnectionManager::new(server.endpoint(), Config::default(), Arc::clone(&interest))
            .unwrap()
    };
    let subscriptions = Arc::new(SubscriptionManager::new(connection.clone(), interest));
    let _first = subscriptions.subscribe_market(vec![U256::from(1_u8)]).unwrap();
    {
        let _entered = handler_runtime.enter();
        subscriptions.start_reconnection_handler();
    }

    // The handler has its state receiver; the first connection comes up and sends the queued
    // subscribe.
    server.open();
    let first = connection_runtime.block_on(server.request());
    assert_eq!(first, subscribe(1));
    assert!(connection.state().is_connected());

    // The handler's task runs for the first time, then the next subscribe is sent.
    handler_runtime.block_on(async {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    });
    let _second = subscriptions.subscribe_market(vec![U256::from(2_u8)]).unwrap();
    let next = connection_runtime.block_on(server.request());
    assert_eq!(
        next,
        subscribe(2),
        "the handler sent the first connection's subscriptions again"
    );
}

/// The raw stream is subscribed once the first connection is up: it still starts with that
/// connection's `Connected`.
#[tokio::test]
async fn a_raw_stream_subscribed_after_the_first_connect_starts_with_it() {
    let server = Server::start().await;
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
    let first = timeout(Duration::from_secs(5), raw.next())
        .await
        .expect("the raw stream should yield")
        .expect("the raw stream should not end")
        .unwrap();
    assert!(
        matches!(
            first,
            MarketRawStreamEvent::Continuity {
                generation,
                reason: MarketStreamContinuity::Connected,
            } if generation.as_u64() == 1
        ),
        "the raw stream started with {first:?}"
    );
}
