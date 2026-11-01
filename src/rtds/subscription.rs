#![expect(
    clippy::module_name_repetitions,
    reason = "Subscription types deliberately include the module name for clarity"
)]

use std::sync::{Arc, PoisonError, RwLock};
use std::time::Instant;

use async_stream::try_stream;
use dashmap::{DashMap, Entry};
use futures::Stream;
use tokio::sync::broadcast::error::RecvError;

use super::error::RtdsError;
use super::types::request::{Subscription, SubscriptionRequest};
use super::types::response::{RtdsMessage, parse_messages};
use crate::Result;
use crate::auth::Credentials;
use crate::ws::ConnectionManager;
use crate::ws::connection::{ConnectionGeneration, ConnectionState};

#[non_exhaustive]
#[derive(Clone)]
pub struct SimpleParser;

impl crate::ws::traits::MessageParser<RtdsMessage> for SimpleParser {
    fn parse(&self, bytes: &[u8]) -> Result<Vec<RtdsMessage>> {
        parse_messages(bytes)
    }
}

/// Unique identifier for a topic/type subscription combination.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicType {
    /// Topic name (e.g., `crypto_prices`, `comments`)
    pub topic: String,
    /// Message type (e.g., `update`, `comment_created`, `*`)
    pub msg_type: String,
}

impl TopicType {
    /// Create a new topic/type identifier.
    #[must_use]
    pub fn new(topic: String, msg_type: String) -> Self {
        Self { topic, msg_type }
    }
}

/// Information about an active subscription.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    /// Topic and message type this subscription targets
    pub topic_type: TopicType,
    /// Optional filters for this subscription
    pub filters: Option<String>,
    /// CLOB authentication if required
    pub clob_auth: Option<Credentials>,
    /// When the subscription was created
    pub created_at: Instant,
}

/// Manages active subscriptions and routes messages to subscribers.
pub struct SubscriptionManager {
    connection: ConnectionManager<RtdsMessage, SimpleParser>,
    active_subs: DashMap<String, SubscriptionInfo>,
    /// Subscribed topics with reference counts (for multiplexing)
    subscribed_topics: DashMap<TopicType, usize>,
    last_auth: RwLock<Option<Credentials>>,
}

impl SubscriptionManager {
    /// Create a new subscription manager.
    #[must_use]
    pub fn new(connection: ConnectionManager<RtdsMessage, SimpleParser>) -> Self {
        Self {
            connection,
            active_subs: DashMap::new(),
            subscribed_topics: DashMap::new(),
            last_auth: RwLock::new(None),
        }
    }

    /// Start the reconnection handler that re-subscribes on connection recovery.
    ///
    /// The connection's generation decides, not the state the task first reads, so a task that
    /// first runs late neither replays the first connection's subscriptions nor misses a reconnect.
    pub fn start_reconnection_handler(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let mut state_rx = this.connection.state_receiver();

        tokio::spawn(async move {
            // The newest generation that has every subscription: the first sends the requests
            // queued before it.
            let mut subscribed = ConnectionGeneration(1);

            loop {
                // Wait for next state change
                if state_rx.changed().await.is_err() {
                    // Channel closed, connection manager is gone
                    break;
                }

                let state = *state_rx.borrow_and_update();

                match state {
                    ConnectionState::Connected { .. } => {
                        let generation = this.connection.generation();
                        if generation > subscribed {
                            // Reconnect to subscriptions
                            #[cfg(feature = "tracing")]
                            tracing::debug!("RTDS reconnected, re-establishing subscriptions");
                            this.resubscribe_all();
                            subscribed = generation;
                        }
                    }
                    ConnectionState::Disconnected => {
                        // Connection permanently closed
                        break;
                    }
                    _ => {
                        // Other states are no-op
                    }
                }
            }
        });
    }

    /// Re-send subscription requests for all tracked topics.
    fn resubscribe_all(&self) {
        // Get stored auth for re-subscription on reconnect.
        // We can recover from poisoned lock because Option<Credentials> has no inconsistent intermediate state.
        let auth = self
            .last_auth
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();

        let subscriptions: Vec<Subscription> = self
            .active_subs
            .iter()
            .map(|entry| {
                let info = entry.value();
                let mut sub = Subscription {
                    topic: info.topic_type.topic.clone(),
                    msg_type: info.topic_type.msg_type.clone(),
                    filters: info.filters.clone(),
                    clob_auth: None,
                };
                // Apply stored auth if subscription originally had auth
                if info.clob_auth.is_some()
                    && let Some(creds) = &auth
                {
                    sub = sub.with_clob_auth(creds.clone());
                }
                sub
            })
            .collect();

        if subscriptions.is_empty() {
            return;
        }

        #[cfg(feature = "tracing")]
        tracing::debug!(count = subscriptions.len(), "Re-subscribing to RTDS topics");

        let request = SubscriptionRequest::subscribe(subscriptions);
        if let Err(e) = self.connection.send(&request) {
            #[cfg(feature = "tracing")]
            tracing::warn!(%e, "Failed to re-subscribe to RTDS topics");
            #[cfg(not(feature = "tracing"))]
            let _: &crate::error::Error = &e;
        }
    }

    /// Subscribe to a topic with the given configuration.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Subscription is consumed to build SubscriptionInfo"
    )]
    pub fn subscribe(
        &self,
        subscription: Subscription,
    ) -> Result<impl Stream<Item = Result<RtdsMessage>>> {
        let topic_type = TopicType::new(subscription.topic.clone(), subscription.msg_type.clone());

        // Store auth for re-subscription on reconnect.
        // We can recover from poisoned lock because Option<Credentials> has no inconsistent intermediate state.
        if let Some(auth) = &subscription.clob_auth {
            *self
                .last_auth
                .write()
                .unwrap_or_else(PoisonError::into_inner) = Some(auth.clone());
        }

        // Increment refcount or insert new topic with refcount=1
        // Using Entry API to atomically check and update, with send inside the guard
        // to prevent TOCTOU race between refcount check and network send
        match self.subscribed_topics.entry(topic_type.clone()) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() += 1;
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    topic = %subscription.topic,
                    msg_type = %subscription.msg_type,
                    "RTDS topic already subscribed, multiplexing"
                );
            }
            Entry::Vacant(entry) => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    topic = %subscription.topic,
                    msg_type = %subscription.msg_type,
                    "Subscribing to RTDS topic"
                );

                // Send subscribe request while holding the entry lock to prevent
                // a concurrent unsubscribe from racing with us
                let request = SubscriptionRequest::subscribe(vec![subscription.clone()]);
                self.connection.send(&request)?;
                // Only insert after successful send
                entry.insert(1);
            }
        }

        // Register subscription info
        let sub_id = format!("{}:{}", topic_type.topic, topic_type.msg_type);
        self.active_subs.insert(
            sub_id,
            SubscriptionInfo {
                topic_type: topic_type.clone(),
                filters: subscription.filters.clone(),
                clob_auth: subscription.clob_auth.clone(),
                created_at: Instant::now(),
            },
        );

        // Create filtered stream with its own receiver
        let mut rx = self.connection.subscribe();
        let target_topic = topic_type.topic;
        let target_type = topic_type.msg_type;

        Ok(try_stream! {
            loop {
                match rx.recv().await {
                    Ok(envelope) => {
                        let msg = envelope.message;
                        // Filter messages by topic and type
                        let matches_topic = msg.topic == target_topic;
                        let matches_type = target_type == "*" || msg.msg_type == target_type;

                        if matches_topic && matches_type {
                            yield msg;
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        #[cfg(not(feature = "tracing"))]
                        let _ = n;
                        #[cfg(feature = "tracing")]
                        tracing::warn!("RTDS subscription lagged, missed {n} messages — continuing");
                    }
                    Err(RecvError::Closed) => {
                        break;
                    }
                }
            }
        })
    }

    /// Get information about all active subscriptions.
    #[must_use]
    pub fn active_subscriptions(&self) -> Vec<SubscriptionInfo> {
        self.active_subs
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get the number of active subscriptions.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.active_subs.len()
    }

    /// Unsubscribe from topics.
    ///
    /// This decrements the reference count for each topic. Only sends an unsubscribe
    /// request to the server when the reference count reaches zero (no other streams
    /// are using that topic).
    pub fn unsubscribe(&self, topic_types: &[TopicType]) -> Result<()> {
        if topic_types.is_empty() {
            return Err(RtdsError::SubscriptionFailed(
                "topic_types cannot be empty: at least one topic must be provided for unsubscription"
                    .to_owned(),
            )
            .into());
        }

        // Atomically decrement refcounts and send unsubscribe while holding the entry lock
        // to prevent TOCTOU race between refcount check and network send
        for topic_type in topic_types {
            if let Entry::Occupied(mut entry) = self.subscribed_topics.entry(topic_type.clone()) {
                let refcount = entry.get_mut();
                *refcount = refcount.saturating_sub(1);
                if *refcount == 0 {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        topic = %topic_type.topic,
                        msg_type = %topic_type.msg_type,
                        "Unsubscribing from RTDS topic"
                    );

                    // Send unsubscribe while holding the entry lock to prevent
                    // a concurrent subscribe from racing with us
                    let request = SubscriptionRequest::unsubscribe(vec![Subscription {
                        topic: topic_type.topic.clone(),
                        msg_type: topic_type.msg_type.clone(),
                        filters: None,
                        clob_auth: None,
                    }]);
                    self.connection.send(&request)?;
                    entry.remove();
                }
            }
        }

        // Remove active_subs entries where all topics are now unsubscribed
        self.active_subs
            .retain(|_, info| self.subscribed_topics.contains_key(&info.topic_type));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use futures::StreamExt as _;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::runtime::Builder;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;
    use crate::ws::config::Config;
    use crate::ws::test_venue::{answering, first, within};

    /// An RTDS double: every text frame a client sends but its heartbeat, with the number of the
    /// connection it came on; it closes the first connection after its first request.
    async fn serve() -> (String, mpsc::UnboundedReceiver<(usize, String)>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (sent, requests) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut number = 0;
            while let Ok((stream, _)) = listener.accept().await {
                let Ok(socket) = tokio_tungstenite::accept_async(stream).await else {
                    continue;
                };
                number += 1;
                let sent = sent.clone();
                tokio::spawn(async move {
                    let (_write, mut read) = socket.split();
                    while let Some(Ok(message)) = read.next().await {
                        if let Message::Text(text) = message
                            && text != "PING"
                        {
                            drop(sent.send((number, text.to_string())));
                            if number == 1 {
                                break;
                            }
                        }
                    }
                });
            }
        });
        (endpoint, requests)
    }

    fn subscribe(subscription: Subscription) -> String {
        serde_json::to_string(&SubscriptionRequest::subscribe(vec![subscription])).unwrap()
    }

    /// The handler's task first runs once a reconnect has brought up the second connection. That
    /// connection still gets the subscriptions.
    #[test]
    fn a_handler_that_first_runs_after_a_reconnect_resubscribes() {
        // The connection runs on its own runtime; the handler's, a current-thread one, runs
        // nothing until the test drives it.
        let connection_runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let handler_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let (endpoint, mut requests) = connection_runtime.block_on(serve());
        let mut config = Config::default();
        config.reconnect.initial_backoff = Duration::from_millis(50);
        config.reconnect.max_backoff = Duration::from_millis(50);
        let connection = {
            let _entered = connection_runtime.enter();
            ConnectionManager::new(endpoint, config, SimpleParser).unwrap()
        };
        let subscriptions = Arc::new(SubscriptionManager::new(connection.clone()));
        {
            let _entered = handler_runtime.enter();
            subscriptions.start_reconnection_handler();
        }
        let prices = Subscription::crypto_prices(None);
        let _prices = subscriptions.subscribe(prices.clone()).unwrap();

        // The first connection sends the subscribe and is closed; the second comes up.
        let next = |requests: &mut mpsc::UnboundedReceiver<(usize, String)>| {
            connection_runtime.block_on(async {
                timeout(Duration::from_secs(5), requests.recv())
                    .await
                    .expect("the server should receive a request")
                    .expect("the server should still run")
            })
        };
        assert_eq!(next(&mut requests), (1, subscribe(prices.clone())));
        connection_runtime.block_on(async {
            let mut generation = connection.generation_receiver();
            let mut state = connection.state_receiver();
            timeout(Duration::from_secs(5), async {
                generation
                    .wait_for(|generation| generation.0 == 2)
                    .await
                    .unwrap();
                state.wait_for(|state| state.is_connected()).await.unwrap();
            })
            .await
            .expect("the second connection should come up");
        });

        // The handler's task runs for the first time, then another topic is subscribed.
        handler_runtime.block_on(async {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
        });
        let comments = Subscription::comments(None);
        let _comments = subscriptions.subscribe(comments).unwrap();
        assert_eq!(
            next(&mut requests),
            (2, subscribe(prices)),
            "the second connection didn't get the subscriptions again first"
        );
    }

    /// The topic's first message, sent while its subscribe is between its request and its
    /// receiver, is the stream's first message. The subscribe is held after its request by its
    /// place among the active subscriptions, until the answer has reached the connection's
    /// receivers; then the venue sends its marker.
    #[test]
    fn a_stream_gets_the_answer_to_its_subscribe() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let price = |timestamp: i64| {
            json!({
                "topic": "crypto_prices",
                "type": "update",
                "timestamp": timestamp,
                "payload": { "symbol": "btcusdt", "timestamp": timestamp, "value": 1 },
            })
            .to_string()
        };
        let (endpoint, marker) = runtime.block_on(answering(price(1), price(2)));
        let connection = {
            let _entered = runtime.enter();
            ConnectionManager::new(endpoint, Config::default(), SimpleParser).unwrap()
        };
        let mut state = connection.state_receiver();
        within(&runtime, "the connection should come up", async {
            state
                .wait_for(|state| state.is_connected())
                .await
                .map(drop)
                .unwrap();
        });
        let subscriptions = Arc::new(SubscriptionManager::new(connection.clone()));

        let mut answers = connection.subscribe();
        let held = subscriptions
            .active_subs
            .entry("crypto_prices:update".to_owned());
        let (subscribed, is_subscribed) = std::sync::mpsc::channel();
        let subscriber = {
            let subscriptions = Arc::clone(&subscriptions);
            thread::spawn(move || {
                let stream = subscriptions
                    .subscribe(Subscription::crypto_prices(None))
                    .unwrap();
                subscribed.send(()).unwrap();
                first(stream)
            })
        };
        within(
            &runtime,
            "the venue should answer the subscribe",
            answers.recv(),
        )
        .unwrap();
        drop(held);
        is_subscribed
            .recv_timeout(Duration::from_secs(5))
            .expect("the subscribe should return");
        marker.send(()).unwrap();
        let message = subscriber.join().unwrap();
        assert_eq!(
            message.timestamp, 1,
            "the stream missed the answer to its subscribe"
        );
    }
}
