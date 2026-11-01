#![expect(
    clippy::module_name_repetitions,
    reason = "Subscription types deliberately include the module name for clarity"
)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Instant;

use async_stream::try_stream;
use dashmap::{DashMap, Entry};
use futures::Stream;
use tokio::sync::broadcast::error::RecvError;

use super::interest::{InterestTracker, MessageInterest};
use super::types::request::SubscriptionRequest;
use super::types::response::WsMessage;
use super::types::stream::{
    MarketRawStreamEvent, MarketStreamContinuity, MarketStreamEvent, MarketStreamTerminal,
};
use crate::Result;
use crate::auth::Credentials;
use crate::types::{B256, U256};
use crate::ws::ConnectionManager;
use crate::ws::connection::ConnectionState;
use crate::ws::{
    ConnectionDiagnosticKind, ConnectionEvent, ConnectionGeneration, RawConnectionEvent, WsError,
};

/// What a subscription is targeting.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum SubscriptionTarget {
    /// Subscribed to market data for specific assets.
    Assets(Vec<U256>),
    /// Subscribed to user events for specific markets.
    Markets(Vec<B256>),
}

impl SubscriptionTarget {
    /// Returns the channel type this target belongs to.
    #[must_use]
    pub const fn channel(&self) -> ChannelType {
        match self {
            Self::Assets(_) => ChannelType::Market,
            Self::Markets(_) => ChannelType::User,
        }
    }
}

fn message_matches_assets(msg: &WsMessage, asset_ids: &HashSet<U256>) -> bool {
    match msg {
        WsMessage::Book(book) => asset_ids.contains(&book.asset_id),
        WsMessage::PriceChange(price) => price
            .price_changes
            .iter()
            .any(|pc| asset_ids.contains(&pc.asset_id)),
        WsMessage::LastTradePrice(ltp) => asset_ids.contains(&ltp.asset_id),
        WsMessage::TickSizeChange(tsc) => asset_ids.contains(&tsc.asset_id),
        WsMessage::BestBidAsk(bba) => asset_ids.contains(&bba.asset_id),
        WsMessage::NewMarket(nm) => nm.asset_ids.iter().any(|id| asset_ids.contains(id)),
        WsMessage::MarketResolved(mr) => mr.asset_ids.iter().any(|id| asset_ids.contains(id)),
        _ => false,
    }
}

/// Information about an active subscription.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    /// What this subscription is targeting.
    pub target: SubscriptionTarget,
    /// When the subscription was created.
    pub created_at: Instant,
}

impl SubscriptionInfo {
    /// Returns the channel type for this subscription.
    #[must_use]
    pub const fn channel(&self) -> ChannelType {
        self.target.channel()
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelType {
    /// Public market data channel
    Market,
    /// Authenticated user data channel
    User,
}

/// The `Connected` continuity of `generation`, unless the stream announced it or a later one, or
/// no connection was ever established (zero).
fn announce(
    announced: &mut ConnectionGeneration,
    generation: ConnectionGeneration,
) -> Option<MarketRawStreamEvent> {
    (generation > *announced).then(|| {
        *announced = generation;
        MarketRawStreamEvent::Continuity {
            generation,
            reason: MarketStreamContinuity::Connected,
        }
    })
}

/// Manages active subscriptions and routes messages to subscribers.
pub struct SubscriptionManager {
    connection: ConnectionManager<WsMessage, Arc<InterestTracker>>,
    active_subs: DashMap<String, SubscriptionInfo>,
    interest: Arc<InterestTracker>,
    /// Subscribed assets with reference counts (for multiplexing)
    subscribed_assets: DashMap<U256, usize>,
    /// Subscribed markets with reference counts (for multiplexing)
    subscribed_markets: DashMap<B256, usize>,
    last_auth: Arc<RwLock<Option<Credentials>>>,
    /// Track if custom features were enabled for any market subscription
    /// (enables `best_bid_ask`, `new_market`, `market_resolved` messages)
    custom_features_enabled: AtomicBool,
}

impl SubscriptionManager {
    /// Create a new subscription manager.
    #[must_use]
    pub fn new(
        connection: ConnectionManager<WsMessage, Arc<InterestTracker>>,
        interest: Arc<InterestTracker>,
    ) -> Self {
        Self {
            connection,
            active_subs: DashMap::new(),
            interest,
            subscribed_assets: DashMap::new(),
            subscribed_markets: DashMap::new(),
            last_auth: Arc::new(RwLock::new(None)),
            custom_features_enabled: AtomicBool::new(false),
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
                            tracing::debug!("WebSocket reconnected, re-establishing subscriptions");
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

    /// Re-send subscription requests for all tracked assets and markets.
    fn resubscribe_all(&self) {
        // Collect all subscribed assets
        let assets: Vec<U256> = self.subscribed_assets.iter().map(|r| *r.key()).collect();

        if !assets.is_empty() {
            let custom_features = self.custom_features_enabled.load(Ordering::Relaxed);
            #[cfg(feature = "tracing")]
            tracing::debug!(
                count = assets.len(),
                custom_features,
                "Re-subscribing to market assets"
            );
            let mut request = SubscriptionRequest::market(assets);
            if custom_features {
                request = request.with_custom_features(true);
            }
            if let Err(e) = self.connection.send(&request) {
                #[cfg(feature = "tracing")]
                tracing::warn!(%e, "Failed to re-subscribe to market channel");
                #[cfg(not(feature = "tracing"))]
                let _error: crate::Error = e;
            }
        }

        // Store auth for re-subscription on reconnect.
        // We can recover from poisoned lock because Option<Credentials> has no inconsistent intermediate state.
        let auth = self
            .last_auth
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(auth) = auth {
            let markets: Vec<B256> = self.subscribed_markets.iter().map(|r| *r.key()).collect();

            #[cfg(feature = "tracing")]
            tracing::debug!(
                markets_count = markets.len(),
                "Re-subscribing to user channel"
            );
            let request = SubscriptionRequest::user(markets);
            if let Err(e) = self.connection.send_authenticated(&request, &auth) {
                #[cfg(feature = "tracing")]
                tracing::warn!(%e, "Failed to re-subscribe to user channel");
                #[cfg(not(feature = "tracing"))]
                let _error: crate::Error = e;
            }
        }
    }

    /// Subscribe to public market data channel.
    ///
    /// This will fail if `asset_ids` is empty.
    pub fn subscribe_market(
        &self,
        asset_ids: Vec<U256>,
    ) -> Result<impl Stream<Item = Result<WsMessage>> + use<>> {
        self.subscribe_market_with_options(asset_ids, false)
    }

    fn register_market_subscription(
        &self,
        asset_ids: Vec<U256>,
        custom_features: bool,
    ) -> Result<HashSet<U256>> {
        if self.connection.is_shutdown() {
            return Err(WsError::ConnectionClosed.into());
        }

        if asset_ids.is_empty() {
            return Err(WsError::SubscriptionFailed(
                "asset_ids cannot be empty: at least one asset ID must be provided for subscription"
                    .to_owned(),
            )
            .into());
        }

        self.interest.add(MessageInterest::MARKET);

        // Track if custom features are enabled (for re-subscription on reconnect)
        if custom_features {
            self.custom_features_enabled.store(true, Ordering::Relaxed);
        }

        // Increment refcounts and determine which assets are truly new
        let new_assets: Vec<U256> = asset_ids
            .iter()
            .filter_map(|id| match self.subscribed_assets.entry(*id) {
                Entry::Occupied(mut o) => {
                    *o.get_mut() += 1;
                    None
                }
                Entry::Vacant(v) => {
                    v.insert(1);
                    Some(id.to_owned())
                }
            })
            .collect();

        // Only send subscription request for new assets
        if new_assets.is_empty() {
            #[cfg(feature = "tracing")]
            tracing::debug!("All requested assets already subscribed, multiplexing");
        } else {
            #[cfg(feature = "tracing")]
            tracing::debug!(
                count = new_assets.len(),
                ?new_assets,
                custom_features,
                "Subscribing to new market assets"
            );
            let mut request = SubscriptionRequest::market(new_assets);
            if custom_features {
                request = request.with_custom_features(true);
            }
            self.connection.send(&request)?;
        }

        // Register subscription
        let sub_id = format!(
            "market:{}",
            asset_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        );
        self.active_subs.insert(
            sub_id,
            SubscriptionInfo {
                target: SubscriptionTarget::Assets(asset_ids.clone()),
                created_at: Instant::now(),
            },
        );

        Ok(asset_ids.into_iter().collect())
    }

    /// Subscribe to public market data channel with options.
    ///
    /// When `custom_features` is true, enables receiving additional message types:
    /// `best_bid_ask`, `new_market`, `market_resolved`.
    ///
    /// This will fail if `asset_ids` is empty.
    pub fn subscribe_market_with_options(
        &self,
        asset_ids: Vec<U256>,
        custom_features: bool,
    ) -> Result<impl Stream<Item = Result<WsMessage>> + use<>> {
        let asset_ids_set = self.register_market_subscription(asset_ids, custom_features)?;

        let mut rx = self.connection.subscribe();

        Ok(try_stream! {
            loop {
                match rx.recv().await {
                    Ok(envelope) => {
                        let msg = envelope.message;
                        // Filter messages by asset_id
                        if message_matches_assets(&msg, &asset_ids_set) {
                            yield msg
                        }
                    }
                    #[cfg(feature = "tracing")]
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!("Subscription lagged, missed {n} messages — continuing");
                    }
                    #[cfg(not(feature = "tracing"))]
                    Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => {
                        break;
                    }
                }
            }
        })
    }

    /// Subscribe to a single ordered market stream with data and continuity events.
    pub fn subscribe_market_events(
        &self,
        asset_ids: Vec<U256>,
    ) -> Result<impl Stream<Item = Result<MarketStreamEvent>> + use<>> {
        let connection = self.connection.clone();
        let mut event_rx = connection.subscribe_events();
        let asset_ids_set = self.register_market_subscription(asset_ids, true)?;

        Ok(try_stream! {
            let mut current_generation: Option<ConnectionGeneration> = None;

            loop {
                match event_rx.recv().await {
                    Ok(ConnectionEvent::Message(envelope)) => {
                                let generation = envelope.generation;
                                if current_generation != Some(generation) {
                                    current_generation = Some(generation);
                                    yield MarketStreamEvent::Continuity {
                                        generation,
                                        reason: MarketStreamContinuity::Connected,
                                    };
                                }

                                if message_matches_assets(&envelope.message, &asset_ids_set) {
                                    yield MarketStreamEvent::Message {
                                        generation,
                                        message: envelope.message,
                                    };
                                }
                            }
                    Ok(ConnectionEvent::Diagnostic(diagnostic)) => {
                                match diagnostic.kind {
                                    ConnectionDiagnosticKind::Connected => {
                                        if current_generation != Some(diagnostic.generation) {
                                            current_generation = Some(diagnostic.generation);
                                            yield MarketStreamEvent::Continuity {
                                                generation: diagnostic.generation,
                                                reason: MarketStreamContinuity::Connected,
                                            };
                                        }
                                    }
                                    ConnectionDiagnosticKind::ReconnectExhausted { attempts } => {
                                        yield MarketStreamEvent::Terminal {
                                            generation: diagnostic.generation,
                                            reason: MarketStreamTerminal::ReconnectExhausted { attempts },
                                        };
                                        break;
                                    }
                                    ConnectionDiagnosticKind::Shutdown => {
                                        yield MarketStreamEvent::Terminal {
                                            generation: diagnostic.generation,
                                            reason: MarketStreamTerminal::Shutdown,
                                        };
                                        break;
                                    }
                                    kind => {
                                        if let Some(reason) =
                                            MarketStreamContinuity::from_connection_diagnostic(kind)
                                        {
                                            yield MarketStreamEvent::Continuity {
                                                generation: diagnostic.generation,
                                                reason,
                                            };
                                        }
                                    }
                                }
                            }
                    Err(RecvError::Lagged(n)) => {
                        yield MarketStreamEvent::Continuity {
                            generation: connection.generation(),
                            reason: MarketStreamContinuity::Lagged { missed: n },
                        };
                    }
                    Err(RecvError::Closed) => {
                        yield MarketStreamEvent::Terminal {
                            generation: connection.generation(),
                            reason: MarketStreamTerminal::ChannelClosed,
                        };
                        break;
                    }
                }
            }
        })
    }

    /// Subscribe to raw market-channel frames and lifecycle events on the existing connection.
    pub fn subscribe_market_raw_events(
        &self,
        asset_ids: Vec<U256>,
    ) -> Result<impl Stream<Item = Result<MarketRawStreamEvent>> + use<>> {
        let connection = self.connection.clone();
        let mut raw_rx = connection.subscribe_raw_events();
        self.register_market_subscription(asset_ids, true)?;

        Ok(try_stream! {
            // A receiver taken after a connection came up, or one that lagged past it, never gets
            // its `Connected`, so a generation's first frame announces it too, once.
            let mut announced = ConnectionGeneration::zero();

            loop {
                match raw_rx.recv().await {
                    Ok(RawConnectionEvent::Inbound(frame)) => {
                        if let Some(connected) = announce(&mut announced, frame.generation) {
                            yield connected;
                        }
                        yield MarketRawStreamEvent::Inbound {
                            generation: frame.generation,
                            protocol: frame.protocol,
                            bytes: frame.bytes,
                            observed_at: frame.observed_at,
                        };
                    }
                    Ok(RawConnectionEvent::Outbound(frame)) => {
                        if let Some(connected) = announce(&mut announced, frame.generation) {
                            yield connected;
                        }
                        yield MarketRawStreamEvent::Outbound {
                            generation: frame.generation,
                            protocol: frame.protocol,
                            bytes: frame.bytes,
                            observed_at: frame.observed_at,
                        };
                    }
                    Ok(RawConnectionEvent::Diagnostic(diagnostic)) => {
                        // A parser diagnostic comes from a frame of a live connection, so it
                        // announces its generation as a frame does. The others can come after
                        // their connection is gone, which an announcement would deny.
                        if matches!(diagnostic.kind, ConnectionDiagnosticKind::ParserFailure(_))
                            && let Some(connected) = announce(&mut announced, diagnostic.generation)
                        {
                            yield connected;
                        }
                        match diagnostic.kind {
                            ConnectionDiagnosticKind::Connected => {
                                if let Some(connected) =
                                    announce(&mut announced, diagnostic.generation)
                                {
                                    yield connected;
                                }
                            }
                            ConnectionDiagnosticKind::ReconnectExhausted { attempts } => {
                                yield MarketRawStreamEvent::Terminal {
                                    generation: diagnostic.generation,
                                    reason: MarketStreamTerminal::ReconnectExhausted { attempts },
                                };
                                break;
                            }
                            ConnectionDiagnosticKind::Shutdown => {
                                yield MarketRawStreamEvent::Terminal {
                                    generation: diagnostic.generation,
                                    reason: MarketStreamTerminal::Shutdown,
                                };
                                break;
                            }
                            kind => {
                                if let Some(reason) =
                                    MarketStreamContinuity::from_connection_diagnostic(kind)
                                {
                                    yield MarketRawStreamEvent::Continuity {
                                        generation: diagnostic.generation,
                                        reason,
                                    };
                                }
                            }
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        yield MarketRawStreamEvent::Continuity {
                            generation: connection.generation(),
                            reason: MarketStreamContinuity::Lagged { missed: n },
                        };
                    }
                    Err(RecvError::Closed) => {
                        yield MarketRawStreamEvent::Terminal {
                            generation: connection.generation(),
                            reason: MarketStreamTerminal::ChannelClosed,
                        };
                        break;
                    }
                }
            }
        })
    }

    /// Subscribe to authenticated user channel.
    pub fn subscribe_user(
        &self,
        markets: Vec<B256>,
        auth: &Credentials,
    ) -> Result<impl Stream<Item = Result<WsMessage>> + use<>> {
        if self.connection.is_shutdown() {
            return Err(WsError::ConnectionClosed.into());
        }

        self.interest.add(MessageInterest::USER);

        // Store auth for re-subscription on reconnect.
        // We can recover from poisoned lock because Option<Credentials> has no inconsistent intermediate state.
        *self
            .last_auth
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(auth.clone());

        // Increment refcounts and determine which markets are truly new
        let new_markets: Vec<B256> = markets
            .iter()
            .filter_map(|id| match self.subscribed_markets.entry(id.to_owned()) {
                Entry::Occupied(mut o) => {
                    *o.get_mut() += 1;
                    None
                }
                Entry::Vacant(v) => {
                    v.insert(1);
                    Some(id.to_owned())
                }
            })
            .collect();

        // Only send subscription request for new markets (or if subscribing to all)
        if !markets.is_empty() && new_markets.is_empty() {
            #[cfg(feature = "tracing")]
            tracing::debug!("All requested markets already subscribed, multiplexing");
        } else {
            #[cfg(feature = "tracing")]
            tracing::debug!(
                count = new_markets.len(),
                ?new_markets,
                "Subscribing to user channel"
            );
            let request = SubscriptionRequest::user(new_markets);
            self.connection.send_authenticated(&request, auth)?;
        }

        // Register subscription
        let sub_id = format!(
            "user:{}",
            markets
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        );
        self.active_subs.insert(
            sub_id,
            SubscriptionInfo {
                target: SubscriptionTarget::Markets(markets),
                created_at: Instant::now(),
            },
        );

        // Create stream for user messages
        let mut rx = self.connection.subscribe();

        Ok(try_stream! {
            loop {
                match rx.recv().await {
                    Ok(envelope) => {
                        let msg = envelope.message;
                        if msg.is_user() {
                            yield msg;
                        }
                    }
                    #[cfg(feature = "tracing")]
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!("Subscription lagged, missed {n} messages — continuing");
                    }
                    #[cfg(not(feature = "tracing"))]
                    Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => {
                        break;
                    }
                }
            }
        })
    }

    /// Get information about all active subscriptions.
    #[must_use]
    pub fn active_subscriptions(&self) -> HashMap<ChannelType, Vec<SubscriptionInfo>> {
        self.active_subs
            .iter()
            .fold(HashMap::new(), |mut acc, entry| {
                acc.entry(entry.value().channel())
                    .or_default()
                    .push(entry.value().clone());
                acc
            })
    }

    /// Get the number of active subscriptions.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.active_subs.len()
    }

    /// Check if there are any subscriptions for a specific channel type.
    #[must_use]
    pub fn has_subscriptions(&self, channel: ChannelType) -> bool {
        match channel {
            ChannelType::Market => !self.subscribed_assets.is_empty(),
            ChannelType::User => !self.subscribed_markets.is_empty(),
        }
    }

    /// Unsubscribe from market data for specific assets.
    ///
    /// This decrements the reference count for each asset. Only sends an unsubscribe
    /// request to the server when the reference count reaches zero (no other streams
    /// are using that asset).
    pub fn unsubscribe_market(&self, asset_ids: &[U256]) -> Result<()> {
        if asset_ids.is_empty() {
            return Err(WsError::SubscriptionFailed(
                "asset_ids cannot be empty: at least one asset ID must be provided for unsubscription"
                    .to_owned(),
            )
            .into());
        }

        let mut to_unsubscribe = Vec::new();

        // Atomically decrement refcounts and remove assets that reach zero
        // Using Entry API to prevent TOCTOU race between decrement and removal
        for id in asset_ids {
            if let Entry::Occupied(mut entry) = self.subscribed_assets.entry(*id) {
                let refcount = entry.get_mut();
                *refcount = refcount.saturating_sub(1);
                if *refcount == 0 {
                    entry.remove();
                    to_unsubscribe.push(*id);
                }
            }
        }

        // Send unsubscribe only for zero-refcount assets
        if !to_unsubscribe.is_empty() {
            #[cfg(feature = "tracing")]
            tracing::debug!(
                count = to_unsubscribe.len(),
                ?to_unsubscribe,
                "Unsubscribing from market assets"
            );
            let request = SubscriptionRequest::market_unsubscribe(to_unsubscribe);
            self.connection.send(&request)?;
        }

        // Remove active_subs entries where all assets are now unsubscribed
        self.active_subs.retain(|_, info| {
            if let SubscriptionTarget::Assets(assets) = &info.target {
                // Keep entry only if at least one asset is still subscribed
                assets
                    .iter()
                    .any(|a| self.subscribed_assets.contains_key(a))
            } else {
                true // Keep non-market subscriptions
            }
        });

        Ok(())
    }

    /// Unsubscribe from user events for specific markets.
    ///
    /// This decrements the reference count for each market. Only sends an unsubscribe
    /// request to the server when the reference count reaches zero (no other streams
    /// are using that market).
    pub fn unsubscribe_user(&self, markets: &[B256]) -> Result<()> {
        if markets.is_empty() {
            return Err(WsError::SubscriptionFailed(
                "markets cannot be empty: at least one market ID must be provided for unsubscription"
                    .to_owned(),
            )
            .into());
        }

        let mut to_unsubscribe = Vec::new();

        // Atomically decrement refcounts and remove markets that reach zero
        // Using Entry API to prevent TOCTOU race between decrement and removal
        for m in markets {
            if let Entry::Occupied(mut entry) = self.subscribed_markets.entry(*m) {
                let refcount = entry.get_mut();
                *refcount = refcount.saturating_sub(1);
                if *refcount == 0 {
                    entry.remove();
                    to_unsubscribe.push(*m);
                }
            }
        }

        // Send unsubscribe only for zero-refcount markets
        if !to_unsubscribe.is_empty() {
            #[cfg(feature = "tracing")]
            tracing::debug!(
                count = to_unsubscribe.len(),
                ?to_unsubscribe,
                "Unsubscribing from user markets"
            );

            // Get auth for unsubscribe request
            let auth = self
                .last_auth
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
                .ok_or(WsError::AuthenticationFailed)?;

            let request = SubscriptionRequest::user_unsubscribe(to_unsubscribe);
            self.connection.send_authenticated(&request, &auth)?;
        }

        // Remove active_subs entries where all markets are now unsubscribed
        self.active_subs.retain(|_, info| {
            if let SubscriptionTarget::Markets(markets) = &info.target {
                // Keep entry only if at least one market is still subscribed
                markets
                    .iter()
                    .any(|m| self.subscribed_markets.contains_key(m))
            } else {
                true // Keep non-user subscriptions
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;
    use std::thread;
    use std::time::Duration;

    use serde_json::json;
    use tokio::runtime::{Builder, Runtime};
    use uuid::Uuid;

    use super::*;
    use crate::ws::config::Config;
    use crate::ws::test_venue::{answering, first, within};

    const ASSET: &str =
        "65818619657568813474341868652308942079804919287380422192892211131408793125422";
    const MARKET: &str = "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af";

    fn book(hash: &str) -> String {
        json!({
            "event_type": "book",
            "asset_id": ASSET,
            "market": MARKET,
            "bids": [{ "price": ".48", "size": "30" }],
            "asks": [{ "price": ".52", "size": "25" }],
            "timestamp": "123456789000",
            "hash": hash,
        })
        .to_string()
    }

    fn order(id: &str) -> String {
        json!({
            "event_type": "order",
            "id": id,
            "asset_id": ASSET,
            "market": MARKET,
            "owner": "9180014b-33c8-9240-a14b-bdca11c0a465",
            "original_size": "10",
            "outcome": "YES",
            "price": "0.57",
            "side": "SELL",
            "size_matched": "0",
            "status": "LIVE",
            "timestamp": "1672290687",
            "type": "PLACEMENT",
        })
        .to_string()
    }

    /// A connection to `endpoint`, once it is up, and its subscriptions.
    fn connect(
        runtime: &Runtime,
        endpoint: String,
    ) -> (
        ConnectionManager<WsMessage, Arc<InterestTracker>>,
        Arc<SubscriptionManager>,
    ) {
        let interest = Arc::new(InterestTracker::new());
        let connection = {
            let _entered = runtime.enter();
            ConnectionManager::new(endpoint, Config::default(), Arc::clone(&interest)).unwrap()
        };
        let mut state = connection.state_receiver();
        within(runtime, "the connection should come up", async {
            state
                .wait_for(|state| state.is_connected())
                .await
                .map(drop)
                .unwrap();
        });
        let subscriptions = Arc::new(SubscriptionManager::new(connection.clone(), interest));
        (connection, subscriptions)
    }

    /// Subscribes with `subscribe` on a thread of its own while `sub_id`'s place among the active
    /// subscriptions is held, so the subscribe stops after its request until the venue's answer
    /// has reached the connection's receivers. Then the venue sends its marker. Returns the
    /// stream's first message.
    fn first_after_held_subscribe<S>(
        runtime: &Runtime,
        connection: &ConnectionManager<WsMessage, Arc<InterestTracker>>,
        subscriptions: &Arc<SubscriptionManager>,
        sub_id: String,
        marker: &tokio::sync::mpsc::UnboundedSender<()>,
        subscribe: impl FnOnce(&SubscriptionManager) -> S + Send + 'static,
    ) -> WsMessage
    where
        S: Stream<Item = Result<WsMessage>>,
    {
        let mut answers = connection.subscribe();
        let held = subscriptions.active_subs.entry(sub_id);
        let (subscribed, is_subscribed) = std::sync::mpsc::channel();
        let subscriber = {
            let subscriptions = Arc::clone(subscriptions);
            thread::spawn(move || {
                let stream = subscribe(&subscriptions);
                subscribed.send(()).unwrap();
                first(stream)
            })
        };
        within(
            runtime,
            "the venue should answer the subscribe",
            answers.recv(),
        )
        .unwrap();
        drop(held);
        is_subscribed
            .recv_timeout(Duration::from_secs(5))
            .expect("the subscribe should return");
        marker.send(()).unwrap();
        subscriber.join().unwrap()
    }

    fn runtime() -> Runtime {
        Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    /// The token's first book, sent while its subscribe is between its request and its receiver,
    /// is the market stream's first message.
    #[test]
    fn a_market_stream_gets_the_answer_to_its_subscribe() {
        let runtime = runtime();
        let (endpoint, marker) = runtime.block_on(answering(book("answer"), book("marker")));
        let (connection, subscriptions) = connect(&runtime, endpoint);
        let asset = U256::from_str(ASSET).unwrap();
        let message = first_after_held_subscribe(
            &runtime,
            &connection,
            &subscriptions,
            format!("market:{asset}"),
            &marker,
            move |subscriptions| {
                subscriptions
                    .subscribe_market_with_options(vec![asset], false)
                    .unwrap()
            },
        );
        let WsMessage::Book(book) = message else {
            panic!("expected a book, got {message:?}");
        };
        assert_eq!(
            book.hash.as_deref(),
            Some("answer"),
            "the market stream missed the answer to its subscribe"
        );
    }

    /// The venue's first user event, sent while the subscribe is between its request and its
    /// receiver, is the user stream's first message.
    #[test]
    fn a_user_stream_gets_the_answer_to_its_subscribe() {
        let runtime = runtime();
        let (endpoint, marker) = runtime.block_on(answering(order("answer"), order("marker")));
        let (connection, subscriptions) = connect(&runtime, endpoint);
        let market = B256::from_str(MARKET).unwrap();
        let credentials = Credentials::new(Uuid::nil(), "secret".to_owned(), "phrase".to_owned());
        let message = first_after_held_subscribe(
            &runtime,
            &connection,
            &subscriptions,
            format!("user:{market}"),
            &marker,
            move |subscriptions| {
                subscriptions
                    .subscribe_user(vec![market], &credentials)
                    .unwrap()
            },
        );
        let WsMessage::Order(order) = message else {
            panic!("expected an order, got {message:?}");
        };
        assert_eq!(
            order.id, "answer",
            "the user stream missed the answer to its subscribe"
        );
    }
}
