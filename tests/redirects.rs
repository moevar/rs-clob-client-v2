//! A request that changes state, answered with a redirect, reaches the venue once and goes nowhere
//! else (PumpAmp V3 bug 0011).
#![cfg(feature = "clob")]
#![allow(
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    reason = "Do not need additional syntax for setting up tests"
)]

mod common;

use std::str::FromStr as _;

use alloy::signers::Signer as _;
use alloy::signers::local::LocalSigner;
use httpmock::{Method, Mock, MockServer};
use polymarket_client_sdk_v2::clob::types::{SignableOrder, TickSize};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::error::Status;
use polymarket_client_sdk_v2::{POLYGON, Result};
use serde_json::json;

use crate::common::{
    API_KEY, PASSPHRASE, POLY_SIGNATURE, PRIVATE_KEY, SECRET, create_authenticated,
    ensure_requirements, token_1,
};

/// The redirects whose `Location` a client could follow.
const REDIRECTS: [u16; 4] = [301, 302, 307, 308];

/// `venue` answers `method path` with `status` and a `Location` on `target`, another origin.
struct Redirect<'a> {
    answered: Mock<'a>,
    /// The followed requests that carried the SDK's signed headers.
    signed: Mock<'a>,
    /// The followed requests without them.
    unsigned: Mock<'a>,
}

impl<'a> Redirect<'a> {
    fn new(
        venue: &'a MockServer,
        target: &'a MockServer,
        method: Method,
        path: &str,
        status: u16,
    ) -> Self {
        let answered = venue.mock(|when, then| {
            when.method(method).path(path);
            then.status(status).header("Location", target.url("/moved"));
        });
        // httpmock answers with the first mock that matches, so `signed` takes what it can.
        let signed = target.mock(|when, then| {
            when.path("/moved").header_exists(POLY_SIGNATURE);
            then.status(200).json_body(json!({}));
        });
        let unsigned = target.mock(|when, then| {
            when.path("/moved");
            then.status(200).json_body(json!({}));
        });
        Self {
            answered,
            signed,
            unsigned,
        }
    }

    /// What reached the target, if anything did; the venue must have been asked once.
    fn followed(&self, status: u16, what: &str, result: &dyn std::fmt::Debug) -> Option<String> {
        self.answered.assert_calls(1);
        let reached = self.signed.calls() + self.unsigned.calls();
        (reached > 0).then(|| {
            format!(
                "a {status} answer to {what} was followed: {reached} request(s) reached the \
                 redirect's target, {} with the signed headers; the call gave {result:?}",
                self.signed.calls()
            )
        })
    }
}

/// What is wrong with the call's result, if anything: a stopped redirect comes back as its status,
/// an unknown outcome for the caller.
fn not_stopped<T: std::fmt::Debug>(status: u16, what: &str, result: &Result<T>) -> Option<String> {
    let answer = result
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<Status>());
    match answer {
        Some(answer) if answer.status_code.as_u16() == status => None,
        _ => Some(format!(
            "a {status} answer to {what} didn't come back as it: {result:?}"
        )),
    }
}

/// Fails with every redirect that was followed.
fn assert_none_followed(followed: &[String]) {
    assert!(followed.is_empty(), "{}", followed.join("\n"));
}

#[tokio::test]
async fn an_order_answered_with_a_redirect_reaches_the_venue_once() -> anyhow::Result<()> {
    let signer = LocalSigner::from_str(PRIVATE_KEY)?.with_chain_id(Some(POLYGON));
    let mut followed = Vec::new();
    for status in REDIRECTS {
        let (venue, target) = (MockServer::start(), MockServer::start());
        let client = create_authenticated(&venue).await?;
        ensure_requirements(&venue, token_1(), TickSize::Hundredth);
        let redirect = Redirect::new(&venue, &target, Method::POST, "/order", status);
        let order = client.sign(&signer, SignableOrder::default()).await?;
        let result = client.post_order(order).await;
        followed.extend(redirect.followed(status, "POST /order", &result));
        followed.extend(not_stopped(status, "POST /order", &result));
    }
    assert_none_followed(&followed);
    Ok(())
}

#[tokio::test]
async fn a_cancel_answered_with_a_redirect_reaches_the_venue_once() -> anyhow::Result<()> {
    let mut followed = Vec::new();
    for status in REDIRECTS {
        let (venue, target) = (MockServer::start(), MockServer::start());
        let client = create_authenticated(&venue).await?;
        let redirect = Redirect::new(&venue, &target, Method::DELETE, "/order", status);
        let result = client.cancel_order("1").await;
        followed.extend(redirect.followed(status, "DELETE /order", &result));
        followed.extend(not_stopped(status, "DELETE /order", &result));
    }
    assert_none_followed(&followed);
    Ok(())
}

#[tokio::test]
async fn a_key_creation_answered_with_a_redirect_reaches_the_venue_once() -> anyhow::Result<()> {
    let signer = LocalSigner::from_str(PRIVATE_KEY)?.with_chain_id(Some(POLYGON));
    let mut followed = Vec::new();
    for status in REDIRECTS {
        let (venue, target) = (MockServer::start(), MockServer::start());
        let client = Client::new(&venue.base_url(), Config::default())?;
        let redirect = Redirect::new(&venue, &target, Method::POST, "/auth/api-key", status);
        let result = client.create_api_key(&signer, None).await;
        followed.extend(redirect.followed(status, "POST /auth/api-key", &result));
        followed.extend(not_stopped(status, "POST /auth/api-key", &result));
    }
    assert_none_followed(&followed);
    Ok(())
}

#[tokio::test]
async fn authenticating_after_a_redirected_key_creation_derives_at_the_venue() -> anyhow::Result<()>
{
    let signer = LocalSigner::from_str(PRIVATE_KEY)?.with_chain_id(Some(POLYGON));
    let mut followed = Vec::new();
    for status in REDIRECTS {
        let (venue, target) = (MockServer::start(), MockServer::start());
        let redirect = Redirect::new(&venue, &target, Method::POST, "/auth/api-key", status);
        let derived = venue.mock(|when, then| {
            when.method(Method::GET).path("/auth/derive-api-key");
            then.status(200).json_body(json!({
                "apiKey": API_KEY.to_string(),
                "passphrase": PASSPHRASE,
                "secret": SECRET
            }));
        });
        let result = Client::new(&venue.base_url(), Config::default())?
            .authentication_builder(&signer)
            .authenticate()
            .await
            .map(drop);
        followed.extend(redirect.followed(status, "POST /auth/api-key in authenticate", &result));
        // The create's stopped redirect is a status, which falls back to the venue's derive.
        derived.assert_calls(1);
        assert!(result.is_ok(), "{status}: {result:?}");
    }
    assert_none_followed(&followed);
    Ok(())
}

#[cfg(feature = "bridge")]
#[tokio::test]
async fn a_deposit_answered_with_a_redirect_reaches_the_bridge_once() -> anyhow::Result<()> {
    use polymarket_client_sdk_v2::bridge::Client as Bridge;
    use polymarket_client_sdk_v2::bridge::types::DepositRequest;
    use polymarket_client_sdk_v2::types::address;

    let mut followed = Vec::new();
    for status in REDIRECTS {
        let (venue, target) = (MockServer::start(), MockServer::start());
        let client = Bridge::new(&venue.base_url())?;
        let redirect = Redirect::new(&venue, &target, Method::POST, "/deposit", status);
        let request = DepositRequest::builder()
            .address(address!("56687bf447db6ffa42ffe2204a05edaa20f55839"))
            .build();
        let result = client.deposit(&request).await;
        followed.extend(redirect.followed(status, "POST /deposit", &result));
        followed.extend(not_stopped(status, "POST /deposit", &result));
    }
    assert_none_followed(&followed);
    Ok(())
}
