// This file is Copyright its original authors, visible in version control
// history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE
// or http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your option.
// You may not use this file except in accordance with one or both of these
// licenses.

//! Contains the main bLIP-57 / LSPS7 server object, [`LSPS7ServiceHandler`].
//!
//! The LSP keeps its own record of which channels it has leased and until when; this handler
//! holds the extension orders. A client's `get_extendable_channels` and `create_order` requests
//! surface as [`LSPS7ServiceEvent`]s for the LSP to answer from its records, and `get_order`
//! requests are answered from the persisted orders.

use alloc::string::ToString;
use alloc::vec::Vec;

use core::future::Future as StdFuture;
use core::ops::Deref;
use core::pin::pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task;

use super::event::LSPS7ServiceEvent;
use super::msgs::{
	LSPS7CreateOrderRequest, LSPS7CreateOrderResponse, LSPS7ExtendableChannel,
	LSPS7GetExtendableChannelsResponse, LSPS7GetOrderRequest, LSPS7Message, LSPS7OrderId,
	LSPS7Request, LSPS7Response, LSPS7_GET_ORDER_REQUEST_ORDER_NOT_FOUND_ERROR_CODE,
};
use super::peer_state::{ExtensionOrder, PeerState};
use crate::lsps1::msgs::{LSPS1PaymentInfo, LSPS1PaymentState};
use crate::lsps1::service::PaymentMethod;
use crate::message_queue::MessageQueue;

use crate::events::EventQueue;
use crate::lsps0::ser::{
	LSPSDateTime, LSPSProtocolMessageHandler, LSPSRequestId, LSPSResponseError,
	LSPS0_CLIENT_REJECTED_ERROR_CODE,
};
use crate::persist::{
	LIQUIDITY_MANAGER_PERSISTENCE_PRIMARY_NAMESPACE, LSPS7_SERVICE_PERSISTENCE_SECONDARY_NAMESPACE,
};
use crate::prelude::hash_map::Entry;
use crate::prelude::HashMap;
use crate::sync::{Arc, Mutex, RwLock};
use crate::utils;
use crate::utils::async_poll::dummy_waker;
use crate::utils::time::TimeProvider;

use lightning::ln::msgs::{ErrorAction, LightningError};
use lightning::sign::EntropySource;
use lightning::util::errors::APIError;
use lightning::util::logger::Level;
use lightning::util::persist::KVStore;
use lightning::util::ser::Writeable;

use bitcoin::secp256k1::PublicKey;

/// Server-side configuration options for bLIP-57 / LSPS7 channel lease extensions.
///
/// The limits that apply to an extension (the most blocks one order may add) are set per
/// channel, in the [`LSPS7ExtendableChannel`]s the LSP returns.
#[derive(Clone, Debug, Default)]
pub struct LSPS7ServiceConfig {}

const MAX_TOTAL_PEERS: usize = 100000;

/// The main object allowing to send and receive bLIP-57 / LSPS7 messages.
pub struct LSPS7ServiceHandler<ES: EntropySource, K: KVStore + Clone, TP: Deref + Clone>
where
	TP::Target: TimeProvider,
{
	entropy_source: ES,
	kv_store: K,
	pending_messages: Arc<MessageQueue>,
	pending_events: Arc<EventQueue<K>>,
	per_peer_state: RwLock<HashMap<PublicKey, Mutex<PeerState>>>,
	persistence_in_flight: AtomicUsize,
	time_provider: TP,
	config: LSPS7ServiceConfig,
}

impl<ES: EntropySource, K: KVStore + Clone, TP: Deref + Clone> LSPS7ServiceHandler<ES, K, TP>
where
	TP::Target: TimeProvider,
{
	/// Constructs a `LSPS7ServiceHandler`.
	pub(crate) fn new(
		per_peer_state: HashMap<PublicKey, Mutex<PeerState>>, entropy_source: ES,
		pending_messages: Arc<MessageQueue>, pending_events: Arc<EventQueue<K>>, kv_store: K,
		time_provider: TP, config: LSPS7ServiceConfig,
	) -> Self {
		Self {
			entropy_source,
			kv_store,
			pending_messages,
			pending_events,
			per_peer_state: RwLock::new(per_peer_state),
			persistence_in_flight: AtomicUsize::new(0),
			time_provider,
			config,
		}
	}

	/// Returns a reference to the used config.
	pub fn config(&self) -> &LSPS7ServiceConfig {
		&self.config
	}

	pub(crate) fn peer_disconnected(&self, counterparty_node_id: PublicKey) {
		let outer_state_lock = self.per_peer_state.read().unwrap();
		if let Some(inner_state_lock) = outer_state_lock.get(&counterparty_node_id) {
			let mut peer_state_lock = inner_state_lock.lock().unwrap();
			peer_state_lock.prune_pending_requests();
			peer_state_lock.prune_expired_request_state();
		}
	}

	pub(crate) async fn persist(&self) -> Result<bool, lightning::io::Error> {
		if self.persistence_in_flight.fetch_add(1, Ordering::AcqRel) > 0 {
			// Another call is persisting; the increment tells it to go around again.
			return Ok(false);
		}

		let res = self.do_persist().await;
		debug_assert!(res.is_err() || self.persistence_in_flight.load(Ordering::Acquire) == 0);
		self.persistence_in_flight.store(0, Ordering::Release);
		res
	}

	async fn do_persist(&self) -> Result<bool, lightning::io::Error> {
		let mut did_persist = false;

		loop {
			let mut need_remove = Vec::new();
			let mut need_persist = Vec::new();

			{
				let outer_state_lock = self.per_peer_state.read().unwrap();
				for (counterparty_node_id, inner_state_lock) in outer_state_lock.iter() {
					let mut peer_state_lock = inner_state_lock.lock().unwrap();
					peer_state_lock.prune_expired_request_state();
					if peer_state_lock.is_prunable() {
						need_remove.push(*counterparty_node_id);
					} else if peer_state_lock.needs_persist() {
						need_persist.push(*counterparty_node_id);
					}
				}
			}

			for counterparty_node_id in need_persist.into_iter() {
				self.persist_peer_state(counterparty_node_id).await?;
				did_persist = true;
			}

			for counterparty_node_id in need_remove {
				let mut future_opt = None;
				{
					// Hold the write lock across the removal (not the future) so writes for this
					// peer stay ordered with other `persist_peer_state` calls.
					let mut per_peer_state = self.per_peer_state.write().unwrap();
					if let Entry::Occupied(mut entry) = per_peer_state.entry(counterparty_node_id) {
						let state = entry.get_mut().get_mut().unwrap();
						if state.is_prunable() {
							entry.remove();
							let key = counterparty_node_id.to_string();
							future_opt = Some(self.kv_store.remove(
								LIQUIDITY_MANAGER_PERSISTENCE_PRIMARY_NAMESPACE,
								LSPS7_SERVICE_PERSISTENCE_SECONDARY_NAMESPACE,
								&key,
								true,
							));
						} else {
							state.set_needs_persist(true);
						}
					} else {
						debug_assert!(false);
					}
				}
				if let Some(future) = future_opt {
					future.await?;
					did_persist = true;
				} else {
					self.persist_peer_state(counterparty_node_id).await?;
				}
			}

			if self.persistence_in_flight.fetch_sub(1, Ordering::AcqRel) != 1 {
				self.persistence_in_flight.store(1, Ordering::Release);
				continue;
			}
			break;
		}

		Ok(did_persist)
	}

	async fn persist_peer_state(
		&self, counterparty_node_id: PublicKey,
	) -> Result<(), lightning::io::Error> {
		let fut = {
			let outer_state_lock = self.per_peer_state.read().unwrap();
			match outer_state_lock.get(&counterparty_node_id) {
				None => return Ok(()),
				Some(entry) => {
					let mut peer_state_lock = entry.lock().unwrap();
					if !peer_state_lock.needs_persist() {
						return Ok(());
					}
					peer_state_lock.set_needs_persist(false);
					let key = counterparty_node_id.to_string();
					let encoded = peer_state_lock.encode();
					self.kv_store.write(
						LIQUIDITY_MANAGER_PERSISTENCE_PRIMARY_NAMESPACE,
						LSPS7_SERVICE_PERSISTENCE_SECONDARY_NAMESPACE,
						&key,
						encoded,
					)
				},
			}
		};

		fut.await.map_err(|e| {
			self.per_peer_state
				.read()
				.unwrap()
				.get(&counterparty_node_id)
				.map(|p| p.lock().unwrap().set_needs_persist(true));
			e
		})
	}

	async fn persist_after_change(&self, counterparty_node_id: PublicKey) -> Result<(), APIError> {
		self.persist_peer_state(counterparty_node_id).await.map_err(|e| APIError::APIMisuseError {
			err: format!("Failed to persist peer state for {}: {}", counterparty_node_id, e),
		})
	}

	/// Registers a request from `counterparty_node_id`, or answers it with an error when the
	/// peer limits are reached.
	fn register_request(
		&self, request_id: &LSPSRequestId, counterparty_node_id: &PublicKey, request: LSPS7Request,
		error_response: fn(LSPSResponseError) -> LSPS7Response,
	) -> Result<(), LightningError> {
		let mut message_queue_notifier = self.pending_messages.notifier();
		let mut outer_state_lock = self.per_peer_state.write().unwrap();
		let num_peers = outer_state_lock.len();
		let inner_state_entry = outer_state_lock.entry(*counterparty_node_id);
		let rejection =
			if matches!(inner_state_entry, Entry::Vacant(_)) && num_peers >= MAX_TOTAL_PEERS {
				Some(format!("reached the maximum number of peers ({})", MAX_TOTAL_PEERS))
			} else {
				let mut peer_state_lock =
					inner_state_entry.or_insert(Mutex::new(PeerState::default())).lock().unwrap();
				peer_state_lock
					.register_request(request_id.clone(), request)
					.err()
					.map(|e| e.to_string())
			};
		match rejection {
			None => Ok(()),
			Some(err) => {
				let response = error_response(LSPSResponseError {
					code: LSPS0_CLIENT_REJECTED_ERROR_CODE,
					message: format!("Failed to handle request due to: {}", err),
					data: None,
				});
				let msg = LSPS7Message::Response(request_id.clone(), response).into();
				message_queue_notifier.enqueue(counterparty_node_id, msg);
				Err(LightningError { err, action: ErrorAction::IgnoreAndLog(Level::Debug) })
			},
		}
	}

	fn handle_get_extendable_channels_request(
		&self, request_id: LSPSRequestId, counterparty_node_id: &PublicKey, request: LSPS7Request,
	) -> Result<(), LightningError> {
		self.register_request(
			&request_id,
			counterparty_node_id,
			request,
			LSPS7Response::GetExtendableChannelsError,
		)?;
		self.pending_events.notifier().enqueue(LSPS7ServiceEvent::GetExtendableChannels {
			request_id,
			counterparty_node_id: *counterparty_node_id,
		});
		Ok(())
	}

	fn handle_create_order_request(
		&self, request_id: LSPSRequestId, counterparty_node_id: &PublicKey,
		params: LSPS7CreateOrderRequest,
	) -> Result<(), LightningError> {
		self.register_request(
			&request_id,
			counterparty_node_id,
			LSPS7Request::CreateOrder(params.clone()),
			LSPS7Response::CreateOrderError,
		)?;
		self.pending_events.notifier().enqueue(LSPS7ServiceEvent::RequestForPaymentDetails {
			request_id,
			counterparty_node_id: *counterparty_node_id,
			request: params,
		});
		Ok(())
	}

	fn handle_get_order_request(
		&self, request_id: LSPSRequestId, counterparty_node_id: &PublicKey,
		params: LSPS7GetOrderRequest,
	) -> Result<(), LightningError> {
		let mut message_queue_notifier = self.pending_messages.notifier();
		// Orders are kept per peer, so a peer only ever finds its own orders.
		let response = {
			let outer_state_lock = self.per_peer_state.read().unwrap();
			outer_state_lock.get(counterparty_node_id).and_then(|inner| {
				let peer_state_lock = inner.lock().unwrap();
				peer_state_lock.get_order(&params.order_id).map(|o| o.to_response(&params.order_id))
			})
		};
		match response {
			Some(response) => {
				let msg = LSPS7Message::Response(request_id, LSPS7Response::GetOrder(response));
				message_queue_notifier.enqueue(counterparty_node_id, msg.into());
				Ok(())
			},
			None => {
				let response = LSPS7Response::GetOrderError(LSPSResponseError {
					code: LSPS7_GET_ORDER_REQUEST_ORDER_NOT_FOUND_ERROR_CODE,
					message: "Order with the requested order_id has not been found.".to_string(),
					data: None,
				});
				let msg = LSPS7Message::Response(request_id, response).into();
				message_queue_notifier.enqueue(counterparty_node_id, msg);
				Err(LightningError {
					err: format!(
						"Received get_order request for an unknown order from {}",
						counterparty_node_id
					),
					action: ErrorAction::IgnoreAndLog(Level::Info),
				})
			},
		}
	}

	/// Answers a `get_extendable_channels` request with the peer's channels that can be extended.
	///
	/// Should be called in response to receiving a [`LSPS7ServiceEvent::GetExtendableChannels`]
	/// event. bLIP-57 requires these to be channels between the client and the LSP only.
	pub fn extendable_channels(
		&self, request_id: LSPSRequestId, counterparty_node_id: PublicKey,
		extendable_channels: Vec<LSPS7ExtendableChannel>,
	) -> Result<(), APIError> {
		let mut message_queue_notifier = self.pending_messages.notifier();
		self.take_request(&request_id, &counterparty_node_id, |r| {
			matches!(r, LSPS7Request::GetExtendableChannels(_))
		})?;
		let response = LSPS7Response::GetExtendableChannels(LSPS7GetExtendableChannelsResponse {
			extendable_channels,
		});
		let msg = LSPS7Message::Response(request_id, response).into();
		message_queue_notifier.enqueue(&counterparty_node_id, msg);
		Ok(())
	}

	/// Accepts a `create_order` request and sends the client the payment details.
	///
	/// Should be called in response to receiving a [`LSPS7ServiceEvent::RequestForPaymentDetails`]
	/// event once the LSP has checked the request against its own records. `channel` is the
	/// channel the request names, as [`Self::extendable_channels`] would report it now; the new
	/// expiry is its `expiration_block` plus the requested `channel_extension_expiry_blocks`.
	///
	/// Returns the id of the new order, which the order's later state transitions take.
	///
	/// Fails, leaving the request pending, if `channel` is not the requested channel, the
	/// requested extension is outside `1..=channel.max_channel_extension_expiry_blocks`, the
	/// payment details are malformed, or onchain payment is offered without the refund address
	/// bLIP-57 requires. Use [`Self::reject_order`] to turn such a request down.
	pub async fn send_payment_details(
		&self, request_id: LSPSRequestId, counterparty_node_id: PublicKey,
		channel: LSPS7ExtendableChannel, payment_details: LSPS1PaymentInfo,
	) -> Result<LSPS7OrderId, APIError> {
		let mut message_queue_notifier = self.pending_messages.notifier();
		check_payment_details(&payment_details)?;

		let response = {
			let outer_state_lock = self.per_peer_state.read().unwrap();
			let inner_state_lock =
				outer_state_lock.get(&counterparty_node_id).ok_or_else(|| {
					APIError::APIMisuseError {
						err: format!(
							"No state for the counterparty exists: {}",
							counterparty_node_id
						),
					}
				})?;
			let mut peer_state_lock = inner_state_lock.lock().unwrap();
			let request = match peer_state_lock.get_request(&request_id) {
				Ok(LSPS7Request::CreateOrder(request)) => request.clone(),
				Ok(_) => {
					return Err(APIError::APIMisuseError {
						err: "The request is not a create_order request".to_string(),
					})
				},
				Err(e) => {
					return Err(APIError::APIMisuseError {
						err: format!("Failed to send response due to: {}", e),
					})
				},
			};
			let new_channel_expiry_block = check_extension(&request, &channel, &payment_details)?;
			peer_state_lock.remove_request(&request_id).map_err(|e| APIError::APIMisuseError {
				err: format!("Failed to send response due to: {}", e),
			})?;

			let order_id = self.generate_order_id();
			let created_at = LSPSDateTime::new_from_duration_since_epoch(
				self.time_provider.duration_since_epoch(),
			);
			let order = ExtensionOrder::new(
				channel,
				request.channel_extension_expiry_blocks,
				new_channel_expiry_block,
				request.token.clone().unwrap_or_default(),
				created_at,
				payment_details,
			);
			let response = order.to_response(&order_id);
			peer_state_lock.new_order(order_id, order);
			response
		};

		let order_id = response.order_id.clone();
		let msg = LSPS7Message::Response(request_id, LSPS7Response::CreateOrder(response));
		message_queue_notifier.enqueue(&counterparty_node_id, msg.into());
		self.persist_after_change(counterparty_node_id).await?;
		Ok(order_id)
	}

	/// Turns down a pending `get_extendable_channels` or `create_order` request with the given
	/// error, for example [`LSPS7_CREATE_ORDER_REQUEST_OPTION_MISMATCH_ERROR_CODE`] for a channel
	/// that is not extendable or an extension that is too long, or
	/// [`LSPS7_CREATE_ORDER_REQUEST_UNRECOGNIZED_OR_STALE_TOKEN_ERROR_CODE`] for a bad token.
	///
	/// [`LSPS7_CREATE_ORDER_REQUEST_OPTION_MISMATCH_ERROR_CODE`]: super::msgs::LSPS7_CREATE_ORDER_REQUEST_OPTION_MISMATCH_ERROR_CODE
	/// [`LSPS7_CREATE_ORDER_REQUEST_UNRECOGNIZED_OR_STALE_TOKEN_ERROR_CODE`]: super::msgs::LSPS7_CREATE_ORDER_REQUEST_UNRECOGNIZED_OR_STALE_TOKEN_ERROR_CODE
	pub fn reject_order(
		&self, request_id: LSPSRequestId, counterparty_node_id: PublicKey, error: LSPSResponseError,
	) -> Result<(), APIError> {
		let mut message_queue_notifier = self.pending_messages.notifier();
		let request = self.take_request(&request_id, &counterparty_node_id, |_| true)?;
		let response = match request {
			LSPS7Request::GetExtendableChannels(_) => {
				LSPS7Response::GetExtendableChannelsError(error)
			},
			_ => LSPS7Response::CreateOrderError(error),
		};
		let msg = LSPS7Message::Response(request_id, response).into();
		message_queue_notifier.enqueue(&counterparty_node_id, msg);
		Ok(())
	}

	fn take_request(
		&self, request_id: &LSPSRequestId, counterparty_node_id: &PublicKey,
		expected: impl Fn(&LSPS7Request) -> bool,
	) -> Result<LSPS7Request, APIError> {
		let outer_state_lock = self.per_peer_state.read().unwrap();
		let inner_state_lock =
			outer_state_lock.get(counterparty_node_id).ok_or_else(|| APIError::APIMisuseError {
				err: format!("No state for the counterparty exists: {}", counterparty_node_id),
			})?;
		let mut peer_state_lock = inner_state_lock.lock().unwrap();
		let request = peer_state_lock.get_request(request_id).map_err(|e| {
			APIError::APIMisuseError { err: format!("Failed to send response due to: {}", e) }
		})?;
		if !expected(request) {
			return Err(APIError::APIMisuseError {
				err: "The pending request is of a different kind".to_string(),
			});
		}
		peer_state_lock.remove_request(request_id).map_err(|e| APIError::APIMisuseError {
			err: format!("Failed to send response due to: {}", e),
		})
	}

	/// Marks an order as paid. The paid method's state becomes HOLD.
	///
	/// For a Lightning payment, call this on LDK's [`Event::PaymentClaimable`] without claiming
	/// the payment. bLIP-57 requires the new expiry to be persisted before the preimage is
	/// released: record the extension, call [`Self::order_extension_completed`], then claim.
	///
	/// [`Event::PaymentClaimable`]: lightning::events::Event::PaymentClaimable
	pub async fn order_payment_received(
		&self, counterparty_node_id: PublicKey, order_id: LSPS7OrderId, method: PaymentMethod,
	) -> Result<(), APIError> {
		self.update_order(counterparty_node_id, |p| p.order_payment_received(&order_id, method))?;
		self.persist_after_change(counterparty_node_id).await
	}

	/// Marks an order as completed once the LSP has recorded the new expiry. HOLD payments become
	/// PAID and the order's channel reports the new expiry.
	pub async fn order_extension_completed(
		&self, counterparty_node_id: PublicKey, order_id: LSPS7OrderId,
	) -> Result<(), APIError> {
		self.update_order(counterparty_node_id, |p| p.order_extension_completed(&order_id))?;
		self.persist_after_change(counterparty_node_id).await
	}

	/// Marks an order as failed, with every payment option REFUNDED.
	///
	/// Call this when an order expires unpaid, or when a paid extension cannot be granted and the
	/// payment is being returned.
	pub async fn order_failed_and_refunded(
		&self, counterparty_node_id: PublicKey, order_id: LSPS7OrderId,
	) -> Result<(), APIError> {
		self.update_order(counterparty_node_id, |p| p.order_failed_and_refunded(&order_id))?;
		self.persist_after_change(counterparty_node_id).await
	}

	fn update_order(
		&self, counterparty_node_id: PublicKey,
		update: impl FnOnce(&mut PeerState) -> Result<(), super::peer_state::PeerStateError>,
	) -> Result<(), APIError> {
		let outer_state_lock = self.per_peer_state.read().unwrap();
		let inner_state_lock = outer_state_lock.get(&counterparty_node_id).ok_or_else(|| {
			APIError::APIMisuseError {
				err: format!("No existing state with counterparty {}", counterparty_node_id),
			}
		})?;
		let mut peer_state_lock = inner_state_lock.lock().unwrap();
		update(&mut peer_state_lock)
			.map_err(|e| APIError::APIMisuseError { err: format!("Failed to update order: {}", e) })
	}

	/// Looks up an order by id alone, whichever peer it belongs to.
	pub fn get_order(
		&self, order_id: &LSPS7OrderId,
	) -> Option<(PublicKey, LSPS7CreateOrderResponse)> {
		let outer_state_lock = self.per_peer_state.read().unwrap();
		for (counterparty_node_id, inner_state_lock) in outer_state_lock.iter() {
			let peer_state_lock = inner_state_lock.lock().unwrap();
			if let Some(order) = peer_state_lock.get_order(order_id) {
				return Some((*counterparty_node_id, order.to_response(order_id)));
			}
		}
		None
	}

	/// Returns every order the service holds, with the peer each belongs to.
	pub fn list_orders(&self) -> Vec<(PublicKey, LSPS7CreateOrderResponse)> {
		let outer_state_lock = self.per_peer_state.read().unwrap();
		let mut orders = Vec::new();
		for (counterparty_node_id, inner_state_lock) in outer_state_lock.iter() {
			let peer_state_lock = inner_state_lock.lock().unwrap();
			for (order_id, order) in peer_state_lock.orders() {
				orders.push((*counterparty_node_id, order.to_response(order_id)));
			}
		}
		orders
	}

	fn generate_order_id(&self) -> LSPS7OrderId {
		let bytes = self.entropy_source.get_secure_random_bytes();
		LSPS7OrderId(utils::hex_str(&bytes[0..16]))
	}
}

/// Checks a `create_order` request against the channel and payment details the LSP accepts it
/// with, and returns the new expiry.
fn check_extension(
	request: &LSPS7CreateOrderRequest, channel: &LSPS7ExtendableChannel,
	payment_details: &LSPS1PaymentInfo,
) -> Result<u32, APIError> {
	if channel.short_channel_id != request.short_channel_id {
		return Err(APIError::APIMisuseError {
			err: format!(
				"The channel given ({}) is not the one requested ({})",
				channel.short_channel_id, request.short_channel_id
			),
		});
	}
	if request.channel_extension_expiry_blocks == 0
		|| request.channel_extension_expiry_blocks > channel.max_channel_extension_expiry_blocks
	{
		return Err(APIError::APIMisuseError {
			err: format!(
				"The requested extension of {} blocks is outside 1..={}",
				request.channel_extension_expiry_blocks,
				channel.max_channel_extension_expiry_blocks
			),
		});
	}
	if payment_details.onchain.is_some() && request.refund_onchain_address.is_none() {
		return Err(APIError::APIMisuseError {
			err: "Onchain payments must be disabled if no refund_onchain_address is set."
				.to_string(),
		});
	}
	channel.expiration_block.checked_add(request.channel_extension_expiry_blocks).ok_or_else(|| {
		APIError::APIMisuseError { err: "The new expiry overflows a block height".to_string() }
	})
}

fn check_payment_details(payment_details: &LSPS1PaymentInfo) -> Result<(), APIError> {
	if payment_details.bolt11.is_none()
		&& payment_details.bolt12.is_none()
		&& payment_details.onchain.is_none()
	{
		let err = "At least one payment option must be provided".to_string();
		return Err(APIError::APIMisuseError { err });
	}
	let not_expecting = |s: &LSPS1PaymentState| *s != LSPS1PaymentState::ExpectPayment;
	if payment_details.bolt11.as_ref().is_some_and(|p| not_expecting(&p.state))
		|| payment_details.bolt12.as_ref().is_some_and(|p| not_expecting(&p.state))
		|| payment_details.onchain.as_ref().is_some_and(|p| not_expecting(&p.state))
	{
		return Err(APIError::APIMisuseError {
			err: "All payment methods must start in ExpectPayment state".to_string(),
		});
	}
	Ok(())
}

impl<ES: EntropySource, K: KVStore + Clone, TP: Deref + Clone> LSPSProtocolMessageHandler
	for LSPS7ServiceHandler<ES, K, TP>
where
	TP::Target: TimeProvider,
{
	type ProtocolMessage = LSPS7Message;
	const PROTOCOL_NUMBER: Option<u16> = Some(7);

	fn handle_message(
		&self, message: Self::ProtocolMessage, counterparty_node_id: &PublicKey,
	) -> Result<(), LightningError> {
		match message {
			LSPS7Message::Request(request_id, request) => match request {
				LSPS7Request::GetExtendableChannels(_) => self
					.handle_get_extendable_channels_request(
						request_id,
						counterparty_node_id,
						request,
					),
				LSPS7Request::CreateOrder(params) => {
					self.handle_create_order_request(request_id, counterparty_node_id, params)
				},
				LSPS7Request::GetOrder(params) => {
					self.handle_get_order_request(request_id, counterparty_node_id, params)
				},
			},
			_ => {
				debug_assert!(
					false,
					"Service handler received LSPS7 response message. This should never happen."
				);
				Err(LightningError {
					err: format!(
						"Service handler received LSPS7 response message from node {:?}. This should never happen.",
						counterparty_node_id
					),
					action: ErrorAction::IgnoreAndLog(Level::Info),
				})
			},
		}
	}
}

/// A synchronous wrapper around [`LSPS7ServiceHandler`] to be used in contexts where async is not
/// available.
pub struct LSPS7ServiceHandlerSync<'a, ES: EntropySource, K: KVStore + Clone, TP: Deref + Clone>
where
	TP::Target: TimeProvider,
{
	inner: &'a LSPS7ServiceHandler<ES, K, TP>,
}

fn poll_sync<T>(fut: impl StdFuture<Output = T>) -> T {
	let mut fut = pin!(fut);
	let mut waker = dummy_waker();
	let mut ctx = task::Context::from_waker(&mut waker);
	match fut.as_mut().poll(&mut ctx) {
		task::Poll::Ready(result) => result,
		task::Poll::Pending => {
			// In a sync context, we can't wait for the future to complete.
			unreachable!("Should not be pending in a sync context");
		},
	}
}

impl<'a, ES: EntropySource, K: KVStore + Clone, TP: Deref + Clone>
	LSPS7ServiceHandlerSync<'a, ES, K, TP>
where
	TP::Target: TimeProvider,
{
	pub(crate) fn from_inner(inner: &'a LSPS7ServiceHandler<ES, K, TP>) -> Self {
		Self { inner }
	}

	/// Wraps [`LSPS7ServiceHandler::config`].
	pub fn config(&self) -> &LSPS7ServiceConfig {
		self.inner.config()
	}

	/// Wraps [`LSPS7ServiceHandler::extendable_channels`].
	pub fn extendable_channels(
		&self, request_id: LSPSRequestId, counterparty_node_id: PublicKey,
		extendable_channels: Vec<LSPS7ExtendableChannel>,
	) -> Result<(), APIError> {
		self.inner.extendable_channels(request_id, counterparty_node_id, extendable_channels)
	}

	/// Wraps [`LSPS7ServiceHandler::send_payment_details`].
	pub fn send_payment_details(
		&self, request_id: LSPSRequestId, counterparty_node_id: PublicKey,
		channel: LSPS7ExtendableChannel, payment_details: LSPS1PaymentInfo,
	) -> Result<LSPS7OrderId, APIError> {
		poll_sync(self.inner.send_payment_details(
			request_id,
			counterparty_node_id,
			channel,
			payment_details,
		))
	}

	/// Wraps [`LSPS7ServiceHandler::reject_order`].
	pub fn reject_order(
		&self, request_id: LSPSRequestId, counterparty_node_id: PublicKey, error: LSPSResponseError,
	) -> Result<(), APIError> {
		self.inner.reject_order(request_id, counterparty_node_id, error)
	}

	/// Wraps [`LSPS7ServiceHandler::order_payment_received`].
	pub fn order_payment_received(
		&self, counterparty_node_id: PublicKey, order_id: LSPS7OrderId, method: PaymentMethod,
	) -> Result<(), APIError> {
		poll_sync(self.inner.order_payment_received(counterparty_node_id, order_id, method))
	}

	/// Wraps [`LSPS7ServiceHandler::order_extension_completed`].
	pub fn order_extension_completed(
		&self, counterparty_node_id: PublicKey, order_id: LSPS7OrderId,
	) -> Result<(), APIError> {
		poll_sync(self.inner.order_extension_completed(counterparty_node_id, order_id))
	}

	/// Wraps [`LSPS7ServiceHandler::order_failed_and_refunded`].
	pub fn order_failed_and_refunded(
		&self, counterparty_node_id: PublicKey, order_id: LSPS7OrderId,
	) -> Result<(), APIError> {
		poll_sync(self.inner.order_failed_and_refunded(counterparty_node_id, order_id))
	}

	/// Wraps [`LSPS7ServiceHandler::get_order`].
	pub fn get_order(
		&self, order_id: &LSPS7OrderId,
	) -> Option<(PublicKey, LSPS7CreateOrderResponse)> {
		self.inner.get_order(order_id)
	}

	/// Wraps [`LSPS7ServiceHandler::list_orders`].
	pub fn list_orders(&self) -> Vec<(PublicKey, LSPS7CreateOrderResponse)> {
		self.inner.list_orders()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::lsps7::msgs::LSPS7OriginalOrder;

	fn channel() -> LSPS7ExtendableChannel {
		LSPS7ExtendableChannel {
			original_order: Some(LSPS7OriginalOrder {
				id: "lsps1-order".to_string(),
				service: "LSPS1".to_string(),
			}),
			extension_order_ids: None,
			short_channel_id: "800000x1x0".to_string(),
			max_channel_extension_expiry_blocks: 52560,
			expiration_block: 850000,
		}
	}

	fn request(scid: &str, blocks: u32) -> LSPS7CreateOrderRequest {
		LSPS7CreateOrderRequest {
			short_channel_id: scid.to_string(),
			channel_extension_expiry_blocks: blocks,
			token: None,
			refund_onchain_address: None,
		}
	}

	fn no_onchain() -> LSPS1PaymentInfo {
		LSPS1PaymentInfo { bolt11: None, bolt12: None, onchain: None }
	}

	#[test]
	fn extension_adds_to_the_current_expiry() {
		assert_eq!(
			check_extension(&request("800000x1x0", 1), &channel(), &no_onchain()).unwrap(),
			850001
		);
		assert_eq!(
			check_extension(&request("800000x1x0", 52560), &channel(), &no_onchain()).unwrap(),
			902560
		);
	}

	#[test]
	fn extension_checks() {
		assert!(check_extension(&request("800000x1x0", 0), &channel(), &no_onchain()).is_err());
		assert!(check_extension(&request("800000x1x0", 52561), &channel(), &no_onchain()).is_err());
		assert!(check_extension(&request("800000x2x0", 10), &channel(), &no_onchain()).is_err());
		let mut late = channel();
		late.expiration_block = u32::MAX;
		assert!(check_extension(&request("800000x1x0", 1), &late, &no_onchain()).is_err());
	}
}
