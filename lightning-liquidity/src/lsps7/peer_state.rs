// This file is Copyright its original authors, visible in version control
// history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE
// or http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your option.
// You may not use this file except in accordance with one or both of these
// licenses.

//! Contains peer state objects that are used by `LSPS7ServiceHandler`.

use super::msgs::{
	LSPS7CreateOrderResponse, LSPS7ExtendableChannel, LSPS7OrderId, LSPS7OrderState,
	LSPS7OriginalOrder, LSPS7Request,
};

use crate::lsps0::ser::{LSPSDateTime, LSPSRequestId};
use crate::lsps1::msgs::{LSPS1PaymentInfo, LSPS1PaymentState};
use crate::lsps1::service::PaymentMethod;
use crate::prelude::HashMap;

use lightning::util::hash_tables::new_hash_map;
use lightning::{impl_writeable_tlv_based, impl_writeable_tlv_based_enum};

use alloc::string::String;
use alloc::vec::Vec;

use core::fmt;

const MAX_PENDING_REQUESTS_PER_PEER: usize = 10;

/// Where an extension order is in its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ExtensionOrderState {
	/// Awaiting payment. Payment states are EXPECT_PAYMENT.
	ExpectingPayment { payment_details: LSPS1PaymentInfo },
	/// Payment received but not yet settled; the paid method is HOLD.
	Paid { payment_details: LSPS1PaymentInfo },
	/// The new expiry is in force (terminal). The paid method is PAID.
	Completed { payment_details: LSPS1PaymentInfo },
	/// The order failed and any payment was returned (terminal). Payment states are REFUNDED.
	FailedAndRefunded { payment_details: LSPS1PaymentInfo },
}

impl_writeable_tlv_based_enum!(ExtensionOrderState,
	(0, ExpectingPayment) => {
		(0, payment_details, required),
	},
	(2, Paid) => {
		(0, payment_details, required),
	},
	(4, Completed) => {
		(0, payment_details, required),
	},
	(6, FailedAndRefunded) => {
		(0, payment_details, required),
	}
);

impl ExtensionOrderState {
	fn payment_details(&self) -> &LSPS1PaymentInfo {
		match self {
			Self::ExpectingPayment { payment_details }
			| Self::Paid { payment_details }
			| Self::Completed { payment_details }
			| Self::FailedAndRefunded { payment_details } => payment_details,
		}
	}

	fn order_state(&self) -> LSPS7OrderState {
		match self {
			Self::ExpectingPayment { .. } | Self::Paid { .. } => LSPS7OrderState::Created,
			Self::Completed { .. } => LSPS7OrderState::Completed,
			Self::FailedAndRefunded { .. } => LSPS7OrderState::Failed,
		}
	}

	fn name(&self) -> &'static str {
		match self {
			Self::ExpectingPayment { .. } => "ExpectingPayment",
			Self::Paid { .. } => "Paid",
			Self::Completed { .. } => "Completed",
			Self::FailedAndRefunded { .. } => "FailedAndRefunded",
		}
	}

	/// ExpectingPayment -> Paid, with `method` set to HOLD.
	fn payment_received(&mut self, method: PaymentMethod) -> Result<(), PeerStateError> {
		match self {
			Self::ExpectingPayment { payment_details } => {
				let mut details = payment_details.clone();
				let state = match method {
					PaymentMethod::Bolt11 => details.bolt11.as_mut().map(|p| &mut p.state),
					PaymentMethod::Bolt12 => details.bolt12.as_mut().map(|p| &mut p.state),
					PaymentMethod::Onchain => details.onchain.as_mut().map(|p| &mut p.state),
				};
				match state {
					Some(state) => *state = LSPS1PaymentState::Hold,
					None => return Err(PeerStateError::PaymentMethodNotConfigured),
				}
				*self = Self::Paid { payment_details: details };
				Ok(())
			},
			_ => Err(self.invalid("payment_received")),
		}
	}

	/// Paid -> Completed, with HOLD payments set to PAID.
	fn extension_completed(&mut self) -> Result<(), PeerStateError> {
		match self {
			Self::Paid { payment_details } => {
				let mut details = payment_details.clone();
				set_payment_states(&mut details, |s| {
					if *s == LSPS1PaymentState::Hold {
						*s = LSPS1PaymentState::Paid;
					}
				});
				*self = Self::Completed { payment_details: details };
				Ok(())
			},
			_ => Err(self.invalid("extension_completed")),
		}
	}

	/// ExpectingPayment|Paid -> FailedAndRefunded, with all payments set to REFUNDED.
	fn failed_and_refunded(&mut self) -> Result<(), PeerStateError> {
		match self {
			Self::ExpectingPayment { payment_details } | Self::Paid { payment_details } => {
				let mut details = payment_details.clone();
				set_payment_states(&mut details, |s| *s = LSPS1PaymentState::Refunded);
				*self = Self::FailedAndRefunded { payment_details: details };
				Ok(())
			},
			_ => Err(self.invalid("failed_and_refunded")),
		}
	}

	fn invalid(&self, action: &'static str) -> PeerStateError {
		PeerStateError::InvalidStateTransition { from: self.name(), action }
	}
}

fn set_payment_states(details: &mut LSPS1PaymentInfo, f: impl Fn(&mut LSPS1PaymentState)) {
	if let Some(p) = details.bolt11.as_mut() {
		f(&mut p.state);
	}
	if let Some(p) = details.bolt12.as_mut() {
		f(&mut p.state);
	}
	if let Some(p) = details.onchain.as_mut() {
		f(&mut p.state);
	}
}

/// An extension order, with the channel as it was when the order was created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ExtensionOrder {
	pub(super) short_channel_id: String,
	pub(super) original_order_id: Option<String>,
	pub(super) original_order_service: Option<String>,
	pub(super) prior_extension_order_ids: Vec<String>,
	pub(super) max_channel_extension_expiry_blocks: u32,
	pub(super) expiration_block: u32,
	pub(super) channel_extension_expiry_blocks: u32,
	pub(super) new_channel_expiry_block: u32,
	pub(super) token: String,
	pub(super) created_at: LSPSDateTime,
	pub(super) state: ExtensionOrderState,
}

impl_writeable_tlv_based!(ExtensionOrder, {
	(0, short_channel_id, required),
	(2, original_order_id, option),
	(4, original_order_service, option),
	(6, prior_extension_order_ids, optional_vec),
	(8, max_channel_extension_expiry_blocks, required),
	(10, expiration_block, required),
	(12, channel_extension_expiry_blocks, required),
	(14, new_channel_expiry_block, required),
	(16, token, required),
	(18, created_at, required),
	(20, state, required),
});

impl ExtensionOrder {
	pub(super) fn new(
		channel: LSPS7ExtendableChannel, channel_extension_expiry_blocks: u32,
		new_channel_expiry_block: u32, token: String, created_at: LSPSDateTime,
		payment_details: LSPS1PaymentInfo,
	) -> Self {
		let (original_order_id, original_order_service) = match channel.original_order {
			Some(o) => (Some(o.id), Some(o.service)),
			None => (None, None),
		};
		Self {
			short_channel_id: channel.short_channel_id,
			original_order_id,
			original_order_service,
			prior_extension_order_ids: channel.extension_order_ids.unwrap_or_default(),
			max_channel_extension_expiry_blocks: channel.max_channel_extension_expiry_blocks,
			expiration_block: channel.expiration_block,
			channel_extension_expiry_blocks,
			new_channel_expiry_block,
			token,
			created_at,
			state: ExtensionOrderState::ExpectingPayment { payment_details },
		}
	}

	/// The order as an LSPS7 `create_order` / `get_order` response.
	///
	/// Once the order is completed, the channel carries the new expiry and lists this order
	/// among its extensions.
	pub(super) fn to_response(&self, order_id: &LSPS7OrderId) -> LSPS7CreateOrderResponse {
		let completed = matches!(self.state, ExtensionOrderState::Completed { .. });
		let mut extension_order_ids = self.prior_extension_order_ids.clone();
		if completed {
			extension_order_ids.push(order_id.0.clone());
		}
		let original_order = match (&self.original_order_id, &self.original_order_service) {
			(Some(id), Some(service)) => {
				Some(LSPS7OriginalOrder { id: id.clone(), service: service.clone() })
			},
			_ => None,
		};
		LSPS7CreateOrderResponse {
			order_id: order_id.clone(),
			channel_extension_expiry_blocks: self.channel_extension_expiry_blocks,
			new_channel_expiry_block: self.new_channel_expiry_block,
			token: self.token.clone(),
			created_at: self.created_at.clone(),
			order_state: self.state.order_state(),
			payment: self.state.payment_details().clone(),
			channel: LSPS7ExtendableChannel {
				original_order,
				extension_order_ids: if extension_order_ids.is_empty() {
					None
				} else {
					Some(extension_order_ids)
				},
				short_channel_id: self.short_channel_id.clone(),
				max_channel_extension_expiry_blocks: self.max_channel_extension_expiry_blocks,
				expiration_block: if completed {
					self.new_channel_expiry_block
				} else {
					self.expiration_block
				},
			},
		}
	}

	fn is_unpaid(&self) -> bool {
		matches!(
			self.state,
			ExtensionOrderState::ExpectingPayment { .. }
				| ExtensionOrderState::FailedAndRefunded { .. }
		)
	}

	fn is_prunable(&self) -> bool {
		let all_payment_details_expired;
		#[cfg(feature = "time")]
		{
			let details = self.state.payment_details();
			all_payment_details_expired =
				details.bolt11.as_ref().map_or(true, |d| d.expires_at.is_past())
					&& details.bolt12.as_ref().map_or(true, |d| d.expires_at.is_past())
					&& details.onchain.as_ref().map_or(true, |d| d.expires_at.is_past());
		}
		#[cfg(not(feature = "time"))]
		{
			all_payment_details_expired = false;
		}
		all_payment_details_expired && self.is_unpaid()
	}
}

#[derive(Default)]
pub(crate) struct PeerState {
	orders: HashMap<LSPS7OrderId, ExtensionOrder>,
	pending_requests: HashMap<LSPSRequestId, LSPS7Request>,
	needs_persist: bool,
}

impl_writeable_tlv_based!(PeerState, {
	(0, orders, required),
	(_unused, pending_requests, (static_value, new_hash_map())),
	(_unused, needs_persist, (static_value, false)),
});

impl PeerState {
	pub(super) fn register_request(
		&mut self, request_id: LSPSRequestId, request: LSPS7Request,
	) -> Result<(), PeerStateError> {
		if self.pending_requests_and_unpaid_orders() >= MAX_PENDING_REQUESTS_PER_PEER {
			return Err(PeerStateError::TooManyPendingRequests);
		}
		if self.pending_requests.contains_key(&request_id) {
			return Err(PeerStateError::DuplicateRequestId);
		}
		self.pending_requests.insert(request_id, request);
		Ok(())
	}

	pub(super) fn get_request(
		&self, request_id: &LSPSRequestId,
	) -> Result<&LSPS7Request, PeerStateError> {
		self.pending_requests.get(request_id).ok_or(PeerStateError::UnknownRequestId)
	}

	pub(super) fn remove_request(
		&mut self, request_id: &LSPSRequestId,
	) -> Result<LSPS7Request, PeerStateError> {
		self.pending_requests.remove(request_id).ok_or(PeerStateError::UnknownRequestId)
	}

	pub(super) fn new_order(&mut self, order_id: LSPS7OrderId, order: ExtensionOrder) {
		self.orders.insert(order_id, order);
		self.needs_persist = true;
	}

	pub(super) fn get_order(&self, order_id: &LSPS7OrderId) -> Option<&ExtensionOrder> {
		self.orders.get(order_id)
	}

	pub(super) fn orders(&self) -> impl Iterator<Item = (&LSPS7OrderId, &ExtensionOrder)> {
		self.orders.iter()
	}

	pub(super) fn order_payment_received(
		&mut self, order_id: &LSPS7OrderId, method: PaymentMethod,
	) -> Result<(), PeerStateError> {
		let order = self.orders.get_mut(order_id).ok_or(PeerStateError::UnknownOrderId)?;
		order.state.payment_received(method)?;
		self.needs_persist = true;
		Ok(())
	}

	pub(super) fn order_extension_completed(
		&mut self, order_id: &LSPS7OrderId,
	) -> Result<(), PeerStateError> {
		let order = self.orders.get_mut(order_id).ok_or(PeerStateError::UnknownOrderId)?;
		order.state.extension_completed()?;
		self.needs_persist = true;
		Ok(())
	}

	pub(super) fn order_failed_and_refunded(
		&mut self, order_id: &LSPS7OrderId,
	) -> Result<(), PeerStateError> {
		let order = self.orders.get_mut(order_id).ok_or(PeerStateError::UnknownOrderId)?;
		order.state.failed_and_refunded()?;
		self.needs_persist = true;
		Ok(())
	}

	pub(super) fn needs_persist(&self) -> bool {
		self.needs_persist
	}

	pub(super) fn set_needs_persist(&mut self, needs_persist: bool) {
		self.needs_persist = needs_persist;
	}

	pub(super) fn is_prunable(&self) -> bool {
		self.pending_requests.is_empty() && self.orders.is_empty()
	}

	pub(super) fn prune_pending_requests(&mut self) {
		self.pending_requests.clear();
	}

	pub(super) fn prune_expired_request_state(&mut self) {
		let before = self.orders.len();
		self.orders.retain(|_, order| !order.is_prunable());
		if self.orders.len() != before {
			self.needs_persist = true;
		}
	}

	fn pending_requests_and_unpaid_orders(&self) -> usize {
		let unpaid = self
			.orders
			.values()
			.filter(|o| matches!(o.state, ExtensionOrderState::ExpectingPayment { .. }))
			.count();
		self.pending_requests.len() + unpaid
	}
}

#[derive(Debug, Clone)]
pub(super) enum PeerStateError {
	UnknownRequestId,
	DuplicateRequestId,
	UnknownOrderId,
	TooManyPendingRequests,
	PaymentMethodNotConfigured,
	InvalidStateTransition { from: &'static str, action: &'static str },
}

impl fmt::Display for PeerStateError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::UnknownRequestId => write!(f, "unknown request id"),
			Self::DuplicateRequestId => write!(f, "duplicate request id"),
			Self::UnknownOrderId => write!(f, "unknown order id"),
			Self::TooManyPendingRequests => write!(f, "too many pending requests"),
			Self::PaymentMethodNotConfigured => {
				write!(f, "payment method not configured for this order")
			},
			Self::InvalidStateTransition { from, action } => {
				write!(f, "invalid state transition: cannot {} from {}", action, from)
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::lsps1::msgs::LSPS1Bolt11PaymentInfo;

	use lightning::util::ser::{Readable, Writeable};
	use lightning_invoice::Bolt11Invoice;

	use core::str::FromStr;

	fn payment_info() -> LSPS1PaymentInfo {
		let invoice_str = "lnbc252u1p3aht9ysp580g4633gd2x9lc5al0wd8wx0mpn9748jeyz46kqjrpxn52uhfpjqpp5qgf67tcqmuqehzgjm8mzya90h73deafvr4m5705l5u5l4r05l8cqdpud3h8ymm4w3jhytnpwpczqmt0de6xsmre2pkxzm3qydmkzdjrdev9s7zhgfaqxqyjw5qcqpjrzjqt6xptnd85lpqnu2lefq4cx070v5cdwzh2xlvmdgnu7gqp4zvkus5zapryqqx9qqqyqqqqqqqqqqqcsq9q9qyysgqen77vu8xqjelum24hgjpgfdgfgx4q0nehhalcmuggt32japhjuksq9jv6eksjfnppm4hrzsgyxt8y8xacxut9qv3fpyetz8t7tsymygq8yzn05";
		LSPS1PaymentInfo {
			bolt11: Some(LSPS1Bolt11PaymentInfo {
				state: LSPS1PaymentState::ExpectPayment,
				expires_at: LSPSDateTime::from_str("2035-01-01T00:00:00Z").unwrap(),
				fee_total_sat: 1000,
				order_total_sat: 1000,
				invoice: Bolt11Invoice::from_str(invoice_str).unwrap(),
			}),
			bolt12: None,
			onchain: None,
		}
	}

	fn channel() -> LSPS7ExtendableChannel {
		LSPS7ExtendableChannel {
			original_order: Some(LSPS7OriginalOrder {
				id: "lsps1-order".to_string(),
				service: "LSPS1".to_string(),
			}),
			extension_order_ids: Some(vec!["earlier".to_string()]),
			short_channel_id: "800000x1x0".to_string(),
			max_channel_extension_expiry_blocks: 52560,
			expiration_block: 850000,
		}
	}

	fn order() -> ExtensionOrder {
		ExtensionOrder::new(
			channel(),
			4320,
			854320,
			String::new(),
			LSPSDateTime::from_str("2030-01-01T00:00:00Z").unwrap(),
			payment_info(),
		)
	}

	#[test]
	fn completed_order_reports_the_new_expiry() {
		let id = LSPS7OrderId("ext".to_string());
		let mut order = order();
		let created = order.to_response(&id);
		assert_eq!(created.order_state, LSPS7OrderState::Created);
		assert_eq!(created.channel, channel());

		order.state.payment_received(PaymentMethod::Bolt11).unwrap();
		let paid = order.to_response(&id);
		assert_eq!(paid.payment.bolt11.unwrap().state, LSPS1PaymentState::Hold);
		assert_eq!(paid.channel.expiration_block, 850000);

		order.state.extension_completed().unwrap();
		let done = order.to_response(&id);
		assert_eq!(done.order_state, LSPS7OrderState::Completed);
		assert_eq!(done.payment.bolt11.unwrap().state, LSPS1PaymentState::Paid);
		assert_eq!(done.channel.expiration_block, 854320);
		assert_eq!(
			done.channel.extension_order_ids,
			Some(vec!["earlier".to_string(), "ext".to_string()])
		);
	}

	#[test]
	fn invalid_transitions_are_refused() {
		let mut order = order();
		assert!(order.state.extension_completed().is_err());
		assert!(matches!(
			order.state.payment_received(PaymentMethod::Onchain),
			Err(PeerStateError::PaymentMethodNotConfigured)
		));
		order.state.payment_received(PaymentMethod::Bolt11).unwrap();
		assert!(order.state.payment_received(PaymentMethod::Bolt11).is_err());
		order.state.failed_and_refunded().unwrap();
		assert_eq!(
			order.state.payment_details().bolt11.as_ref().unwrap().state,
			LSPS1PaymentState::Refunded
		);
		assert!(order.state.failed_and_refunded().is_err());
	}

	#[test]
	fn peer_state_round_trips() {
		let mut state = PeerState::default();
		state.new_order(LSPS7OrderId("a".to_string()), order());
		let mut without_history = order();
		without_history.original_order_id = None;
		without_history.original_order_service = None;
		without_history.prior_extension_order_ids = Vec::new();
		state.new_order(LSPS7OrderId("b".to_string()), without_history.clone());
		state
			.order_payment_received(&LSPS7OrderId("a".to_string()), PaymentMethod::Bolt11)
			.unwrap();

		let read = PeerState::read(&mut &state.encode()[..]).unwrap();
		assert_eq!(read.orders, state.orders);
		assert!(!read.needs_persist);
		let b = read.get_order(&LSPS7OrderId("b".to_string())).unwrap();
		assert_eq!(b.to_response(&LSPS7OrderId("b".to_string())).channel.original_order, None);
		assert_eq!(b.to_response(&LSPS7OrderId("b".to_string())).channel.extension_order_ids, None);
	}
}
