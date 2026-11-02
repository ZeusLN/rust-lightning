// This file is Copyright its original authors, visible in version control
// history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE
// or http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your option.
// You may not use this file except in accordance with one or both of these
// licenses.

#![cfg(all(test, feature = "time"))]

mod common;

use common::{
	create_service_and_client_nodes, create_service_and_client_nodes_with_kv_stores,
	get_lsps_message, LSPSNodes,
};

use lightning::ln::functional_test_utils::{
	create_chanmon_cfgs, create_network, create_node_cfgs, create_node_chanmgrs, Node,
};
use lightning::ln::peer_handler::CustomMessageHandler;
use lightning::util::test_utils::{TestBroadcaster, TestStore};
use lightning_liquidity::events::LiquidityEvent;
use lightning_liquidity::lsps0::ser::{
	LSPSDateTime, LSPSRequestId, LSPSResponseError, RawLSPSMessage,
};
use lightning_liquidity::lsps1::msgs::{LSPS1PaymentInfo, LSPS1PaymentState};
use lightning_liquidity::lsps1::service::PaymentMethod;
use lightning_liquidity::lsps7::client::LSPS7ClientConfig;
use lightning_liquidity::lsps7::event::{LSPS7ClientEvent, LSPS7ServiceEvent};
use lightning_liquidity::lsps7::msgs::{
	LSPS7ExtendableChannel, LSPS7OrderId, LSPS7OrderState,
	LSPS7_CREATE_ORDER_REQUEST_OPTION_MISMATCH_ERROR_CODE,
	LSPS7_GET_ORDER_REQUEST_ORDER_NOT_FOUND_ERROR_CODE,
};
use lightning_liquidity::lsps7::service::LSPS7ServiceConfig;
use lightning_liquidity::utils::time::DefaultTimeProvider;
use lightning_liquidity::{LiquidityClientConfig, LiquidityManagerSync, LiquidityServiceConfig};

use bitcoin::{Address, Network};

use serde_json::{json, Value};

use std::str::FromStr;
use std::sync::Arc;

const BOLT11_INVOICE: &str = "lnbc252u1p3aht9ysp580g4633gd2x9lc5al0wd8wx0mpn9748jeyz46kqjrpxn52uhfpjqpp5qgf67tcqmuqehzgjm8mzya90h73deafvr4m5705l5u5l4r05l8cqdpud3h8ymm4w3jhytnpwpczqmt0de6xsmre2pkxzm3qydmkzdjrdev9s7zhgfaqxqyjw5qcqpjrzjqt6xptnd85lpqnu2lefq4cx070v5cdwzh2xlvmdgnu7gqp4zvkus5zapryqqx9qqqyqqqqqqqqqqqcsq9q9qyysgqen77vu8xqjelum24hgjpgfdgfgx4q0nehhalcmuggt32japhjuksq9jv6eksjfnppm4hrzsgyxt8y8xacxut9qv3fpyetz8t7tsymygq8yzn05";
const ONCHAIN_ADDRESS: &str = "bc1p5uvtaxzkjwvey2tfy49k5vtqfpjmrgm09cvs88ezyy8h2zv7jhas9tu4yr";
const ORDER_ID: &str = "cc5c6e1b-9445-50e9-0574-01b7e524af8d";
const SCID: &str = "761432x100x0";

fn setup_test_lsps7_nodes<'a, 'b, 'c>(nodes: Vec<Node<'a, 'b, 'c>>) -> LSPSNodes<'a, 'b, 'c> {
	let service_config = LiquidityServiceConfig {
		lsps1_service_config: None,
		lsps2_service_config: None,
		lsps5_service_config: None,
		lsps7_service_config: None,
		advertise_service: false,
	};
	let client_config = LiquidityClientConfig {
		lsps1_client_config: None,
		lsps2_client_config: None,
		lsps5_client_config: None,
		lsps7_client_config: Some(LSPS7ClientConfig {}),
	};
	create_service_and_client_nodes(
		nodes,
		service_config,
		client_config,
		Arc::new(DefaultTimeProvider),
	)
}

/// Parses an outgoing JSON-RPC request, checks its method and id, and returns its params.
fn parse_request(
	message: RawLSPSMessage, expected_method: &str, expected_id: &LSPSRequestId,
) -> Value {
	let request: Value = serde_json::from_str(&message.payload).unwrap();
	assert_eq!(request["jsonrpc"], "2.0");
	assert_eq!(request["method"], expected_method);
	assert_eq!(request["id"], expected_id.0.as_str());
	request["params"].clone()
}

fn result_message(request_id: &LSPSRequestId, result: Value) -> RawLSPSMessage {
	let payload = json!({ "jsonrpc": "2.0", "id": request_id.0, "result": result });
	RawLSPSMessage { payload: payload.to_string() }
}

fn error_message(request_id: &LSPSRequestId, code: i32, message: &str) -> RawLSPSMessage {
	let payload = json!({
		"jsonrpc": "2.0",
		"id": request_id.0,
		"error": { "code": code, "message": message },
	});
	RawLSPSMessage { payload: payload.to_string() }
}

fn extendable_channel() -> Value {
	json!({
		"original_order": { "id": "bb4b5d0a-8334-49d8-9463-90a6d413af7c", "service": "LSPS1" },
		"short_channel_id": SCID,
		"max_channel_extension_expiry_blocks": 52560,
		"expiration_block": 850000,
	})
}

fn bolt11_payment(state: &str) -> Value {
	json!({
		"state": state,
		"expires_at": "2035-01-01T00:00:00Z",
		"fee_total_sat": "8888",
		"order_total_sat": "8888",
		"invoice": BOLT11_INVOICE,
	})
}

fn onchain_payment() -> Value {
	json!({
		"state": "EXPECT_PAYMENT",
		"expires_at": "2035-01-01T00:00:00Z",
		"fee_total_sat": "9999",
		"order_total_sat": "9999",
		"address": ONCHAIN_ADDRESS,
		"min_onchain_payment_confirmations": 1,
		"min_fee_for_0conf": 253,
	})
}

#[test]
fn get_extendable_channels_flow() {
	let chanmon_cfgs = create_chanmon_cfgs(2);
	let node_cfgs = create_node_cfgs(2, &chanmon_cfgs);
	let node_chanmgrs = create_node_chanmgrs(2, &node_cfgs, &[None, None]);
	let nodes = create_network(2, &node_cfgs, &node_chanmgrs);
	let LSPSNodes { service_node, client_node } = setup_test_lsps7_nodes(nodes);
	let service_node_id = service_node.inner.node.get_our_node_id();
	let client_handler = client_node.liquidity_manager.lsps7_client_handler().unwrap();

	let request_id = client_handler.request_extendable_channels(service_node_id);
	let request = get_lsps_message!(client_node, service_node_id);
	let params = parse_request(request, "lsps7.get_extendable_channels", &request_id);
	assert_eq!(params, json!({}));

	let lsps2_channel = json!({
		"short_channel_id": "761433x200x1",
		"max_channel_extension_expiry_blocks": 4320,
		"expiration_block": 860000,
	});
	let response = result_message(
		&request_id,
		json!({ "extendable_channels": [extendable_channel(), lsps2_channel] }),
	);
	client_node.liquidity_manager.handle_custom_message(response, service_node_id).unwrap();

	match client_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Client(LSPS7ClientEvent::ExtendableChannelsReady {
			request_id: event_request_id,
			counterparty_node_id,
			extendable_channels,
		}) => {
			assert_eq!(event_request_id, request_id);
			assert_eq!(counterparty_node_id, service_node_id);
			assert_eq!(extendable_channels.len(), 2);
			let original_order = extendable_channels[0].original_order.as_ref().unwrap();
			assert_eq!(original_order.service, "LSPS1");
			assert_eq!(extendable_channels[0].short_channel_id, SCID);
			assert!(extendable_channels[1].original_order.is_none());
			assert_eq!(extendable_channels[1].max_channel_extension_expiry_blocks, 4320);
		},
		event => panic!("Unexpected event: {:?}", event),
	}
	assert!(client_node.liquidity_manager.next_event().is_none());
}

#[test]
fn create_order_flow() {
	let chanmon_cfgs = create_chanmon_cfgs(2);
	let node_cfgs = create_node_cfgs(2, &chanmon_cfgs);
	let node_chanmgrs = create_node_chanmgrs(2, &node_cfgs, &[None, None]);
	let nodes = create_network(2, &node_cfgs, &node_chanmgrs);
	let LSPSNodes { service_node, client_node } = setup_test_lsps7_nodes(nodes);
	let service_node_id = service_node.inner.node.get_our_node_id();
	let client_handler = client_node.liquidity_manager.lsps7_client_handler().unwrap();

	let refund_address = Address::from_str(ONCHAIN_ADDRESS).unwrap().assume_checked();
	let request_id = client_handler.create_order(
		&service_node_id,
		SCID.to_string(),
		20160,
		Some("coupon".to_string()),
		Some(refund_address),
	);
	let request = get_lsps_message!(client_node, service_node_id);
	let params = parse_request(request, "lsps7.create_order", &request_id);
	assert_eq!(
		params,
		json!({
			"short_channel_id": SCID,
			"channel_extension_expiry_blocks": 20160,
			"token": "coupon",
			"refund_onchain_address": ONCHAIN_ADDRESS,
		})
	);

	let result = json!({
		"order_id": ORDER_ID,
		"channel_extension_expiry_blocks": 20160,
		"new_channel_expiry_block": 870160,
		"token": "coupon",
		"created_at": "2025-01-01T00:00:00Z",
		"order_state": "CREATED",
		"payment": {
			"bolt11": bolt11_payment("EXPECT_PAYMENT"),
			"onchain": onchain_payment(),
		},
		"channel": extendable_channel(),
	});
	let response = result_message(&request_id, result);
	client_node.liquidity_manager.handle_custom_message(response, service_node_id).unwrap();

	match client_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Client(LSPS7ClientEvent::OrderCreated {
			request_id: event_request_id,
			counterparty_node_id,
			order_id,
			order_state,
			channel_extension_expiry_blocks,
			new_channel_expiry_block,
			token,
			created_at,
			payment,
			channel,
		}) => {
			assert_eq!(event_request_id, request_id);
			assert_eq!(counterparty_node_id, service_node_id);
			assert_eq!(order_id, LSPS7OrderId(ORDER_ID.to_string()));
			assert_eq!(order_state, LSPS7OrderState::Created);
			assert_eq!(channel_extension_expiry_blocks, 20160);
			assert_eq!(new_channel_expiry_block, 870160);
			assert_eq!(token, "coupon");
			assert_eq!(created_at, LSPSDateTime::from_str("2025-01-01T00:00:00Z").unwrap());
			assert!(payment.bolt11.is_some());
			assert!(payment.bolt12.is_none());
			assert!(payment.onchain.is_some());
			assert_eq!(channel.short_channel_id, SCID);
		},
		event => panic!("Unexpected event: {:?}", event),
	}
	assert!(client_node.liquidity_manager.next_event().is_none());
}

#[test]
fn check_order_status_without_token() {
	let chanmon_cfgs = create_chanmon_cfgs(2);
	let node_cfgs = create_node_cfgs(2, &chanmon_cfgs);
	let node_chanmgrs = create_node_chanmgrs(2, &node_cfgs, &[None, None]);
	let nodes = create_network(2, &node_cfgs, &node_chanmgrs);
	let LSPSNodes { service_node, client_node } = setup_test_lsps7_nodes(nodes);
	let service_node_id = service_node.inner.node.get_our_node_id();
	let client_handler = client_node.liquidity_manager.lsps7_client_handler().unwrap();

	let request_id =
		client_handler.check_order_status(&service_node_id, LSPS7OrderId(ORDER_ID.to_string()));
	let request = get_lsps_message!(client_node, service_node_id);
	let params = parse_request(request, "lsps7.get_order", &request_id);
	assert_eq!(params, json!({ "order_id": ORDER_ID }));

	// The LSP omits `token` and offers only a BOLT 11 payment option.
	let result = json!({
		"order_id": ORDER_ID,
		"channel_extension_expiry_blocks": 20160,
		"new_channel_expiry_block": 870160,
		"created_at": "2025-01-01T00:00:00Z",
		"order_state": "COMPLETED",
		"payment": { "bolt11": bolt11_payment("PAID") },
		"channel": extendable_channel(),
	});
	let response = result_message(&request_id, result);
	client_node.liquidity_manager.handle_custom_message(response, service_node_id).unwrap();

	match client_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Client(LSPS7ClientEvent::OrderStatus {
			request_id: event_request_id,
			order_id,
			order_state,
			token,
			payment,
			..
		}) => {
			assert_eq!(event_request_id, request_id);
			assert_eq!(order_id, LSPS7OrderId(ORDER_ID.to_string()));
			assert_eq!(order_state, LSPS7OrderState::Completed);
			assert_eq!(token, "");
			assert!(payment.bolt11.is_some());
			assert!(payment.bolt12.is_none());
			assert!(payment.onchain.is_none());
		},
		event => panic!("Unexpected event: {:?}", event),
	}

	// The LSP must still be served after the response above.
	let request_id = client_handler.request_extendable_channels(service_node_id);
	let _ = get_lsps_message!(client_node, service_node_id);
	let response = result_message(&request_id, json!({ "extendable_channels": [] }));
	client_node.liquidity_manager.handle_custom_message(response, service_node_id).unwrap();
	match client_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Client(LSPS7ClientEvent::ExtendableChannelsReady {
			extendable_channels,
			..
		}) => assert!(extendable_channels.is_empty()),
		event => panic!("Unexpected event: {:?}", event),
	}
}

#[test]
fn error_responses() {
	let chanmon_cfgs = create_chanmon_cfgs(2);
	let node_cfgs = create_node_cfgs(2, &chanmon_cfgs);
	let node_chanmgrs = create_node_chanmgrs(2, &node_cfgs, &[None, None]);
	let nodes = create_network(2, &node_cfgs, &node_chanmgrs);
	let LSPSNodes { service_node, client_node } = setup_test_lsps7_nodes(nodes);
	let service_node_id = service_node.inner.node.get_our_node_id();
	let client_handler = client_node.liquidity_manager.lsps7_client_handler().unwrap();

	let request_id = client_handler.request_extendable_channels(service_node_id);
	let _ = get_lsps_message!(client_node, service_node_id);
	let response = error_message(&request_id, -32603, "Internal error");
	assert!(client_node
		.liquidity_manager
		.handle_custom_message(response, service_node_id)
		.is_err());
	match client_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Client(LSPS7ClientEvent::ExtendableChannelsRequestFailed {
			request_id: event_request_id,
			counterparty_node_id,
			error,
		}) => {
			assert_eq!(event_request_id, request_id);
			assert_eq!(counterparty_node_id, service_node_id);
			assert_eq!(error.code, -32603);
			assert_eq!(error.message, "Internal error");
		},
		event => panic!("Unexpected event: {:?}", event),
	}

	// Optional request fields are left out of the request when unset.
	let request_id =
		client_handler.create_order(&service_node_id, "1x2x3".to_string(), 20160, None, None);
	let request = get_lsps_message!(client_node, service_node_id);
	let params = parse_request(request, "lsps7.create_order", &request_id);
	assert_eq!(
		params,
		json!({ "short_channel_id": "1x2x3", "channel_extension_expiry_blocks": 20160 })
	);
	let response = error_message(&request_id, -32602, "Invalid params");
	assert!(client_node
		.liquidity_manager
		.handle_custom_message(response, service_node_id)
		.is_err());
	match client_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Client(LSPS7ClientEvent::OrderRequestFailed {
			request_id: event_request_id,
			error,
			..
		}) => {
			assert_eq!(event_request_id, request_id);
			assert_eq!(error.code, -32602);
		},
		event => panic!("Unexpected event: {:?}", event),
	}

	let request_id =
		client_handler.check_order_status(&service_node_id, LSPS7OrderId(ORDER_ID.to_string()));
	let _ = get_lsps_message!(client_node, service_node_id);
	let response = error_message(&request_id, 101, "Order not found");
	assert!(client_node
		.liquidity_manager
		.handle_custom_message(response, service_node_id)
		.is_err());
	match client_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Client(LSPS7ClientEvent::OrderRequestFailed {
			request_id: event_request_id,
			error,
			..
		}) => {
			assert_eq!(event_request_id, request_id);
			assert_eq!(error.code, 101);
		},
		event => panic!("Unexpected event: {:?}", event),
	}
	assert!(client_node.liquidity_manager.next_event().is_none());
}

fn setup_lsps7_service_nodes<'a, 'b, 'c>(
	nodes: Vec<Node<'a, 'b, 'c>>, service_kv_store: Arc<TestStore>,
) -> LSPSNodes<'a, 'b, 'c> {
	let (service_config, client_config) = lsps7_service_configs();
	create_service_and_client_nodes_with_kv_stores(
		nodes,
		service_config,
		client_config,
		Arc::new(DefaultTimeProvider),
		service_kv_store,
		Arc::new(TestStore::new(false)),
	)
}

fn lsps7_service_configs() -> (LiquidityServiceConfig, LiquidityClientConfig) {
	let service_config = LiquidityServiceConfig {
		lsps1_service_config: None,
		lsps2_service_config: None,
		lsps5_service_config: None,
		lsps7_service_config: Some(LSPS7ServiceConfig::default()),
		advertise_service: true,
	};
	let client_config = LiquidityClientConfig {
		lsps1_client_config: None,
		lsps2_client_config: None,
		lsps5_client_config: None,
		lsps7_client_config: Some(LSPS7ClientConfig {}),
	};
	(service_config, client_config)
}

fn leased_channel() -> LSPS7ExtendableChannel {
	serde_json::from_value(extendable_channel()).unwrap()
}

fn bolt11_payment_info() -> LSPS1PaymentInfo {
	serde_json::from_value(json!({ "bolt11": bolt11_payment("EXPECT_PAYMENT") })).unwrap()
}

/// Client asks for extendable channels and creates an order; the service answers both.
/// Returns the order id the service assigned.
fn order_extension(nodes: &LSPSNodes, blocks: u32) -> LSPS7OrderId {
	let LSPSNodes { service_node, client_node } = nodes;
	let service_node_id = service_node.inner.node.get_our_node_id();
	let client_node_id = client_node.inner.node.get_our_node_id();
	let client_handler = client_node.liquidity_manager.lsps7_client_handler().unwrap();
	let service_handler = service_node.liquidity_manager.lsps7_service_handler().unwrap();

	client_handler.request_extendable_channels(service_node_id);
	let request = get_lsps_message!(client_node, service_node_id);
	service_node.liquidity_manager.handle_custom_message(request, client_node_id).unwrap();
	let request_id = match service_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Service(LSPS7ServiceEvent::GetExtendableChannels {
			request_id,
			counterparty_node_id,
		}) => {
			assert_eq!(counterparty_node_id, client_node_id);
			request_id
		},
		e => panic!("Unexpected event: {:?}", e),
	};
	service_handler
		.extendable_channels(request_id, client_node_id, vec![leased_channel()])
		.unwrap();
	let response = get_lsps_message!(service_node, client_node_id);
	client_node.liquidity_manager.handle_custom_message(response, service_node_id).unwrap();
	match client_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Client(LSPS7ClientEvent::ExtendableChannelsReady {
			extendable_channels,
			..
		}) => assert_eq!(extendable_channels, vec![leased_channel()]),
		e => panic!("Unexpected event: {:?}", e),
	}

	client_handler.create_order(&service_node_id, SCID.to_string(), blocks, None, None);
	let request = get_lsps_message!(client_node, service_node_id);
	service_node.liquidity_manager.handle_custom_message(request, client_node_id).unwrap();
	let request_id = match service_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Service(LSPS7ServiceEvent::RequestForPaymentDetails {
			request_id,
			request,
			..
		}) => {
			assert_eq!(request.short_channel_id, SCID);
			assert_eq!(request.channel_extension_expiry_blocks, blocks);
			request_id
		},
		e => panic!("Unexpected event: {:?}", e),
	};
	let order_id = service_handler
		.send_payment_details(request_id, client_node_id, leased_channel(), bolt11_payment_info())
		.unwrap();
	let response = get_lsps_message!(service_node, client_node_id);
	client_node.liquidity_manager.handle_custom_message(response, service_node_id).unwrap();
	match client_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Client(LSPS7ClientEvent::OrderCreated {
			order_id: client_order_id,
			order_state,
			new_channel_expiry_block,
			..
		}) => {
			assert_eq!(client_order_id, order_id);
			assert_eq!(order_state, LSPS7OrderState::Created);
			assert_eq!(new_channel_expiry_block, 850000 + blocks);
		},
		e => panic!("Unexpected event: {:?}", e),
	}
	order_id
}

/// The client's view of an order, fetched with `get_order`.
fn client_order_status(nodes: &LSPSNodes, order_id: &LSPS7OrderId) -> LSPS7ClientEvent {
	let LSPSNodes { service_node, client_node } = nodes;
	let service_node_id = service_node.inner.node.get_our_node_id();
	let client_node_id = client_node.inner.node.get_our_node_id();
	let client_handler = client_node.liquidity_manager.lsps7_client_handler().unwrap();
	client_handler.check_order_status(&service_node_id, order_id.clone());
	let request = get_lsps_message!(client_node, service_node_id);
	let _ = service_node.liquidity_manager.handle_custom_message(request, client_node_id);
	let response = get_lsps_message!(service_node, client_node_id);
	let _ = client_node.liquidity_manager.handle_custom_message(response, service_node_id);
	match client_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Client(event) => event,
		e => panic!("Unexpected event: {:?}", e),
	}
}

#[test]
fn service_extension_flow() {
	let chanmon_cfgs = create_chanmon_cfgs(2);
	let node_cfgs = create_node_cfgs(2, &chanmon_cfgs);
	let node_chanmgrs = create_node_chanmgrs(2, &node_cfgs, &[None, None]);
	let nodes = create_network(2, &node_cfgs, &node_chanmgrs);
	let lsps_nodes = setup_lsps7_service_nodes(nodes, Arc::new(TestStore::new(false)));
	let client_node_id = lsps_nodes.client_node.inner.node.get_our_node_id();
	let service_handler =
		lsps_nodes.service_node.liquidity_manager.lsps7_service_handler().unwrap();

	let order_id = order_extension(&lsps_nodes, 4320);

	service_handler
		.order_payment_received(client_node_id, order_id.clone(), PaymentMethod::Bolt11)
		.unwrap();
	match client_order_status(&lsps_nodes, &order_id) {
		LSPS7ClientEvent::OrderStatus { order_state, payment, channel, .. } => {
			assert_eq!(order_state, LSPS7OrderState::Created);
			assert_eq!(payment.bolt11.unwrap().state, LSPS1PaymentState::Hold);
			assert_eq!(channel.expiration_block, 850000);
		},
		e => panic!("Unexpected event: {:?}", e),
	}

	service_handler.order_extension_completed(client_node_id, order_id.clone()).unwrap();
	match client_order_status(&lsps_nodes, &order_id) {
		LSPS7ClientEvent::OrderStatus { order_state, payment, channel, .. } => {
			assert_eq!(order_state, LSPS7OrderState::Completed);
			assert_eq!(payment.bolt11.unwrap().state, LSPS1PaymentState::Paid);
			assert_eq!(channel.expiration_block, 854320);
			assert_eq!(channel.extension_order_ids, Some(vec![order_id.0.clone()]));
		},
		e => panic!("Unexpected event: {:?}", e),
	}

	let (owner, order) = service_handler.get_order(&order_id).unwrap();
	assert_eq!(owner, client_node_id);
	assert_eq!(order.order_state, LSPS7OrderState::Completed);
	assert_eq!(service_handler.list_orders().len(), 1);

	// A completed order cannot fail anymore.
	assert!(service_handler.order_failed_and_refunded(client_node_id, order_id).is_err());
}

#[test]
fn service_rejections() {
	let chanmon_cfgs = create_chanmon_cfgs(2);
	let node_cfgs = create_node_cfgs(2, &chanmon_cfgs);
	let node_chanmgrs = create_node_chanmgrs(2, &node_cfgs, &[None, None]);
	let nodes = create_network(2, &node_cfgs, &node_chanmgrs);
	let lsps_nodes = setup_lsps7_service_nodes(nodes, Arc::new(TestStore::new(false)));
	let LSPSNodes { service_node, client_node } = &lsps_nodes;
	let service_node_id = service_node.inner.node.get_our_node_id();
	let client_node_id = client_node.inner.node.get_our_node_id();
	let client_handler = client_node.liquidity_manager.lsps7_client_handler().unwrap();
	let service_handler = service_node.liquidity_manager.lsps7_service_handler().unwrap();

	client_handler.create_order(&service_node_id, SCID.to_string(), 60000, None, None);
	let request = get_lsps_message!(client_node, service_node_id);
	service_node.liquidity_manager.handle_custom_message(request, client_node_id).unwrap();
	let request_id = match service_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Service(LSPS7ServiceEvent::RequestForPaymentDetails {
			request_id,
			..
		}) => request_id,
		e => panic!("Unexpected event: {:?}", e),
	};

	// Longer than the channel allows: the service refuses to accept it...
	assert!(service_handler
		.send_payment_details(
			request_id.clone(),
			client_node_id,
			leased_channel(),
			bolt11_payment_info()
		)
		.is_err());
	// ...and the request stays pending, so the LSP can turn it down.
	service_handler
		.reject_order(
			request_id,
			client_node_id,
			LSPSResponseError {
				code: LSPS7_CREATE_ORDER_REQUEST_OPTION_MISMATCH_ERROR_CODE,
				message: "extension too long".to_string(),
				data: None,
			},
		)
		.unwrap();
	let response = get_lsps_message!(service_node, client_node_id);
	// The client handler reports an error response as an Err as well as an event.
	let _ = client_node.liquidity_manager.handle_custom_message(response, service_node_id);
	match client_node.liquidity_manager.next_event().unwrap() {
		LiquidityEvent::LSPS7Client(LSPS7ClientEvent::OrderRequestFailed { error, .. }) => {
			assert_eq!(error.code, LSPS7_CREATE_ORDER_REQUEST_OPTION_MISMATCH_ERROR_CODE)
		},
		e => panic!("Unexpected event: {:?}", e),
	}

	// Unknown orders are not found.
	match client_order_status(&lsps_nodes, &LSPS7OrderId(ORDER_ID.to_string())) {
		LSPS7ClientEvent::OrderRequestFailed { error, .. } => {
			assert_eq!(error.code, LSPS7_GET_ORDER_REQUEST_ORDER_NOT_FOUND_ERROR_CODE)
		},
		e => panic!("Unexpected event: {:?}", e),
	}
	assert!(service_handler.list_orders().is_empty());
}

#[test]
fn service_orders_survive_a_restart() {
	let service_kv_store = Arc::new(TestStore::new(false));
	let order_id;
	let client_node_id;
	{
		let chanmon_cfgs = create_chanmon_cfgs(2);
		let node_cfgs = create_node_cfgs(2, &chanmon_cfgs);
		let node_chanmgrs = create_node_chanmgrs(2, &node_cfgs, &[None, None]);
		let nodes = create_network(2, &node_cfgs, &node_chanmgrs);
		let lsps_nodes = setup_lsps7_service_nodes(nodes, Arc::clone(&service_kv_store));
		client_node_id = lsps_nodes.client_node.inner.node.get_our_node_id();
		order_id = order_extension(&lsps_nodes, 144);
		lsps_nodes
			.service_node
			.liquidity_manager
			.lsps7_service_handler()
			.unwrap()
			.order_payment_received(client_node_id, order_id.clone(), PaymentMethod::Bolt11)
			.unwrap();
		lsps_nodes.service_node.liquidity_manager.persist().unwrap();
	}

	let chanmon_cfgs = create_chanmon_cfgs(2);
	let node_cfgs = create_node_cfgs(2, &chanmon_cfgs);
	let node_chanmgrs = create_node_chanmgrs(2, &node_cfgs, &[None, None]);
	let nodes = create_network(2, &node_cfgs, &node_chanmgrs);
	let (service_config, _) = lsps7_service_configs();
	let restarted = LiquidityManagerSync::new_with_custom_time_provider(
		nodes[0].keys_manager,
		nodes[0].keys_manager,
		nodes[0].node,
		service_kv_store,
		Arc::new(TestBroadcaster::new(Network::Testnet)),
		Some(service_config),
		None,
		Arc::new(DefaultTimeProvider),
	)
	.unwrap();
	let service_handler = restarted.lsps7_service_handler().unwrap();
	let (owner, order) = service_handler.get_order(&order_id).unwrap();
	assert_eq!(owner, client_node_id);
	assert_eq!(order.payment.bolt11.unwrap().state, LSPS1PaymentState::Hold);
	assert_eq!(order.new_channel_expiry_block, 850144);
	service_handler.order_extension_completed(client_node_id, order_id).unwrap();
}
