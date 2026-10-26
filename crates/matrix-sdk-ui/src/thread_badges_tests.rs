// Copyright 2026 The Matrix.org Foundation C.I.C.
// Licensed under the Apache License, Version 2.0.

use matrix_sdk::test_utils::mocks::MatrixMockServer;
use matrix_sdk_test::{async_test, event_factory::EventFactory};
use ruma::{event_id, room_id, user_id};
use serde_json::json;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path_regex},
};

#[async_test]
async fn test_badge_uses_later_public_or_private_receipt_for_both_preferences() {
    use ruma::events::receipt::{ReceiptThread, ReceiptType};
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;
    let id = room_id!("!mixed:example.org");
    let room = server.sync_joined_room(&client, id).await;
    let own = room.own_user_id().to_owned();
    let f = EventFactory::new().room(id).sender(user_id!("@sender:example.org"));
    let root = event_id!("$root");
    let first = event_id!("$first");
    let second = event_id!("$second");
    server
        .mock_room_threads()
        .ok(vec![f.text_msg("root").event_id(root).into_raw()], None)
        .mount()
        .await;
    Mock::given(method("GET")).and(path_regex("/relations/.*"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"chunk": [
            {"event_id":first,"sender":"@sender:example.org","origin_server_ts":100,"type":"m.room.message","content":{"msgtype":"m.text","body":"one"}},
            {"event_id":second,"sender":"@sender:example.org","origin_server_ts":1,"type":"m.room.message","content":{"msgtype":"m.text","body":"two"}}
        ]}))).mount(server.server()).await;
    for (public, private) in [(first, second), (second, first)] {
        server
            .sync_room(
                &client,
                matrix_sdk_test::JoinedRoomBuilder::new(id).add_receipt(
                    f.read_receipts()
                        .add(
                            public,
                            own.clone(),
                            ReceiptType::Read,
                            ReceiptThread::Thread(root.to_owned()),
                        )
                        .add(
                            private,
                            own.clone(),
                            ReceiptType::ReadPrivate,
                            ReceiptThread::Thread(root.to_owned()),
                        )
                        .into_event(),
                ),
            )
            .await;
        for preference in [false, true] {
            let badges = super::thread_badges::load(&client, Some(vec![id.to_owned()]), preference)
                .await
                .unwrap();
            assert_eq!(badges[0].unread, 0);
            assert_eq!(badges[0].mentions, 0);
        }
    }
}

#[async_test]
async fn test_compact_badge_counts_incoming_mentions_without_ui_timeline() {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;
    let room_id = room_id!("!quiet:example.org");
    let room = server.sync_joined_room(&client, room_id).await;
    let own = room.own_user_id().to_owned();
    let factory = EventFactory::new().room(room_id).sender(user_id!("@sender:example.org"));
    server
        .mock_room_threads()
        .ok(vec![factory.text_msg("root").event_id(event_id!("$root")).into_raw()], None)
        .mount()
        .await;
    Mock::given(method("GET")).and(path_regex("/relations/.*"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"chunk": [{
            "event_id": "$reply", "sender": "@sender:example.org", "origin_server_ts": 1,
            "type": "m.room.message", "content": {"msgtype": "m.text", "body": "reply",
                "m.mentions": {"user_ids": [own]}, "m.relates_to": {"rel_type": "m.thread", "event_id": "$root"}}
        }]}))).mount(server.server()).await;
    let badges =
        super::thread_badges::load(&client, Some(vec![room_id.to_owned()]), false).await.unwrap();
    assert_eq!(badges.len(), 1);
    assert_eq!(badges[0].unread, 1);
    assert_eq!(badges[0].mentions, 1);
    assert!(!badges[0].participating);
}
