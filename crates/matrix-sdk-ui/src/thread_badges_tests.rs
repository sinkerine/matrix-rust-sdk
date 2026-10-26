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
