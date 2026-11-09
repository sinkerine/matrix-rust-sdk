// Copyright 2023 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures_util::StreamExt as _;
use matrix_sdk::{
    ThreadingSupport, assert_let_timeout,
    test_utils::mocks::{MatrixMockServer, RoomRelationsResponseTemplate},
};
use matrix_sdk_test::{JoinedRoomBuilder, User, async_test, event_factory::EventFactory};
use matrix_sdk_ui::timeline::{TimelineBuilder, TimelineFocus};
use ruma::{event_id, events::room::message::RoomMessageEventContentWithoutRelation, room_id};

#[async_test]
async fn test_thread_cached_older_page_latest_bundled_edit_beats_older_room_edit() {
    let server = MatrixMockServer::new().await;
    let directory = tempfile::tempdir().unwrap();
    let room_id = room_id!("!thread-cache:localhost");
    let root = event_id!("$root");
    let original_id = event_id!("$original");
    let edit_id = event_id!("$edit");
    let f = EventFactory::new().room(room_id).sender(User::Alice);
    let edit = f
        .text_msg("* edited reply")
        .event_id(edit_id)
        .server_ts(2000)
        .edit(original_id, RoomMessageEventContentWithoutRelation::text_plain("edited reply"))
        .into_raw();

    let build_client = || {
        server
            .client_builder()
            .on_builder(|builder| {
                builder
                    .sqlite_store(directory.path(), None)
                    .with_threading_support(ThreadingSupport::Enabled { with_subscriptions: false })
            })
            .build()
    };

    {
        let client = build_client().await;
        let mut room_builder = JoinedRoomBuilder::new(room_id);
        room_builder = room_builder.add_timeline_event(
            f.text_msg("* older edit")
                .event_id(event_id!("$older_edit"))
                .server_ts(1000)
                .edit(original_id, RoomMessageEventContentWithoutRelation::text_plain("older edit"))
                .into_raw(),
        );
        let room = server.sync_room(&client, room_builder).await;

        server
            .mock_room_event()
            .match_event_id()
            .ok(f.text_msg("Thread root").event_id(root).into())
            .mock_once()
            .mount()
            .await;
        server
            .mock_room_relations()
            .match_target_event(root.to_owned())
            .ok(RoomRelationsResponseTemplate::default()
                .events(vec![
                    f.text_msg("Newest reply")
                        .event_id(event_id!("$newest"))
                        .in_thread(root, original_id)
                        .into_raw(),
                ])
                .next_batch("older"))
            .mock_once()
            .mount()
            .await;
        server
            .mock_room_relations()
            .match_target_event(root.to_owned())
            .match_from("older")
            .ok(RoomRelationsResponseTemplate::default().events(vec![
                f.text_msg("original reply")
                    .event_id(original_id)
                    .in_thread(root, root)
                    .with_bundled_edit(edit)
                    .into_raw(),
            ]))
            .mock_once()
            .mount()
            .await;

        let timeline = TimelineBuilder::new(&room)
            .with_focus(TimelineFocus::Thread { thread_id: root.to_owned() })
            .build()
            .await
            .unwrap();
        let (_, mut updates) = timeline.subscribe().await;
        assert!(!timeline.paginate_backwards(10).await.unwrap());
        assert!(timeline.paginate_backwards(10).await.unwrap());

        loop {
            let items = timeline.items().await;
            if let Some(item) = items
                .iter()
                .filter_map(|item| item.as_event())
                .find(|item| item.event_id() == Some(original_id))
            {
                let message = item.content().as_message().unwrap();
                assert_eq!(message.body(), "edited reply");
                assert!(message.is_edited());
                break;
            }
            assert_let_timeout!(Some(_) = updates.next());
        }

        let (cache, _guards) = client.event_cache().unwrap().thread(room_id, root).await.unwrap();
        let (events, _) = cache.subscribe().await.unwrap();
        assert!(!events.iter().any(|event| event.event_id() == Some(edit_id)));
        let (_, related) =
            cache.find_event_with_relations(original_id, None).await.unwrap().unwrap();
        assert!(related.iter().any(|event| event.event_id() == Some(edit_id)));
        assert!(related.iter().any(|event| event.event_id() == Some(event_id!("$older_edit"))));
    }

    server.verify_and_reset().await;
    server
        .mock_room_relations()
        .match_target_event(root.to_owned())
        .ok(RoomRelationsResponseTemplate::default())
        .never()
        .mount()
        .await;
    server
        .mock_room_event()
        .ok(f.text_msg("Thread root").event_id(root).into())
        .never()
        .mount()
        .await;

    let client = build_client().await;
    let room = client.get_room(room_id).unwrap();
    let timeline = TimelineBuilder::new(&room)
        .with_focus(TimelineFocus::Thread { thread_id: root.to_owned() })
        .build()
        .await
        .unwrap();
    let (initial, mut updates) = timeline.subscribe().await;
    assert_eq!(initial.iter().filter_map(|item| item.as_event()).count(), 1);
    assert_eq!(
        initial.iter().find_map(|item| item.as_event()).unwrap().event_id(),
        Some(event_id!("$newest"))
    );

    assert!(timeline.paginate_backwards(10).await.unwrap());
    let (cache, _guards) = client.event_cache().unwrap().thread(room_id, root).await.unwrap();
    let (_, related) = cache.find_event_with_relations(original_id, None).await.unwrap().unwrap();
    assert_eq!(related.iter().filter(|event| event.event_id() == Some(edit_id)).count(), 1);
    loop {
        let items = timeline.items().await;
        if let Some(item) = items
            .iter()
            .filter_map(|item| item.as_event())
            .find(|item| item.event_id() == Some(original_id))
        {
            let message = item.content().as_message().unwrap();
            if message.is_edited() {
                assert_eq!(message.body(), "edited reply");
                assert_eq!(items.iter().filter_map(|item| item.as_event()).count(), 3);
                assert!(
                    !items
                        .iter()
                        .filter_map(|item| item.as_event())
                        .any(|item| item.event_id() == Some(edit_id))
                );
                break;
            }
        }
        assert_let_timeout!(Some(_) = updates.next());
    }
}
