// Copyright 2026 The Matrix.org Foundation C.I.C.
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

//! Account-wide thread discovery. A snapshot is complete only after every joined
//! room and every page has been loaded successfully. Concurrency bounds work in
//! flight, never the set of rooms or threads included in the result.

use std::collections::{HashMap, HashSet};

use futures_util::{StreamExt, stream};
use matrix_sdk::{Client, Room, room::ListThreadsOptions};
use ruma::OwnedRoomId;

use crate::timeline::thread_list_service::{ThreadListItem, ThreadListService};

/// A room's complete thread list, including rooms with no threads.
#[derive(Clone, Debug)]
pub struct InboxRoom {
    /// The joined room ID.
    pub room_id: OwnedRoomId,
    /// All thread roots visible to the current account in this room.
    pub threads: Vec<ThreadListItem>,
}

/// Load a complete account snapshot. No room-list ranking or unread heuristic
/// may be used here: a quiet room can contain an unread thread.
pub async fn load(client: &Client) -> matrix_sdk::Result<Vec<InboxRoom>> {
    load_with_progress(client, None, |_| {}).await
}

/// Emit each server page as soon as it is mapped. Completion of this future,
/// rather than a page callback, indicates a complete account snapshot.
pub async fn load_with_progress(
    client: &Client,
    ids: Option<Vec<OwnedRoomId>>,
    progress: impl Fn(InboxRoom) + Send + Sync,
) -> matrix_sdk::Result<Vec<InboxRoom>> {
    let ids = match ids {
        Some(ids) => ids,
        None => {
            client
                .send(ruma::api::client::membership::joined_rooms::v3::Request::new())
                .await?
                .joined_rooms
        }
    };
    let rooms = ids
        .into_iter()
        .map(|id| {
            client
                .get_room(&id)
                .filter(|room| room.state() == matrix_sdk::RoomState::Joined)
                .ok_or(matrix_sdk::Error::InsufficientData)
        })
        .collect::<matrix_sdk::Result<Vec<_>>>()?;
    load_rooms_with_progress(rooms, progress).await
}

/// Reload changed rooms with bounded network concurrency.
pub async fn load_rooms(rooms: Vec<Room>) -> matrix_sdk::Result<Vec<InboxRoom>> {
    load_rooms_with_progress(rooms, |_| {}).await
}

/// Stream independent room pages without waiting for slower rooms.
pub async fn load_rooms_with_progress(
    rooms: Vec<Room>,
    progress: impl Fn(InboxRoom) + Send + Sync,
) -> matrix_sdk::Result<Vec<InboxRoom>> {
    let mut pending =
        stream::iter(rooms.into_iter().map(|room| load_room(room, &progress))).buffer_unordered(4);
    let mut result = Vec::new();
    while let Some(room) = pending.next().await {
        result.push(room?);
    }
    result.sort_by(|a, b| a.room_id.cmp(&b.room_id));
    Ok(result)
}

async fn load_room(
    room: Room,
    progress: &(impl Fn(InboxRoom) + Send + Sync),
) -> matrix_sdk::Result<InboxRoom> {
    let mut from = None;
    let mut tokens = HashSet::new();
    let mut threads = HashMap::new();
    loop {
        let page = room
            .list_threads(ListThreadsOptions {
                from,
                limit: Some(20u32.into()),
                ..Default::default()
            })
            .await?;
        let mut batch = Vec::new();
        for event in page.chunk {
            // A malformed root must fail the snapshot rather than silently
            // lower the account-wide badge count.
            let item = ThreadListService::build_thread_list_item(&room, event)
                .await
                .ok_or(matrix_sdk::Error::InsufficientData)?;
            if !threads.contains_key(&item.root_event.event_id) {
                batch.push(item.clone());
            }
            threads.insert(item.root_event.event_id.clone(), item);
        }
        progress(InboxRoom { room_id: room.room_id().to_owned(), threads: batch });
        match page.prev_batch_token {
            Some(token) if tokens.insert(token.clone()) => from = Some(token),
            Some(_) => return Err(matrix_sdk::Error::InsufficientData),
            None => break,
        }
    }
    let mut threads: Vec<_> = threads.into_values().collect();
    threads.sort_by_key(|item| {
        std::cmp::Reverse(item.latest_event.as_ref().unwrap_or(&item.root_event).timestamp)
    });
    Ok(InboxRoom { room_id: room.room_id().to_owned(), threads })
}

#[cfg(test)]
mod tests {
    use matrix_sdk::test_utils::mocks::MatrixMockServer;
    use matrix_sdk_test::{async_test, event_factory::EventFactory};
    use ruma::{event_id, room_id, user_id};

    use super::load_rooms;

    #[async_test]
    async fn test_exhausts_pages_and_deduplicates_roots() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let id = room_id!("!quiet:example.org");
        let f = EventFactory::new().room(id).sender(user_id!("@alice:example.org"));
        let first = f.text_msg("root").event_id(event_id!("$one")).into_raw();
        server
            .mock_room_threads()
            .ok(vec![first.clone()], Some("page2".into()))
            .mock_once()
            .mount()
            .await;
        server
            .mock_room_threads()
            .match_from("page2")
            .ok(
                vec![first, f.text_msg("another root").event_id(event_id!("$two")).into_raw()],
                None,
            )
            .mock_once()
            .mount()
            .await;
        server.sync_joined_room(&client, id).await;
        let snapshot = load_rooms(client.joined_rooms()).await.unwrap();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].threads.len(), 2);
    }

    #[async_test]
    async fn test_emits_first_page_before_later_page_failure() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let id = room_id!("!quiet:example.org");
        let f = EventFactory::new().room(id).sender(user_id!("@alice:example.org"));
        server
            .mock_room_threads()
            .ok(
                vec![f.text_msg("root").event_id(event_id!("$one")).into_raw()],
                Some("page2".into()),
            )
            .mock_once()
            .mount()
            .await;
        server
            .mock_room_threads()
            .match_from("page2")
            .ok(vec![], Some("page2".into()))
            .mount()
            .await;
        server.sync_joined_room(&client, id).await;
        let pages = std::sync::Mutex::new(Vec::new());
        let result = super::load_rooms_with_progress(client.joined_rooms(), |page| {
            pages.lock().unwrap().push(page);
        })
        .await;
        assert!(result.is_err());
        let pages = pages.lock().unwrap();
        assert_eq!(pages[0].threads.len(), 1);
        assert_eq!(pages[0].threads[0].root_event.event_id, event_id!("$one"));
    }

    #[async_test]
    async fn test_includes_more_than_twenty_quiet_rooms() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        for number in 0..25 {
            let id = ruma::RoomId::parse(format!("!quiet{number}:example.org")).unwrap();
            server.sync_joined_room(&client, &id).await;
        }
        server.mock_room_threads().ok(vec![], None).mount().await;
        let snapshot = load_rooms(client.joined_rooms()).await.unwrap();
        assert_eq!(snapshot.len(), 25);
        assert!(snapshot.iter().all(|room| room.threads.is_empty()));
    }

    #[async_test]
    async fn test_server_membership_cannot_be_silently_truncated_to_local_rooms() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/_matrix/client/v3/joined_rooms"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"joined_rooms": ["!not-yet-synced:example.org"]}),
            ))
            .mount(server.server())
            .await;
        assert!(super::load(&client).await.is_err());
    }

    #[async_test]
    async fn test_repeated_page_token_fails_instead_of_publishing_partial_totals() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        server.sync_joined_room(&client, room_id!("!quiet:example.org")).await;
        server.mock_room_threads().ok(vec![], Some("loop".into())).mount().await;
        assert!(load_rooms(client.joined_rooms()).await.is_err());
    }
}
