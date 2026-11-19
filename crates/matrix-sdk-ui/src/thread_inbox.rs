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

//! Account-wide thread discovery. A snapshot is complete only after every
//! joined room and every page has been loaded successfully. Concurrency bounds
//! work in flight, never the set of rooms or threads included in the result.

use std::collections::{HashMap, HashSet};

use futures_util::{StreamExt, stream};
use matrix_sdk::{
    Client, Room,
    deserialized_responses::TimelineEvent,
    room::{IncludeRelations, ListThreadsOptions, RelationsOptions},
};
use matrix_sdk_common::serde_helpers::extract_bundled_thread;
use ruma::{
    MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId,
    api::{Direction, client::threads::get_threads::v1::IncludeThreads},
    events::relation::RelationType,
};

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

/// A selected thread root and server-provided participation from its bundled
/// summary.
#[derive(Clone, Debug)]
pub struct InboxPageThread {
    /// The mapped root and bundled latest reply.
    pub item: ThreadListItem,
    /// `current_user_participated` from the server's bundled thread summary.
    pub participated: bool,
}

/// The selected threads of one room in an [`InboxPage`].
#[derive(Clone, Debug)]
pub struct InboxPageRoom {
    /// The joined room ID.
    pub room_id: OwnedRoomId,
    /// Selected threads, newest reply first.
    pub threads: Vec<InboxPageThread>,
}

/// One account page ordered by latest reply time, with resumable per-room
/// cursors.
pub struct InboxPage {
    pub rooms: Vec<InboxPageRoom>,
    pub has_more: bool,
}

/// An unmapped root waiting in a room buffer. Mapping (profiles, content) is
/// deferred until the root is selected for a page.
#[derive(Clone)]
struct Candidate {
    event: TimelineEvent,
    timestamp: MilliSecondsSinceUnixEpoch,
    participated: bool,
}

impl Candidate {
    fn new(event: TimelineEvent) -> Option<Self> {
        event.event_id()?;
        let now = MilliSecondsSinceUnixEpoch::now();
        let bundled = extract_bundled_thread(event.raw());
        // Same ordering key as `ThreadListService::build_event`: the reply's
        // own timestamp, clamped to now, falling back to the root.
        let timestamp = bundled
            .as_ref()
            .and_then(|thread| {
                thread
                    .latest_event
                    .get_field::<MilliSecondsSinceUnixEpoch>("origin_server_ts")
                    .ok()
                    .flatten()
            })
            .or_else(|| event.raw().get_field("origin_server_ts").ok().flatten())?
            .min(now);
        let participated = bundled.is_some_and(|thread| thread.current_user_participated);
        Some(Self { event, timestamp, participated })
    }
}

#[derive(Clone)]
struct RoomCursor {
    room: Room,
    from: Option<String>,
    exhausted: bool,
    tokens: HashSet<String>,
    buffered: Vec<Candidate>,
}

/// Keeps server cursors and unread roots between user-requested pages. It never
/// loads reply histories and does no work until `next_page` is called.
pub struct InboxPager {
    client: Client,
    include: IncludeThreads,
    cursors: Option<Vec<RoomCursor>>,
    seen: HashSet<(OwnedRoomId, OwnedEventId)>,
}

impl InboxPager {
    pub fn new(client: Client) -> Self {
        Self::with_include(client, IncludeThreads::All)
    }

    /// Page only threads the user participated in, filtered by the homeserver
    /// so every page holds up to `limit` such threads.
    pub fn with_include(client: Client, include: IncludeThreads) -> Self {
        Self { client, include, cursors: None, seen: HashSet::new() }
    }

    pub async fn next_page(&mut self, limit: usize) -> matrix_sdk::Result<InboxPage> {
        let mut cursors = match &self.cursors {
            Some(cursors) => cursors.clone(),
            None => {
                let ids = self
                    .client
                    .send(ruma::api::client::membership::joined_rooms::v3::Request::new())
                    .await?
                    .joined_rooms;
                ids.into_iter()
                    .map(|id| {
                        let room = self
                            .client
                            .get_room(&id)
                            .filter(|r| r.state() == matrix_sdk::RoomState::Joined)
                            .ok_or(matrix_sdk::Error::InsufficientData)?;
                        Ok(RoomCursor {
                            room,
                            from: None,
                            exhausted: false,
                            tokens: HashSet::new(),
                            buffered: Vec::new(),
                        })
                    })
                    .collect::<matrix_sdk::Result<Vec<_>>>()?
            }
        };
        let mut seen = self.seen.clone();
        let mut selected: Vec<(usize, Candidate)> = Vec::new();
        while selected.len() < limit {
            // To choose the globally newest head, each non-exhausted room needs
            // a head. Fetch only empty buffers, preserving all other cursors.
            let needed: Vec<_> = cursors
                .iter()
                .enumerate()
                .filter(|(_, c)| c.buffered.is_empty() && !c.exhausted)
                .map(|(index, cursor)| (index, cursor.clone()))
                .collect();
            let include = self.include.clone();
            let mut pending = stream::iter(needed.into_iter().map(|(index, mut cursor)| {
                let include = include.clone();
                async move {
                    while cursor.buffered.is_empty() && !cursor.exhausted {
                        let page = cursor
                            .room
                            .list_threads(ListThreadsOptions {
                                include_threads: include.clone(),
                                from: cursor.from.clone(),
                                limit: Some(20u32.into()),
                            })
                            .await?;
                        for event in page.chunk {
                            cursor.buffered.push(
                                Candidate::new(event).ok_or(matrix_sdk::Error::InsufficientData)?,
                            );
                        }
                        cursor.buffered.sort_by_key(|c| std::cmp::Reverse(c.timestamp));
                        match page.prev_batch_token {
                            Some(token) if cursor.tokens.insert(token.clone()) => {
                                cursor.from = Some(token)
                            }
                            Some(_) => return Err(matrix_sdk::Error::InsufficientData),
                            None => cursor.exhausted = true,
                        }
                    }
                    Ok((index, cursor))
                }
            }))
            .buffer_unordered(8);
            while let Some(result) = pending.next().await {
                let (index, cursor) = result?;
                cursors[index] = cursor;
            }
            drop(pending);
            let Some(index) = cursors
                .iter()
                .enumerate()
                .filter(|(_, c)| !c.buffered.is_empty())
                .max_by_key(|(_, c)| c.buffered[0].timestamp)
                .map(|(index, _)| index)
            else {
                break;
            };
            let candidate = cursors[index].buffered.remove(0);
            let room_id = cursors[index].room.room_id().to_owned();
            let event_id =
                candidate.event.event_id().ok_or(matrix_sdk::Error::InsufficientData)?.to_owned();
            if seen.insert((room_id, event_id)) {
                selected.push((index, candidate));
            }
        }
        // Map only the selected roots; buffered heads of other rooms stay raw.
        let mut rooms: Vec<InboxPageRoom> = Vec::new();
        for (index, candidate) in selected {
            let room = &cursors[index].room;
            let item = ThreadListService::build_thread_list_item(room, candidate.event)
                .await
                .ok_or(matrix_sdk::Error::InsufficientData)?;
            let thread = InboxPageThread { item, participated: candidate.participated };
            match rooms.iter_mut().find(|r| r.room_id == room.room_id()) {
                Some(entry) => entry.threads.push(thread),
                None => rooms.push(InboxPageRoom {
                    room_id: room.room_id().to_owned(),
                    threads: vec![thread],
                }),
            }
        }
        let has_more = cursors.iter().any(|c| !c.exhausted || !c.buffered.is_empty());
        self.cursors = Some(cursors);
        self.seen = seen;
        Ok(InboxPage { rooms, has_more })
    }
}

/// Unread state of one thread, from the newest reply back to the read boundary.
#[derive(Clone, Debug)]
pub struct ThreadUnread {
    /// Incoming replies newer than the boundary.
    pub unread: u64,
    /// Highlighting replies (push rules or explicit mentions) among `unread`.
    pub mentions: u64,
    /// Oldest reply newer than the boundary.
    pub first_unread: Option<OwnedEventId>,
    /// The receipt or own reply that ended the scan. `None` when the scan
    /// reached the root (or was capped) without meeting one.
    pub boundary: Option<OwnedEventId>,
    /// Whether the user sent any scanned reply.
    pub participated: bool,
    /// False when `max_events` was reached before exhausting replies or finding
    /// a boundary: counts are lower bounds.
    pub complete: bool,
}

/// Classify a thread with backwards `/relations` pages, stopping at the newest
/// receipt or own reply. Never builds a timeline and never pages past the
/// boundary. A receipt on the root (or one that can't be located) counts every
/// reply.
pub async fn thread_unread(
    room: &Room,
    root: OwnedEventId,
    receipts: &HashSet<OwnedEventId>,
    max_events: usize,
) -> matrix_sdk::Result<ThreadUnread> {
    let own = room.own_user_id().to_owned();
    let mut result = ThreadUnread {
        unread: 0,
        mentions: 0,
        first_unread: None,
        boundary: None,
        participated: false,
        complete: true,
    };
    let mut from = None;
    let mut tokens = HashSet::new();
    let mut seen = HashSet::new();
    let mut scanned = 0;
    'pages: loop {
        if scanned == max_events {
            result.complete = false;
            break;
        }
        let page = room
            .relations(
                root.clone(),
                RelationsOptions {
                    from,
                    dir: Direction::Backward,
                    limit: Some(((max_events - scanned).min(100) as u32).into()),
                    include_relations: IncludeRelations::RelationsOfType(RelationType::Thread),
                    recurse: false,
                },
            )
            .await?;
        for event in page.chunk {
            if scanned == max_events {
                result.complete = false;
                break 'pages;
            }
            scanned += 1;
            let id = event.event_id().ok_or(matrix_sdk::Error::InsufficientData)?.to_owned();
            if !seen.insert(id.clone()) {
                continue;
            }
            let is_own = event.sender().as_deref() == Some(&*own);
            result.participated |= is_own;
            if receipts.contains(&id) || is_own {
                // Replying implies having read what came before.
                result.boundary = Some(id);
                break 'pages;
            }
            result.unread += 1;
            if is_highlight(&event, &own) {
                result.mentions += 1;
            }
            result.first_unread = Some(id);
        }
        match page.next_batch_token {
            Some(token) if tokens.insert(token.clone()) => from = Some(token),
            Some(_) => return Err(matrix_sdk::Error::InsufficientData),
            None => break,
        }
    }
    if result.complete && result.boundary.is_none() && receipts.contains(&root) {
        result.boundary = Some(root);
    }
    Ok(result)
}

fn is_highlight(event: &TimelineEvent, own: &ruma::UserId) -> bool {
    if event.push_actions().is_some_and(|actions| actions.iter().any(|a| a.is_highlight())) {
        return true;
    }
    // Push context can be unavailable; explicit intentional mentions remain a
    // mention.
    event
        .raw()
        .get_field::<serde_json::Value>("content")
        .ok()
        .flatten()
        .map(|content| {
            let explicit = &content["m.mentions"];
            explicit["room"].as_bool() == Some(true)
                || explicit["user_ids"]
                    .as_array()
                    .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(own.as_str())))
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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
    async fn test_pager_limits_results_and_keeps_buffered_threads_for_next_request() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let id = room_id!("!quiet:example.org");
        let f = EventFactory::new().room(id).sender(user_id!("@alice:example.org"));
        let events = (0..25)
            .map(|i| {
                f.text_msg("root")
                    .event_id(ruma::EventId::parse(format!("$root{i}")).unwrap())
                    .into_raw()
            })
            .collect();
        server.mock_room_threads().ok(events, None).mock_once().mount().await;
        server.sync_joined_room(&client, id).await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/_matrix/client/v3/joined_rooms"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"joined_rooms": [id]})),
            )
            .expect(1)
            .mount(server.server())
            .await;
        let mut pager = super::InboxPager::new(client);
        let first = pager.next_page(20).await.unwrap();
        assert_eq!(first.rooms.iter().map(|r| r.threads.len()).sum::<usize>(), 20);
        assert!(first.has_more);
        let second = pager.next_page(20).await.unwrap();
        assert_eq!(second.rooms.iter().map(|r| r.threads.len()).sum::<usize>(), 5);
        assert!(!second.has_more);
        assert!(pager.next_page(20).await.unwrap().rooms.is_empty());
    }

    #[async_test]
    async fn test_pager_exposes_bundled_participation() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let id = room_id!("!bundled:example.org");
        let f = EventFactory::new().room(id).sender(user_id!("@alice:example.org"));
        let reply = f.text_msg("reply").event_id(event_id!("$reply")).into_raw();
        let root = f
            .text_msg("root")
            .event_id(event_id!("$root"))
            .with_bundled_thread_summary(reply, 1, true)
            .into_raw();
        server.mock_room_threads().ok(vec![root], None).mock_once().mount().await;
        server.sync_joined_room(&client, id).await;
        wiremock::Mock::given(wiremock::matchers::path("/_matrix/client/v3/joined_rooms"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"joined_rooms": [id]})),
            )
            .mount(server.server())
            .await;
        let page = super::InboxPager::new(client).next_page(20).await.unwrap();
        let thread = &page.rooms[0].threads[0];
        assert!(thread.participated);
        assert_eq!(thread.item.latest_event.as_ref().unwrap().event_id, event_id!("$reply"));
    }

    #[async_test]
    async fn test_participated_pager_asks_the_server_to_filter() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let id = room_id!("!mine:example.org");
        server.sync_joined_room(&client, id).await;
        wiremock::Mock::given(wiremock::matchers::path("/_matrix/client/v3/joined_rooms"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"joined_rooms": [id]})),
            )
            .mount(server.server())
            .await;
        wiremock::Mock::given(wiremock::matchers::path_regex(r"/threads$"))
            .and(wiremock::matchers::query_param("include", "participated"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"chunk": []})),
            )
            .expect(1)
            .mount(server.server())
            .await;
        let page = super::InboxPager::with_include(client, super::IncludeThreads::Participated)
            .next_page(20)
            .await
            .unwrap();
        assert!(page.rooms.is_empty() && !page.has_more);
    }

    #[async_test]
    async fn test_pager_merges_room_heads_and_deduplicates_across_pages() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let first_id = room_id!("!first:example.org");
        let second_id = room_id!("!second:example.org");
        for id in [first_id, second_id] {
            server.sync_joined_room(&client, id).await;
        }
        wiremock::Mock::given(wiremock::matchers::path("/_matrix/client/v3/joined_rooms"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"joined_rooms": [first_id, second_id]})),
            )
            .expect(1)
            .mount(server.server())
            .await;
        let f = EventFactory::new().sender(user_id!("@alice:example.org"));
        let root = |room, id: &ruma::EventId, timestamp| {
            f.text_msg("root")
                .room(room)
                .event_id(id)
                .server_ts(1)
                .with_bundled_thread_summary(
                    f.text_msg("latest reply").room(room).server_ts(timestamp).into_raw(),
                    1,
                    false,
                )
                .into_raw()
        };
        wiremock::Mock::given(wiremock::matchers::path(format!(
            "/_matrix/client/v1/rooms/{first_id}/threads"
        )))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "chunk": [root(first_id, event_id!("$newest"), 4000)],
            "next_batch": "older",
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(server.server())
        .await;
        server
            .mock_room_threads()
            .match_from("older")
            .ok(
                vec![
                    root(first_id, event_id!("$newest"), 4000),
                    root(first_id, event_id!("$oldest"), 1000),
                ],
                None,
            )
            .mock_once()
            .mount()
            .await;
        wiremock::Mock::given(wiremock::matchers::path(format!(
            "/_matrix/client/v1/rooms/{second_id}/threads"
        )))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "chunk": [root(second_id, event_id!("$middle"), 3000)],
        })))
        .expect(1)
        .mount(server.server())
        .await;
        let mut pager = super::InboxPager::new(client);
        for (expected, has_more) in [
            (event_id!("$newest"), true),
            (event_id!("$middle"), true),
            (event_id!("$oldest"), false),
        ] {
            let page = pager.next_page(1).await.unwrap();
            assert_eq!(page.rooms.len(), 1);
            assert_eq!(page.rooms[0].threads.len(), 1);
            assert_eq!(page.rooms[0].threads[0].item.root_event.event_id, expected);
            assert_eq!(page.has_more, has_more);
        }
    }

    #[async_test]
    async fn test_pager_failed_page_preserves_buffered_threads_and_cursor() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let id = room_id!("!retry:example.org");
        server.sync_joined_room(&client, id).await;
        wiremock::Mock::given(wiremock::matchers::path("/_matrix/client/v3/joined_rooms"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"joined_rooms": [id]})),
            )
            .expect(1)
            .mount(server.server())
            .await;
        let f = EventFactory::new().room(id).sender(user_id!("@alice:example.org"));
        server
            .mock_room_threads()
            .ok(
                vec![
                    f.text_msg("first").event_id(event_id!("$first")).server_ts(3000).into_raw(),
                    f.text_msg("buffered")
                        .event_id(event_id!("$buffered"))
                        .server_ts(2000)
                        .into_raw(),
                ],
                Some("older".into()),
            )
            .mock_once()
            .mount()
            .await;
        let mut pager = super::InboxPager::new(client);
        assert_eq!(
            pager.next_page(1).await.unwrap().rooms[0].threads[0].item.root_event.event_id,
            event_id!("$first"),
        );
        server
            .mock_room_threads()
            .match_from("older")
            .respond_with_code_and_json(
                403,
                serde_json::json!({"errcode": "M_FORBIDDEN", "error": "try again"}),
            )
            .mock_once()
            .mount()
            .await;
        assert!(pager.next_page(2).await.is_err());
        server.verify_and_reset().await;
        server
            .mock_room_threads()
            .match_from("older")
            .ok(
                vec![f.text_msg("last").event_id(event_id!("$last")).server_ts(1000).into_raw()],
                None,
            )
            .mock_once()
            .mount()
            .await;
        let page = pager.next_page(2).await.unwrap();
        let ids: Vec<&ruma::EventId> = page.rooms[0]
            .threads
            .iter()
            .map(|thread| thread.item.root_event.event_id.as_ref())
            .collect();
        assert_eq!(ids, [event_id!("$buffered"), event_id!("$last")]);
        assert!(!page.has_more);
    }

    #[async_test]
    async fn test_pager_repeated_page_token_fails_without_committing_partial_page() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let id = room_id!("!loop:example.org");
        server.sync_joined_room(&client, id).await;
        wiremock::Mock::given(wiremock::matchers::path("/_matrix/client/v3/joined_rooms"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"joined_rooms": [id]})),
            )
            .mount(server.server())
            .await;
        server.mock_room_threads().ok(vec![], Some("loop".into())).mount().await;
        let mut pager = super::InboxPager::new(client);
        assert!(pager.next_page(20).await.is_err());
        assert!(pager.cursors.is_none());
        assert!(pager.seen.is_empty());
    }

    async fn unread_room(
        server: &MatrixMockServer,
        pages: Vec<(Vec<&str>, Option<&str>, Option<&str>)>,
    ) -> matrix_sdk::Room {
        let client = server.client_builder().build().await;
        let id = room_id!("!unread:example.org");
        let room = server.sync_joined_room(&client, id).await;
        let other = EventFactory::new().room(id).sender(user_id!("@alice:example.org"));
        let own = EventFactory::new().room(id).sender(user_id!("@example:localhost"));
        // Newest first, as returned for `dir=b`. `me:` marks an own reply, `@`
        // a mention.
        for (events, from, next) in pages {
            let chunk: Vec<_> = events
                .into_iter()
                .map(|spec| {
                    let (factory, id) = match spec.strip_prefix("me:") {
                        Some(id) => (&own, id),
                        None => (&other, spec),
                    };
                    let (id, mention) = match id.strip_prefix('@') {
                        Some(id) => (id, true),
                        None => (id, false),
                    };
                    let mut msg = factory
                        .text_msg("reply")
                        .event_id(ruma::EventId::parse(format!("${id}")).unwrap());
                    if mention {
                        msg = msg.mentions(ruma::events::Mentions::with_user_ids([user_id!(
                            "@example:localhost"
                        )
                        .to_owned()]));
                    }
                    msg.into_raw()
                })
                .collect();
            let mut template =
                matrix_sdk::test_utils::mocks::RoomRelationsResponseTemplate::default()
                    .events(chunk);
            if let Some(next) = next {
                template = template.next_batch(next);
            }
            let mock =
                server.mock_room_relations().match_target_event(event_id!("$root").to_owned());
            let mock = match from {
                Some(from) => mock.match_from(from),
                None => mock,
            };
            mock.ok(template).mock_once().mount().await;
        }
        room
    }

    #[async_test]
    async fn test_thread_unread_stops_at_newest_receipt_without_paging_to_root() {
        let server = MatrixMockServer::new().await;
        let room = unread_room(&server, vec![(vec!["c", "@b", "a"], None, Some("older"))]).await;
        let receipts = HashSet::from([event_id!("$a").to_owned()]);
        let unread = super::thread_unread(&room, event_id!("$root").to_owned(), &receipts, 300)
            .await
            .unwrap();
        assert_eq!((unread.unread, unread.mentions), (2, 1));
        assert_eq!(unread.first_unread.as_deref(), Some(event_id!("$b")));
        assert_eq!(unread.boundary.as_deref(), Some(event_id!("$a")));
        assert!(unread.complete && !unread.participated);
    }

    #[async_test]
    async fn test_thread_unread_treats_own_reply_as_boundary() {
        let server = MatrixMockServer::new().await;
        let room = unread_room(&server, vec![(vec!["c", "me:b", "a"], None, Some("older"))]).await;
        let unread =
            super::thread_unread(&room, event_id!("$root").to_owned(), &HashSet::new(), 300)
                .await
                .unwrap();
        assert_eq!(unread.unread, 1);
        assert_eq!(unread.boundary.as_deref(), Some(event_id!("$b")));
        assert!(unread.participated);
    }

    #[async_test]
    async fn test_thread_unread_pages_to_root_for_root_receipt() {
        let server = MatrixMockServer::new().await;
        let room = unread_room(
            &server,
            vec![(vec!["d", "c"], None, Some("older")), (vec!["b", "a"], Some("older"), None)],
        )
        .await;
        let receipts = HashSet::from([event_id!("$root").to_owned()]);
        let unread = super::thread_unread(&room, event_id!("$root").to_owned(), &receipts, 300)
            .await
            .unwrap();
        assert_eq!(unread.unread, 4);
        assert_eq!(unread.first_unread.as_deref(), Some(event_id!("$a")));
        assert_eq!(unread.boundary.as_deref(), Some(event_id!("$root")));
        assert!(unread.complete);
    }

    #[async_test]
    async fn test_thread_unread_reports_capped_scan_as_incomplete() {
        let server = MatrixMockServer::new().await;
        let room =
            unread_room(&server, vec![(vec!["d", "c", "b", "a"], None, Some("older"))]).await;
        let unread = super::thread_unread(&room, event_id!("$root").to_owned(), &HashSet::new(), 2)
            .await
            .unwrap();
        assert_eq!(unread.unread, 2);
        assert!(!unread.complete);
        assert_eq!(unread.boundary, None);
    }

    #[async_test]
    async fn test_thread_unread_cap_does_not_claim_root_receipt_or_fetch_older_page() {
        let server = MatrixMockServer::new().await;
        let room = unread_room(&server, vec![(vec!["d", "c"], None, Some("older"))]).await;
        let receipts = HashSet::from([event_id!("$root").to_owned()]);
        let unread =
            super::thread_unread(&room, event_id!("$root").to_owned(), &receipts, 2).await.unwrap();
        assert_eq!(unread.unread, 2);
        assert_eq!(unread.first_unread.as_deref(), Some(event_id!("$c")));
        assert!(!unread.complete);
        assert!(unread.boundary.is_none());
    }

    #[async_test]
    async fn test_thread_unread_does_not_inspect_boundary_beyond_cap() {
        let server = MatrixMockServer::new().await;
        let room = unread_room(&server, vec![(vec!["c", "me:b"], None, None)]).await;
        let unread = super::thread_unread(&room, event_id!("$root").to_owned(), &HashSet::new(), 1)
            .await
            .unwrap();
        assert_eq!(unread.unread, 1);
        assert!(!unread.complete && !unread.participated);
        assert!(unread.boundary.is_none());
    }

    #[async_test]
    async fn test_thread_unread_zero_cap_does_not_request_replies() {
        let server = MatrixMockServer::new().await;
        let room = unread_room(&server, vec![]).await;
        let unread = super::thread_unread(&room, event_id!("$root").to_owned(), &HashSet::new(), 0)
            .await
            .unwrap();
        assert_eq!((unread.unread, unread.mentions), (0, 0));
        assert!(!unread.complete);
        assert!(unread.first_unread.is_none() && unread.boundary.is_none());
        assert!(
            server
                .server()
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| { !request.url.path().contains("/relations/") })
        );
    }

    #[async_test]
    async fn test_thread_unread_exact_cap_at_end_is_complete() {
        let server = MatrixMockServer::new().await;
        let room = unread_room(&server, vec![(vec!["b", "a"], None, None)]).await;
        let receipts = HashSet::from([event_id!("$root").to_owned()]);
        let unread =
            super::thread_unread(&room, event_id!("$root").to_owned(), &receipts, 2).await.unwrap();
        assert_eq!(unread.unread, 2);
        assert!(unread.complete);
        assert_eq!(unread.boundary.as_deref(), Some(event_id!("$root")));
    }

    #[async_test]
    async fn test_thread_unread_receipt_on_own_reply_records_participation() {
        let server = MatrixMockServer::new().await;
        let room = unread_room(&server, vec![(vec!["c", "me:b", "a"], None, None)]).await;
        let receipts = HashSet::from([event_id!("$a").to_owned(), event_id!("$b").to_owned()]);
        let unread =
            super::thread_unread(&room, event_id!("$root").to_owned(), &receipts, 3).await.unwrap();
        assert_eq!(unread.unread, 1);
        assert_eq!(unread.boundary.as_deref(), Some(event_id!("$b")));
        assert!(unread.complete && unread.participated);
    }

    #[async_test]
    async fn test_thread_unread_deduplicates_overlapping_pages() {
        let server = MatrixMockServer::new().await;
        let room = unread_room(
            &server,
            vec![(vec!["c", "@b"], None, Some("older")), (vec!["@b", "a"], Some("older"), None)],
        )
        .await;
        let unread = super::thread_unread(&room, event_id!("$root").to_owned(), &HashSet::new(), 4)
            .await
            .unwrap();
        assert_eq!((unread.unread, unread.mentions), (3, 1));
        assert_eq!(unread.first_unread.as_deref(), Some(event_id!("$a")));
        assert!(unread.complete);
    }

    #[async_test]
    async fn test_thread_unread_repeated_token_fails() {
        let server = MatrixMockServer::new().await;
        let room = unread_room(
            &server,
            vec![(vec![], None, Some("loop")), (vec![], Some("loop"), Some("loop"))],
        )
        .await;
        assert!(
            super::thread_unread(&room, event_id!("$root").to_owned(), &HashSet::new(), 4)
                .await
                .is_err()
        );
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
