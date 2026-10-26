// Copyright 2026 The Matrix.org Foundation C.I.C.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//! Compact unread classification for closed-inbox badge updates. This path never
//! constructs a UI timeline, sender profile, message preview or observable list.

use std::collections::HashSet;

use futures_util::{StreamExt, stream};
use matrix_sdk::{
    Client, Room,
    room::{IncludeRelations, ListThreadsOptions, RelationsOptions},
};
use ruma::{
    OwnedEventId, OwnedRoomId,
    api::Direction,
    events::{
        receipt::{ReceiptThread, ReceiptType},
        relation::RelationType,
    },
};

/// Minimal state needed to count unread threads and apply room notification rules.
#[derive(Clone, Debug)]
pub struct ThreadBadge {
    /// Joined room containing the thread.
    pub room_id: OwnedRoomId,
    /// Thread root.
    pub thread_id: OwnedEventId,
    /// Unread incoming replies after the selected threaded receipt.
    pub unread: u64,
    /// Explicit mentions among unread replies.
    pub mentions: u64,
    /// Whether the current user sent the root or any reply.
    pub participating: bool,
}

/// Exhaustively classify badges, without allocating UI timelines or previews.
pub async fn load(
    client: &Client,
    ids: Option<Vec<OwnedRoomId>>,
    private: bool,
) -> matrix_sdk::Result<Vec<ThreadBadge>> {
    let ids = match ids {
        Some(ids) => ids,
        None => {
            client
                .send(ruma::api::client::membership::joined_rooms::v3::Request::new())
                .await?
                .joined_rooms
        }
    };
    let mut rooms = Vec::new();
    for id in ids {
        let Some(room) = client.get_room(&id) else {
            return Err(matrix_sdk::Error::InsufficientData);
        };
        if room.state() == matrix_sdk::RoomState::Joined {
            rooms.push(room);
        }
    }
    let mut pending =
        stream::iter(rooms.into_iter().map(|room| load_room(room, private))).buffer_unordered(4);
    let mut badges = Vec::new();
    while let Some(result) = pending.next().await {
        badges.extend(result?);
    }
    Ok(badges)
}

async fn load_room(room: Room, private: bool) -> matrix_sdk::Result<Vec<ThreadBadge>> {
    let mut from = None;
    let mut tokens = HashSet::new();
    let mut roots = HashSet::new();
    let mut badges = Vec::new();
    loop {
        let page = room
            .list_threads(ListThreadsOptions {
                from,
                limit: Some(100u32.into()),
                ..Default::default()
            })
            .await?;
        for root in page.chunk {
            let id = root.event_id().ok_or(matrix_sdk::Error::InsufficientData)?.to_owned();
            if roots.insert(id.clone()) {
                badges.push(
                    classify(
                        &room,
                        id,
                        root.sender().as_deref() == Some(room.own_user_id()),
                        private,
                    )
                    .await?,
                );
            }
        }
        match page.prev_batch_token {
            Some(token) if tokens.insert(token.clone()) => from = Some(token),
            Some(_) => return Err(matrix_sdk::Error::InsufficientData),
            None => break,
        }
    }
    Ok(badges)
}

async fn classify(
    room: &Room,
    root: OwnedEventId,
    mut participating: bool,
    _private: bool,
) -> matrix_sdk::Result<ThreadBadge> {
    let mut receipts = HashSet::new();
    for kind in [ReceiptType::Read, ReceiptType::ReadPrivate] {
        if let Some((id, _)) = room
            .load_user_receipt(kind, &ReceiptThread::Thread(root.clone()), room.own_user_id())
            .await?
        {
            receipts.insert(id);
        }
    }
    let mut remaining = receipts.clone();
    remaining.remove(&root);
    let mut from = None;
    let mut tokens = HashSet::new();
    let mut events = HashSet::new();
    let mut unread = 0;
    let mut mentions = 0;
    loop {
        let page = room
            .relations(
                root.clone(),
                RelationsOptions {
                    from,
                    dir: Direction::Forward,
                    limit: Some(100u32.into()),
                    include_relations: IncludeRelations::RelationsOfType(RelationType::Thread),
                    recurse: false,
                },
            )
            .await?;
        for event in page.chunk {
            let id = event.event_id().ok_or(matrix_sdk::Error::InsufficientData)?.to_owned();
            if !events.insert(id.clone()) {
                continue;
            }
            let own = event.sender().as_deref() == Some(room.own_user_id());
            participating |= own;
            // Forward event order determines the later public/private boundary.
            // Reset at either receipt; timestamps can be skewed or absent.
            if receipts.contains(&id) {
                remaining.remove(&id);
                unread = 0;
                mentions = 0;
            } else if !own {
                let json: serde_json::Value = serde_json::from_str(event.raw().json().get())
                    .map_err(|_| matrix_sdk::Error::InsufficientData)?;
                if json.get("type").and_then(|v| v.as_str()) == Some("m.room.encrypted") {
                    return Err(matrix_sdk::Error::InsufficientData);
                }
                unread += 1;
                let explicit = &json["content"]["m.mentions"];
                if explicit["room"].as_bool() == Some(true)
                    || explicit["user_ids"].as_array().is_some_and(|ids| {
                        ids.iter().any(|id| id.as_str() == Some(room.own_user_id().as_str()))
                    })
                {
                    mentions += 1;
                }
            }
        }
        match page.next_batch_token {
            Some(token) if tokens.insert(token.clone()) => from = Some(token),
            Some(_) => return Err(matrix_sdk::Error::InsufficientData),
            None => break,
        }
    }
    if !remaining.is_empty() {
        return Err(matrix_sdk::Error::InsufficientData);
    }
    Ok(ThreadBadge {
        room_id: room.room_id().to_owned(),
        thread_id: root,
        unread,
        mentions,
        participating,
    })
}
