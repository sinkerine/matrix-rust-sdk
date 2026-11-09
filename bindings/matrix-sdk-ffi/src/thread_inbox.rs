// Copyright 2026 The Matrix.org Foundation C.I.C.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//! Complete account-wide thread discovery exposed to mobile clients.

use std::{fmt::Debug, sync::Arc};

use matrix_sdk_common::{SendOutsideWasm, SyncOutsideWasm};

use crate::{
    ClientError, TaskHandle, runtime::get_runtime_handle, timeline::threads::ThreadListItem,
};

/// All threads in a joined room. Empty rooms are included so consumers can
/// reconcile removals without retaining stale unread badges.
#[derive(uniffi::Record)]
pub struct ThreadInboxRoom {
    /// The room ID.
    pub room_id: String,
    /// Every visible thread root, after exhausting server pagination.
    pub threads: Vec<ThreadListItem>,
}

#[derive(uniffi::Record)]
pub struct ThreadInboxPage {
    pub rooms: Vec<ThreadInboxPageRoom>,
    pub has_more: bool,
}

#[derive(uniffi::Record)]
pub struct ThreadInboxPageRoom {
    pub room_id: String,
    pub threads: Vec<ThreadInboxPageThread>,
}

/// A selected root with the bundled `current_user_participated` flag.
#[derive(uniffi::Record)]
pub struct ThreadInboxPageThread {
    pub item: ThreadListItem,
    pub participated: bool,
}

/// Unread replies newer than the newest receipt or own reply. See
/// [`matrix_sdk_ui::thread_inbox::thread_unread`].
#[derive(uniffi::Record)]
pub struct ThreadInboxUnread {
    pub unread: u64,
    pub mentions: u64,
    pub first_unread: Option<String>,
    pub boundary: Option<String>,
    pub participated: bool,
    pub complete: bool,
}

#[derive(uniffi::Object)]
pub struct ThreadInboxPager {
    inner: tokio::sync::Mutex<matrix_sdk_ui::thread_inbox::InboxPager>,
}

impl ThreadInboxPager {
    pub(crate) fn new(client: matrix_sdk::Client, participated_only: bool) -> Arc<Self> {
        let include = if participated_only {
            ruma::api::client::threads::get_threads::v1::IncludeThreads::Participated
        } else {
            ruma::api::client::threads::get_threads::v1::IncludeThreads::All
        };
        Arc::new(Self {
            inner: tokio::sync::Mutex::new(matrix_sdk_ui::thread_inbox::InboxPager::with_include(
                client, include,
            )),
        })
    }
}

#[matrix_sdk_ffi_macros::export]
impl ThreadInboxPager {
    pub async fn next_page(&self) -> Result<ThreadInboxPage, ClientError> {
        let page = self.inner.lock().await.next_page(20).await?;
        Ok(ThreadInboxPage {
            rooms: page
                .rooms
                .into_iter()
                .map(|room| ThreadInboxPageRoom {
                    room_id: room.room_id.to_string(),
                    threads: room
                        .threads
                        .into_iter()
                        .map(|thread| ThreadInboxPageThread {
                            item: thread.item.into(),
                            participated: thread.participated,
                        })
                        .collect(),
                })
                .collect(),
            has_more: page.has_more,
        })
    }
}

pub(crate) async fn thread_unread(
    client: &matrix_sdk::Client,
    room_id: String,
    thread_id: String,
    receipt_ids: Vec<String>,
    max_events: u32,
) -> Result<ThreadInboxUnread, ClientError> {
    let room = client
        .get_room(&ruma::RoomId::parse(room_id)?)
        .ok_or(ClientError::from_str("Room not found", None))?;
    let receipts = receipt_ids
        .into_iter()
        .map(ruma::EventId::parse)
        .collect::<Result<std::collections::HashSet<_>, _>>()?;
    let unread = matrix_sdk_ui::thread_inbox::thread_unread(
        &room,
        ruma::EventId::parse(thread_id)?,
        &receipts,
        max_events as usize,
    )
    .await?;
    Ok(ThreadInboxUnread {
        unread: unread.unread,
        mentions: unread.mentions,
        first_unread: unread.first_unread.map(|id| id.to_string()),
        boundary: unread.boundary.map(|id| id.to_string()),
        participated: unread.participated,
        complete: unread.complete,
    })
}

/// Incremental server pages. The loading future signals completion or failure.
#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait ThreadInboxPageListener: SendOutsideWasm + SyncOutsideWasm + Debug {
    fn on_page(&self, room: ThreadInboxRoom);
}

pub(crate) async fn load_pages(
    client: &matrix_sdk::Client,
    room_ids: Option<Vec<String>>,
    listener: Box<dyn ThreadInboxPageListener>,
) -> Result<(), ClientError> {
    let ids = room_ids
        .map(|ids| ids.into_iter().map(ruma::RoomId::parse).collect::<Result<Vec<_>, _>>())
        .transpose()?;
    matrix_sdk_ui::thread_inbox::load_with_progress(client, ids, |room| {
        listener.on_page(ThreadInboxRoom {
            room_id: room.room_id.to_string(),
            threads: room.threads.into_iter().map(Into::into).collect(),
        });
    })
    .await?;
    Ok(())
}

#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait ThreadInboxBadgeListener: SendOutsideWasm + SyncOutsideWasm + Debug {
    fn on_badge(&self, badge: ThreadInboxBadge);
}

pub(crate) async fn load_badge_pages(
    client: &matrix_sdk::Client,
    room_ids: Option<Vec<String>>,
    private: bool,
    listener: Box<dyn ThreadInboxBadgeListener>,
) -> Result<(), ClientError> {
    let ids = room_ids
        .map(|ids| ids.into_iter().map(ruma::RoomId::parse).collect::<Result<Vec<_>, _>>())
        .transpose()?;
    matrix_sdk_ui::thread_badges::load_with_progress(client, ids, private, |badge| {
        listener.on_badge(ThreadInboxBadge {
            room_id: badge.room_id.to_string(),
            thread_id: badge.thread_id.to_string(),
            unread: badge.unread,
            mentions: badge.mentions,
            participating: badge.participating,
        });
    })
    .await?;
    Ok(())
}

/// Compact state for badge-only refreshes while the inbox is closed.
#[derive(uniffi::Record)]
pub struct ThreadInboxBadge {
    pub room_id: String,
    pub thread_id: String,
    pub unread: u64,
    pub mentions: u64,
    pub participating: bool,
}

pub(crate) async fn load_badges(
    client: &matrix_sdk::Client,
    room_ids: Option<Vec<String>>,
    private: bool,
) -> Result<Vec<ThreadInboxBadge>, ClientError> {
    let ids = room_ids
        .map(|ids| ids.into_iter().map(ruma::RoomId::parse).collect::<Result<Vec<_>, _>>())
        .transpose()?;
    Ok(matrix_sdk_ui::thread_badges::load(client, ids, private)
        .await?
        .into_iter()
        .map(|badge| ThreadInboxBadge {
            room_id: badge.room_id.to_string(),
            thread_id: badge.thread_id.to_string(),
            unread: badge.unread,
            mentions: badge.mentions,
            participating: badge.participating,
        })
        .collect())
}

/// Invalidates account data after sync, including receipt-only updates.
#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait ThreadInboxListener: SendOutsideWasm + SyncOutsideWasm + Debug {
    /// `None` means a full rescan is needed (initial subscription or lag).
    fn on_update(&self, room_ids: Option<Vec<String>>);
}

pub(crate) fn subscribe(
    client: &matrix_sdk::Client,
    listener: Box<dyn ThreadInboxListener>,
) -> Arc<TaskHandle> {
    let mut receiver = client.subscribe_to_all_room_updates();
    listener.on_update(None);
    Arc::new(TaskHandle::new(get_runtime_handle().spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(update) => listener
                    .on_update(Some(update.iter_all_room_ids().map(ToString::to_string).collect())),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    listener.on_update(None)
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    })))
}

/// Load all joined rooms, or explicitly selected changed rooms. A failure in
/// any room fails the operation; partial snapshots are never labelled complete.
pub(crate) async fn load(
    client: &matrix_sdk::Client,
    room_ids: Option<Vec<String>>,
) -> Result<Vec<ThreadInboxRoom>, ClientError> {
    let snapshots = if let Some(ids) = room_ids {
        let ids = ids
            .into_iter()
            .map(ruma::RoomId::parse)
            .collect::<Result<std::collections::HashSet<_>, _>>()?;
        let rooms =
            client.joined_rooms().into_iter().filter(|room| ids.contains(room.room_id())).collect();
        matrix_sdk_ui::thread_inbox::load_rooms(rooms).await?
    } else {
        matrix_sdk_ui::thread_inbox::load(client).await?
    };
    Ok(snapshots
        .into_iter()
        .map(|room| ThreadInboxRoom {
            room_id: room.room_id.to_string(),
            threads: room.threads.into_iter().map(Into::into).collect(),
        })
        .collect())
}
