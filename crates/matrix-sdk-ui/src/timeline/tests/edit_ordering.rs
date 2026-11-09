// Copyright 2025 The Matrix.org Foundation C.I.C.
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

use matrix_sdk::deserialized_responses::TimelineEvent;
use matrix_sdk_test::{User, async_test};
use ruma::{EventId, event_id, events::room::message::RoomMessageEventContentWithoutRelation};

use super::TestTimeline;
use crate::timeline::{event_item::RemoteEventOrigin, traits::RoomDataProvider};

async fn apply_out_of_band_edit(timeline: &TestTimeline, id: &EventId, ts: u64, body: &str) {
    let event: TimelineEvent = timeline
        .factory
        .text_msg(format!("* {body}"))
        .sender(User::Alice)
        .event_id(id)
        .server_ts(ts)
        .edit(event_id!("$original"), RoomMessageEventContentWithoutRelation::text_plain(body))
        .into();
    timeline
        .controller
        .handle_remote_aggregations(
            vec![eyeball_im::VectorDiff::PushBack { value: event }],
            RemoteEventOrigin::Cache,
        )
        .await;
}

async fn assert_body(timeline: &TestTimeline, expected: &str) {
    let items = timeline.controller.items().await;
    let item = items
        .iter()
        .filter_map(|item| item.as_event())
        .find(|item| item.event_id() == Some(event_id!("$original")))
        .unwrap();
    assert_eq!(item.content().as_message().unwrap().body(), expected);
    assert!(item.content().as_message().unwrap().is_edited());
}

async fn original(timeline: &TestTimeline) {
    timeline
        .handle_live_event(
            timeline
                .factory
                .text_msg("original")
                .event_id(event_id!("$original"))
                .sender(User::Alice),
        )
        .await;
}

#[async_test]
async fn test_out_of_band_edits_reverse_chronological_order() {
    let timeline = TestTimeline::new().await;
    original(&timeline).await;
    apply_out_of_band_edit(&timeline, event_id!("$newer"), 2000, "newer").await;
    apply_out_of_band_edit(&timeline, event_id!("$older"), 1000, "older").await;
    assert_body(&timeline, "newer").await;
}

#[async_test]
async fn test_out_of_band_edits_equal_timestamps_use_event_id() {
    for reverse in [false, true] {
        let timeline = TestTimeline::new().await;
        original(&timeline).await;
        let mut edits = [(event_id!("$a"), "a"), (event_id!("$z"), "z")];
        if reverse {
            edits.reverse();
        }
        for (id, body) in edits {
            apply_out_of_band_edit(&timeline, id, 1000, body).await;
        }
        assert_body(&timeline, "z").await;
    }
}

#[async_test]
async fn test_positioned_edits_keep_timeline_order_with_clock_skew() {
    for out_of_band_first in [false, true] {
        let timeline = TestTimeline::new().await;
        original(&timeline).await;
        if out_of_band_first {
            apply_out_of_band_edit(&timeline, event_id!("$out_of_band"), 1500, "out of band").await;
        }
        for (id, ts, body) in
            [(event_id!("$first"), 3000, "first"), (event_id!("$last"), 2000, "last")]
        {
            timeline
                .handle_live_event(
                    timeline
                        .factory
                        .text_msg(format!("* {body}"))
                        .sender(User::Alice)
                        .event_id(id)
                        .server_ts(ts)
                        .edit(
                            event_id!("$original"),
                            RoomMessageEventContentWithoutRelation::text_plain(body),
                        ),
                )
                .await;
        }
        if !out_of_band_first {
            apply_out_of_band_edit(&timeline, event_id!("$out_of_band"), 1500, "out of band").await;
        }
        assert_body(&timeline, "last").await;
        apply_out_of_band_edit(&timeline, event_id!("$newest"), 4000, "newest").await;
        assert_body(&timeline, "newest").await;
    }
}

#[async_test]
async fn test_out_of_band_invalid_sender_does_not_hide_valid_edit() {
    let timeline = TestTimeline::new().await;
    original(&timeline).await;
    let invalid: TimelineEvent = timeline
        .factory
        .text_msg("* invalid")
        .sender(User::Bob)
        .event_id(event_id!("$invalid"))
        .server_ts(3000)
        .edit(event_id!("$original"), RoomMessageEventContentWithoutRelation::text_plain("invalid"))
        .into();
    timeline
        .controller
        .handle_remote_aggregations(
            vec![eyeball_im::VectorDiff::PushBack { value: invalid }],
            RemoteEventOrigin::Cache,
        )
        .await;
    apply_out_of_band_edit(&timeline, event_id!("$valid"), 2000, "valid").await;
    assert_body(&timeline, "valid").await;
}

#[async_test]
async fn test_positioned_invalid_sender_does_not_hide_valid_edit() {
    let timeline = TestTimeline::new().await;
    for (id, sender, ts, body) in [
        (event_id!("$valid"), User::Alice, 2000, "valid"),
        (event_id!("$invalid"), User::Bob, 3000, "invalid"),
    ] {
        timeline
            .handle_live_event(
                timeline
                    .factory
                    .text_msg(format!("* {body}"))
                    .sender(sender)
                    .event_id(id)
                    .server_ts(ts)
                    .edit(
                        event_id!("$original"),
                        RoomMessageEventContentWithoutRelation::text_plain(body),
                    ),
            )
            .await;
    }
    // Restore the original after both edits so its first resolution must
    // choose the valid earlier position, rather than retain an applied edit.
    original(&timeline).await;
    assert_body(&timeline, "valid").await;
}

#[async_test]
async fn test_out_of_band_malformed_edit_is_ignored_before_selection() {
    let timeline = TestTimeline::new().await;
    original(&timeline).await;
    let invalid: ruma::serde::Raw<ruma::events::AnySyncTimelineEvent> = timeline
        .factory
        .text_msg("* malformed")
        .sender(User::Alice)
        .event_id(event_id!("$malformed"))
        .server_ts(3000)
        .edit(
            event_id!("$original"),
            RoomMessageEventContentWithoutRelation::text_plain("malformed"),
        )
        .into_raw_sync();
    let mut json: serde_json::Value = serde_json::from_str(invalid.json().get()).unwrap();
    // Malformed raw events are rejected by the event parser before they can
    // become edit candidates; they must not prevent a subsequent valid edit.
    json["state_key"] = serde_json::json!(true);
    let invalid = TimelineEvent::from_plaintext(
        ruma::serde::Raw::from_json_string(json.to_string()).unwrap(),
    );
    timeline
        .controller
        .handle_remote_aggregations(
            vec![eyeball_im::VectorDiff::PushBack { value: invalid }],
            RemoteEventOrigin::Cache,
        )
        .await;
    apply_out_of_band_edit(&timeline, event_id!("$valid"), 2000, "valid").await;
    assert_body(&timeline, "valid").await;
}

#[async_test]
async fn test_local_edit_beats_out_of_band_remote_edit() {
    let timeline = TestTimeline::new().await;
    timeline
        .handle_live_event(
            timeline
                .factory
                .text_msg("original")
                .event_id(event_id!("$original"))
                .sender(timeline.data().own_user_id()),
        )
        .await;
    let txn_id = timeline
        .handle_local_event(
            ruma::events::room::message::RoomMessageEventContent::text_plain("local")
                .make_replacement(ruma::events::room::message::ReplacementMetadata::new(
                    event_id!("$original").to_owned(),
                    None,
                ))
                .into(),
        )
        .await;
    // The remote edit uses our sender too, so validity checks cannot mask a
    // priority regression.
    let event: TimelineEvent = timeline
        .factory
        .text_msg("* remote")
        .sender(timeline.data().own_user_id())
        .event_id(event_id!("$remote"))
        .server_ts(9000)
        .edit(event_id!("$original"), RoomMessageEventContentWithoutRelation::text_plain("remote"))
        .into();
    timeline
        .controller
        .handle_remote_aggregations(
            vec![eyeball_im::VectorDiff::PushBack { value: event }],
            RemoteEventOrigin::Cache,
        )
        .await;
    assert_body(&timeline, "local").await;
    timeline
        .controller
        .update_event_send_state(
            &txn_id,
            crate::timeline::EventSendState::Sent { event_id: event_id!("$local").to_owned() },
        )
        .await;
    let event: TimelineEvent = timeline
        .factory
        .text_msg("* remote2")
        .sender(timeline.data().own_user_id())
        .event_id(event_id!("$remote2"))
        .server_ts(9500)
        .edit(event_id!("$original"), RoomMessageEventContentWithoutRelation::text_plain("remote2"))
        .into();
    timeline
        .controller
        .handle_remote_aggregations(
            vec![eyeball_im::VectorDiff::PushBack { value: event }],
            RemoteEventOrigin::Cache,
        )
        .await;
    assert_body(&timeline, "local").await;
    timeline
        .handle_live_event(
            timeline
                .factory
                .text_msg("* local")
                .sender(timeline.data().own_user_id())
                .event_id(event_id!("$local"))
                .server_ts(10000)
                .edit(
                    event_id!("$original"),
                    RoomMessageEventContentWithoutRelation::text_plain("local"),
                ),
        )
        .await;
    assert_body(&timeline, "local").await;
    let items = timeline.controller.items().await;
    assert!(
        items
            .iter()
            .filter_map(|item| item.as_event())
            .find(|item| item.event_id() == Some(event_id!("$original")))
            .unwrap()
            .edit_send_state()
            .is_none()
    );
}
