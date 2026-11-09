# MSC4391 bot commands: SDK support for Element X Android

Status: **APPROVED by the user.** Branch `feat/msc4391-bot-commands`, base `refs/heads/main` @ `e8ebe8abf`.
Worktree `/home/sinkerine/.paseo/worktrees/37dm06q5/feat-msc4391-bot-commands`.
Consumer: the Element X Android Hermes slash-command plan
(`/home/sinkerine/.paseo/worktrees/3b36k2jo/feat-hermes-slash/docs/features/hermes-slash-plan.md`, §2.2, D2/D6/D8, §5).

## Decisions

| ID | Topic | Decision |
|----|-------|----------|
| S1 | Getting the state | Sliding sync (MSC4186) stores room state only from `required_state` and ignores state that arrives in the timeline (`crates/matrix-sdk-base/src/response_processors/room/msc4186/mod.rs:72-77`). So `(org.matrix.msc4391.command_description, "*")` is added to the **room-subscription** extra `required_state`, next to `m.room.pinned_events`. The room-list **list** `required_state` doesn't change. |
| S2 | Constant form | `StateEventType::from(&str)` isn't `const`. The constant becomes `static DEFAULT_ROOM_SUBSCRIPTION_EXTRA_REQUIRED_STATE: LazyLock<[(StateEventType, &str); 2]>`, keeping its name and its one call site (`.iter()` works through `Deref`). `LazyLock` is already used in this crate (`timeline/event_item/mod.rs:353`). |
| S3 | Reply API | Add `Timeline::send_reply_with_extra_content`. `send_reply` delegates to it with `AddMentions::Yes, None`. The FFI method `Timeline::send_reply_with_extra_content` works the same way, and FFI `send_reply` delegates to it with `None, true`. |
| S4 | Changelog | Towncrier fragments in each changed crate's `changelog.d/`. There's no PR number (no push), so fragments get descriptive names, as in the existing `bindings/matrix-sdk-ffi/changelog.d/deactivate-account.changed.md`. If the change goes upstream later, they're renamed `<PR>.added.md`. |
| S5 | Nightly rustfmt | `rustup` isn't installed. `nightly-2026-09-28` rustfmt is built from the Hermes flake's locked rust-overlay (locked 2026-10-25). This was checked: the expression in §4 evaluates to `rust-minimal-1.101.0-nightly-2026-09-28.drv`. If the build fails, report it as a blocker. It can't be waived. |
| S6 | E2E policy | **Resolved by the user:** add a root `AGENTS.md` recording an explicit SDK E2E exemption (see §5.3). |

## 1. Goal and acceptance criteria

### Goal

Element X Android can do two things with the SDK:
1. Read a room's `org.matrix.msc4391.command_description` state events, through the existing
   `Room::subscribe_to_state_events` / `state_events`, for any room the room list service has subscribed to (opened
   rooms).
2. Send a reply through the send queue with extra top-level content fields, and choose whether the replied-to sender
   is added to `m.mentions`. This works in both live and thread-focused timelines.

### Acceptance criteria

1. **Room-subscription required_state.** Every `room_subscriptions.<room>.required_state` that `RoomListService`
   sends ends with `["m.room.pinned_events", ""], ["org.matrix.msc4391.command_description", "*"]`, in that order,
   after the unchanged `DEFAULT_REQUIRED_STATE` pairs.
2. **List required_state unchanged.** The `lists.all_rooms.required_state` sent by `RoomListService` doesn't contain
   `org.matrix.msc4391.command_description`. All existing list expectations pass without edits.
3. **UI API exists** with exactly this signature:
   `pub async fn send_reply_with_extra_content(&self, content: RoomMessageEventContentWithoutRelation, in_reply_to: OwnedEventId, add_mentions: AddMentions, extra_content: Option<serde_json::Map<String, serde_json::Value>>) -> Result<SendHandle, Error>`.
4. **Live reply.** In a live (non-thread) timeline, a reply to Bob's event with content mentions `{user_ids:[@carol]}`,
   `AddMentions::No` and `extra_content = {"org.matrix.msc4391.command": {...}}` sends one `PUT /send/m.room.message`
   whose JSON content has:
   - `m.relates_to.m.in_reply_to.event_id` == Bob's event, and no `rel_type`;
   - `org.matrix.msc4391.command` equal to the given extra value;
   - `m.mentions` == `{"user_ids": ["@carol:…"]}` exactly. Bob isn't added.
5. **Thread reply.** In a `TimelineFocus::Thread { thread_id: root }` timeline, the same call replying to an in-thread
   event sends content with:
   - `m.relates_to.rel_type == "m.thread"` and `m.relates_to.event_id == root`;
   - `m.relates_to.m.in_reply_to.event_id` == the replied-to event, and `m.relates_to.is_falling_back == false`;
   - the extra key present, and `m.mentions` unchanged, without the replied-to sender.
6. **No regression.** `send_reply` behaves as before. All existing `timeline::replies` tests pass unchanged,
   including that the sender is mentioned under the default `AddMentions::Yes` and that replying to oneself adds no
   mentions.
7. **FFI API exists** with exactly this signature:
   `pub async fn send_reply_with_extra_content(&self, msg: Arc<RoomMessageEventContentWithoutRelation>, event_id: String, extra_content_json: Option<String>, add_mentions: bool) -> Result<Arc<SendHandle>, ClientError>`.
   In Kotlin it is `Timeline.sendReplyWithExtraContent(msg, eventId, extraContentJson, addMentions)`.
   An invalid event ID returns `ClientError` (from `RoomError::InvalidRepliedToEventId`). Invalid JSON, or JSON that
   isn't an object, returns `ClientError` (from `serde_json::Error`). Both are checked before anything is queued.
8. **Changelogs and `AGENTS.md`.** Fragments are added under `crates/matrix-sdk-ui/changelog.d/` and
   `bindings/matrix-sdk-ffi/changelog.d/`, and root `AGENTS.md` records the SDK E2E exemption (§5.3).
9. **Checks.** Every command in §4 passes and nothing is waived.

## 2. Design

### 2.1 Module boundaries and data flow

```
bot ── PUT state org.matrix.msc4391.command_description/<key> ──▶ homeserver
homeserver ── sliding sync room_subscriptions.required_state (+MSC4391,"*") ──▶ matrix-sdk-base msc4186 processor
  ──▶ state store ──▶ Room::subscribe_to_state_events / FFI Room.subscribeToStateEvents (existing, unchanged)

Android composer ── FFI Timeline.sendReplyWithExtraContent(msg, eventId, json, addMentions)
  ──▶ matrix-sdk-ui Timeline::send_reply_with_extra_content
      ──▶ infer_reply(Some(id)) [thread-aware] ──▶ override add_mentions ──▶ Room::make_reply_event
      ──▶ Timeline::send_with_extra_content (relation already set → no re-inference)
      ──▶ RoomSendQueue::send(..).with_extra_content(..) ──▶ local echo + PUT /send
```

Changed crates: `matrix-sdk-ui` (room list service and timeline) and `matrix-sdk-ffi` (timeline binding). The
`matrix-sdk`, `matrix-sdk-base` and ruma crates don't change.

### 2.2 `crates/matrix-sdk-ui/src/room_list_service/mod.rs`

```rust
use std::sync::LazyLock; // merged into the existing `std` import

/// The default `required_state` value for sliding sync room subscriptions,
/// added to `DEFAULT_REQUIRED_STATE`. Only opened (subscribed) rooms need it.
///
/// It's a `LazyLock` because `StateEventType::from` isn't `const`.
static DEFAULT_ROOM_SUBSCRIPTION_EXTRA_REQUIRED_STATE: LazyLock<[(StateEventType, &str); 2]> =
    LazyLock::new(|| {
        [
            (StateEventType::RoomPinnedEvents, ""),
            // MSC4391 bot command descriptions, one state event per command.
            (StateEventType::from("org.matrix.msc4391.command_description"), "*"),
        ]
    });
```

`room_subscription_settings()` stays the same: `DEFAULT_ROOM_SUBSCRIPTION_EXTRA_REQUIRED_STATE.iter()` now derefs the
`LazyLock`. `DEFAULT_REQUIRED_STATE` and the list builder (`.required_state(...)` at ~line 236) don't change.

Unit test, added to the existing `mod tests` in the same file:

```rust
#[test]
fn test_room_subscription_required_state_includes_msc4391_command_descriptions() {
    // room_subscription_settings() ends with pinned_events then (msc4391 type, "*"),
    // and DEFAULT_REQUIRED_STATE (lists) contains no msc4391 entry.
}
```

### 2.3 `crates/matrix-sdk-ui/src/timeline/mod.rs`

```rust
#[instrument(skip(self, content))]
pub async fn send_reply(
    &self,
    content: RoomMessageEventContentWithoutRelation,
    in_reply_to: OwnedEventId,
) -> Result<SendHandle, Error> {
    self.send_reply_with_extra_content(content, in_reply_to, AddMentions::Yes, None).await
}

/// Like [`Self::send_reply`], but with additional top-level fields merged
/// into the content (the event's own fields win on conflicts, as in
/// [`Self::send_with_extra_content`]).
///
/// With `add_mentions` set to [`AddMentions::Yes`], the replied-to sender is
/// added to `m.mentions`, unless it is the current user. With
/// [`AddMentions::No`], `content`'s mentions are sent unchanged.
#[instrument(skip(self, content, extra_content))]
pub async fn send_reply_with_extra_content(
    &self,
    content: RoomMessageEventContentWithoutRelation,
    in_reply_to: OwnedEventId,
    add_mentions: AddMentions,
    extra_content: Option<serde_json::Map<String, serde_json::Value>>,
) -> Result<SendHandle, Error> {
    let mut reply = self
        .infer_reply(Some(in_reply_to))
        .await
        .expect("the reply will always be set because we provided a replied-to event id");
    reply.add_mentions = add_mentions;
    let content = self.room().make_reply_event(content, reply).await?;
    self.send_with_extra_content(content.into(), extra_content).await
}
```

Notes:
- `send_with_extra_content` only infers a relation when `content.relation().is_none()`. A reply always has one, so the
  thread or reply relation from `make_reply_event` is kept. Today `send_reply` → `send` → `send_with_extra_content(…,
  None)` takes the same path, so delegating doesn't change behaviour.
- Thread handling comes from `infer_reply`. A thread-focused timeline gets
  `EnforceThread::Threaded(ReplyWithinThread::Yes)`, which gives `rel_type m.thread` and `is_falling_back: false`. A
  live timeline gets `MaybeThreaded`, which forwards the replied-to event's thread.
- `send_location`'s reply path calls `send_reply` and isn't touched.
- Errors: `ReplyError` from `make_reply_event` (unknown event, state event, deserialization) and send-queue errors
  convert through the existing `From` impls into `timeline::Error`. Nothing new is added. Don't log content or
  `extra_content`.

### 2.4 `bindings/matrix-sdk-ffi/src/timeline/mod.rs`

In the existing `#[matrix_sdk_ffi_macros::export] impl Timeline` block, next to `send_reply`:

```rust
/// Send a reply. See [`Self::send_reply_with_extra_content`].
pub async fn send_reply(
    &self,
    msg: Arc<RoomMessageEventContentWithoutRelation>,
    event_id: String,
) -> Result<Arc<SendHandle>, ClientError> {
    self.send_reply_with_extra_content(msg, event_id, None, true).await
}

/// Like [`Self::send_reply`], but merges the given additional top-level
/// fields (a JSON object, encoded as a string) into the reply's content.
/// When `add_mentions` is false, the replied-to sender isn't added to
/// `m.mentions`; the message's own mentions are sent unchanged.
pub async fn send_reply_with_extra_content(
    &self,
    msg: Arc<RoomMessageEventContentWithoutRelation>,
    event_id: String,
    extra_content_json: Option<String>,
    add_mentions: bool,
) -> Result<Arc<SendHandle>, ClientError> {
    let event_id = EventId::parse(&event_id).map_err(|_| RoomError::InvalidRepliedToEventId)?;
    let extra_content: Option<serde_json::Map<String, serde_json::Value>> =
        extra_content_json.map(|json| serde_json::from_str(&json)).transpose()?;
    let add_mentions = if add_mentions { AddMentions::Yes } else { AddMentions::No };
    let handle = self
        .inner
        .send_reply_with_extra_content((*msg).clone(), event_id, add_mentions, extra_content)
        .await?;
    Ok(Arc::new(SendHandle::new(handle)))
}
```

`AddMentions` is imported from `ruma::events::room::message` and added to the existing `room::message::{…}` import.
Error mapping uses the existing `From<RoomError>`, `From<serde_json::Error>` and timeline-error conversions into
`ClientError`. The UniFFI signature keeps the parameter order Android's D6 expects.

### 2.5 Conventions

- Rust style follows the repo `rustfmt.toml`, formatted with `nightly-2026-09-28`, and clippy with `-D warnings`.
- Doc comments go on every new public item. No new dependencies.
- Changelog fragments (S4):
  - `crates/matrix-sdk-ui/changelog.d/msc4391-command-description-required-state.added.md`: "`RoomListService` room
    subscriptions now also request the `org.matrix.msc4391.command_description` state (all state keys), so bot command
    descriptions (MSC4391) of opened rooms are available through `Room::state_events` and
    `Room::subscribe_to_state_events`. The room list's own `required_state` is unchanged."
  - `crates/matrix-sdk-ui/changelog.d/send-reply-with-extra-content.added.md`: "Add
    `Timeline::send_reply_with_extra_content`, which sends a reply with extra top-level content fields and lets the
    caller choose whether the replied-to sender is added to `m.mentions`."
  - `bindings/matrix-sdk-ffi/changelog.d/send-reply-with-extra-content.added.md`: "Add
    `Timeline::send_reply_with_extra_content`, …" (FFI wording, JSON-string parameter).
- `AGENTS.md` (new, repo root; S6) states that repository-level E2E testing is not required in `matrix-rust-sdk`
  (exempt), that downstream client features run their product E2E in the consuming client repository, that where agent
  E2E is required it runs only on initial feature addition and future pushes/deployments need not rerun it, and that
  existing unit and integration tests remain required.
- Commit: one Conventional Commit, e.g.
  `feat(ui): Request MSC4391 command descriptions and add Timeline::send_reply_with_extra_content`, with a body that
  explains why (sliding sync ignores timeline state).

## 3. Tasks

The change is small and tightly coupled (the FFI calls the UI API, and the tests exercise both UI changes), so it is
**one cohesive task in one wave**.

### Wave 1

**T1: MSC4391 room-subscription state and reply-with-extra-content (UI + FFI)**
- **Goal:** meet AC1–AC9.
- **Owned files:**
  - `crates/matrix-sdk-ui/src/room_list_service/mod.rs` (S2 static, unit test)
  - `crates/matrix-sdk-ui/src/timeline/mod.rs` (new method; `send_reply` delegates)
  - `crates/matrix-sdk-ui/tests/integration/room_list_service.rs`: add
    `["org.matrix.msc4391.command_description", "*"]` right after `["m.room.pinned_events", ""]` in all 7
    `room_subscriptions` expectations (~lines 2349, 2419, 2490, 2514, 2645, 2724, 2748). List expectations stay as
    they are.
  - `crates/matrix-sdk-ui/tests/integration/timeline/replies.rs`: two new tests, below.
    `mod replies;` is already registered (`tests/integration/timeline/mod.rs:65`), so **don't edit
    `tests/integration/timeline/mod.rs`**. It is a WIP file in the main checkout.
  - `bindings/matrix-sdk-ffi/src/timeline/mod.rs`
  - `crates/matrix-sdk-ui/changelog.d/msc4391-command-description-required-state.added.md` (new)
  - `crates/matrix-sdk-ui/changelog.d/send-reply-with-extra-content.added.md` (new)
  - `bindings/matrix-sdk-ffi/changelog.d/send-reply-with-extra-content.added.md` (new)
  - `AGENTS.md` (new, repo root; S6 E2E exemption)
- **Consumes:** `Timeline::infer_reply`, `Room::make_reply_event`, `Reply.add_mentions`,
  `Timeline::send_with_extra_content`, `MatrixMockServer` (`mock_room_send`, `sync_room`), `EventFactory`.
- **Produces:** the UI and FFI signatures in §2.3/§2.4, and the room-subscription `required_state` pair.
- **Dependencies:** none.
- **New integration tests** (`replies.rs`). They follow `test_send_reply` and `test_send_reply_enforce_thread_is_reply`.
  In `mock_room_send().respond_with`, assert on `req.body_json::<serde_json::Value>()` so the extra key is visible,
  use `.expect(1)`, and check the local echo and remote echo as the existing tests do.
  - `test_send_reply_with_extra_content`: live timeline, Bob's event synced, content
    `text_plain("/model gpt-5").add_mentions(Mentions::with_user_ids([CAROL]))`, `AddMentions::No`, extra
    `{"org.matrix.msc4391.command": {"command": "model", "arguments": {"name": "gpt-5"}}}`. Asserts AC4.
  - `test_send_reply_with_extra_content_in_thread`: Bob's `$root` and Bob's in-thread `$target`
    (`.in_thread($root, $root)`), timeline built with `TimelineFocus::Thread { thread_id: $root }`, reply to
    `$target` with the same content, `AddMentions::No` and extra. Asserts AC5. Copy the thread-timeline setup from
    `test_send_reply_enforce_thread_is_reply`, including any mocks it needs.
- **Unit test commands:**
  `cargo test -p matrix-sdk-ui --lib room_list_service` and
  `cargo test -p matrix-sdk-ui --test integration -- room_list_service timeline::replies`.

## 4. Test and verification

Run everything from the worktree. Stable Rust comes from the Hermes flake dev shell. Let
`DEV="nix develop /home/sinkerine/.paseo/worktrees/3b36k2jo/feat-hermes-slash -c"`.

| # | Check | Command |
|---|-------|---------|
| 1 | Integration tests (room list + replies) | `$DEV cargo test -p matrix-sdk-ui --test integration -- room_list_service timeline::replies` |
| 2 | Lib unit tests | `$DEV cargo test -p matrix-sdk-ui --lib room_list_service` |
| 3 | FFI type check / build | `$DEV cargo check -p matrix-sdk-ffi` |
| 4 | Lint | `$DEV cargo clippy -p matrix-sdk-ui -p matrix-sdk-ffi --all-targets -- -D warnings` |
| 5 | Format (repo style, `xtask ci style` = `rustup run nightly-2026-09-28 cargo fmt -- --check`) | see below |

Check 5 needs a nightly toolchain, and `rustup` is absent, so it uses the same `nightly-2026-09-28` toolchain built
through rust-overlay:

```sh
nix shell --impure --expr 'let f = builtins.getFlake "path:/home/sinkerine/.paseo/worktrees/3b36k2jo/feat-hermes-slash";
  pkgs = import f.inputs.nixpkgs { system = "x86_64-linux"; overlays = [ f.inputs.rust-overlay.overlays.default ]; };
  in pkgs.rust-bin.nightly."2026-09-28".minimal.override { extensions = [ "rustfmt" ]; }' \
  -c cargo fmt -- --check
```

Confirm with `cargo fmt --version` / `rustfmt --version` in that shell that it reports `nightly-2026-09-28`. If the
toolchain can't be built, stop and report a blocker. The check can't be waived.

Type checking is covered by #3 and #4. #4's `--all-targets` also builds the integration tests and the lib tests.
Before delivery, also confirm `git diff --stat main..feat/msc4391-bot-commands` touches only the owned files in §3.

## 5. E2E journey

### 5.1 SDK-level journey (run in this project; deterministic mock homeserver)

1. **Open a room.** `RoomListService::subscribe_to_rooms([room])`.
   **Expect:** the next sliding sync request's `room_subscriptions.<room>.required_state` ends with
   `m.room.pinned_events ""` and `org.matrix.msc4391.command_description "*"`, and `lists.all_rooms.required_state`
   has no MSC4391 entry. (Covered by the `room_list_service` integration tests.)
2. **Reply to another user with a bot command (live).**
   **Expect:** the AC4 wire content: `m.in_reply_to`, the command block, and `m.mentions` holding only the bot.
   (`test_send_reply_with_extra_content`)
3. **Reply inside a thread.**
   **Expect:** the AC5 wire content: `rel_type m.thread`, root, `m.in_reply_to` target, `is_falling_back: false`, the
   command block, and unchanged mentions. (`test_send_reply_with_extra_content_in_thread`)
4. **Legacy reply.** Run the existing `send_reply` tests.
   **Expect:** they all pass unchanged (AC6).

### 5.2 Product journey (downstream, NOT claimed by this project)

The product E2E is the Android plan's §5 (dedicated Synapse and emulator, stand-in bots). Steps 1–2 (published
descriptions reach `/` suggestions, which depends on S1), 7 (reply: `m.in_reply_to` set, mentions only the bot) and
8 (thread and thread reply, `is_falling_back: false`) exercise this SDK change. The Android orchestrator runs it as
Android T6 after consuming `SDK_READY <sha>`. This plan doesn't report it as done.

### 5.3 E2E policy for this repository (S6, chosen by the user)

Add root `AGENTS.md` recording an explicit SDK E2E exemption:
- Repository-level E2E testing is not required in `matrix-rust-sdk`; for SDK changes supporting a downstream client,
  the product E2E journey runs in the consuming client repository (§5.2).
- Existing unit, integration, lint and formatting checks remain required.
- Where agent E2E applies, it is required only when initially adding a feature; future pushes or deployments do not
  require rerunning E2E.

## 6. Delivery (for the orchestrator)

- No push, PR or deploy.
- The only preapproved landing is `git -C /home/sinkerine/dev/matrix-rust-sdk merge --ff-only
  feat/msc4391-bot-commands`, and only if the fast-forward touches no WIP file. Recheck `git status` at delivery.
  Known WIP:
  - `bindings/matrix-sdk-ffi/src/{client.rs,thread_inbox.rs}`;
  - `crates/matrix-sdk-common/src/deserialized_responses.rs`;
  - `crates/matrix-sdk-ui/src/thread_inbox.rs`;
  - `crates/matrix-sdk-ui/src/timeline/controller/{aggregations.rs,mod.rs}` and
    `crates/matrix-sdk-ui/src/timeline/{tasks.rs,tests/mod.rs}`;
  - `crates/matrix-sdk-ui/tests/integration/timeline/{mod.rs,thread.rs}`;
  - `crates/matrix-sdk/src/event_cache/persistence.rs`;
  - untracked `crates/matrix-sdk-ui/src/timeline/tests/edit_ordering.rs` and
    `crates/matrix-sdk-ui/tests/integration/timeline/edit_ordering.rs`.

  T1's owned files don't overlap this list. It edits `timeline/mod.rs` (the src file), not `timeline/tests/mod.rs` or
  `tests/integration/timeline/mod.rs`. Git still refuses a fast-forward if any touched path is dirty. In that case,
  stop and ask. Never stash, reset, checkout or discard.
- Then send exactly: `paseo send 87b07a20-b96a-4a1e-9e12-d85eca5272c8 --no-wait --prompt "SDK_READY <commit sha> on
  matrix-rust-sdk branch feat/msc4391-bot-commands (main fast-forwarded: yes|no): MSC4391 room-subscription
  required_state + Timeline.sendReplyWithExtraContent FFI. <notes>"`

## 7. Open notes

- With `"*"`, the server sends every `command_description` state event of a subscribed room in `required_state`.
  That costs a little more for opened rooms only. Rooms in the list don't change.
