`RoomListService` room subscriptions now also request the
`org.matrix.msc4391.command_description` state (all state keys), so bot command
descriptions (MSC4391) of opened rooms are available through `Room::state_events`
and `Room::subscribe_to_state_events`. The room list's own `required_state` is unchanged.
