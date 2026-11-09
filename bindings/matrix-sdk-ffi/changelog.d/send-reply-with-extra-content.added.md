Add `Timeline::send_reply_with_extra_content`, which sends a reply with extra
top-level content fields supplied as a JSON object encoded as a string, and lets
the caller choose whether the replied-to sender is added to `m.mentions`.
