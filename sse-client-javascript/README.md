# SSE Client

## introduction

Minimalistic client for testing the SSE event stream (`/rocsys-api/v3/sse-subscriptions`) for testing purposes. It
behaves like a well-behaved customer client: it reconnects with `Last-Event-ID` after the server's `retry:` delay
(plus jitter), treats the stream as dead after 60s without data or pings, logs in again when the token is rejected,
and flags events it receives twice (the replay may re-send an event; clients deduplicate on its `id`).

No dependencies, Node 22 or later.

## credentials

Either an existing bearer token, or the credentials of a Rocsys API user (the client then logs in itself; the login
is limited to 5 calls per minute, so the token is reused until it expires):

```bash
export ROCSYS_API_URL=https://api.rocsys.com   # the default
export ROCSYS_API_TOKEN=...
# or
export ROCSYS_API_EMAIL=...
export ROCSYS_API_PASSWORD=...
```

## run

```bash
node index.js                        # stream the first SSE subscription (created if there is none)
node index.js stream <uuid>          # stream a specific subscription
node index.js stream <uuid> --last-event-id <id>   # start with a replay after <id>
node index.js stream <uuid> --quiet  # only one line per event, no payloads
node index.js stream <uuid> -v       # also show the ': ping' heartbeats

node index.js list
node index.js create ACD_CYCLE ACD_STATUS --acd <acd-uuid>   # no message types / ACDs: everything
node index.js disable <uuid>         # the stream then answers 409 SUBSCRIPTION_DISABLED
node index.js enable <uuid>
node index.js delete <uuid>
```

## things to try

- **Replay**: stop the client (ctrl-c) and note the last `id=`, let some events happen, then start it again with
  `--last-event-id <id>`: the missed events arrive first.
- **Several clients**: start a second client on the same subscription; both stay connected and both receive every
  event. cloud-core logs a warning once a subscription has more than 3 open connections.
- **Deploy / restart**: restart cloud-core; the client reconnects and replays what it missed. A `SIGINT` (ctrl-c on
  `pnpm start`) or `SIGTERM` ends the stream cleanly ("stream closed by the server"), so the client reconnects after
  `retry:` instead of waiting for its watchdog.
- **Inactivity**: a subscription nobody connected to for 3 days is disabled (`disabledReason: INACTIVITY`) by the
  hourly job; `list` shows it. Starting the client on it enables it again silently. Events from the disabled period
  are not replayed.
- **Revocation**: `disable <uuid>` or `delete <uuid>` from a second terminal; within about 20s (one heartbeat) the
  open stream ends and the reconnect gets the 409 / 404. With `ROCSYS_API_TOKEN` set to a short-lived token, the
  stream likewise ends on the first heartbeat after the token expires.
