# Python SSE client example

An example client for the Rocsys Connected API event stream (`/rocsys-api/v3/sse-subscriptions`). It shows how to
receive events reliably over Server-Sent Events (SSE), without running a public webhook endpoint. Use it as a starting
point for your own integration.

What it handles for you:

- **Reconnecting.** A stream can end at any time, for example when we deploy a new version or your access token
  expires. The client reconnects after the delay the server asks for (`retry:`), plus a random jitter, waiting longer
  after repeated failures.
- **No missed events.** On reconnect it sends the id of the last event it received (`Last-Event-ID`), and the server
  first replays everything stored after it. Events are kept for at least 3 days.
- **Dead connections.** The server sends a `: ping` every 20 seconds. If nothing arrives for 60 seconds, the client
  treats the connection as dead and reconnects.
- **Token expiry.** It logs in again when the server rejects the access token.
- **Duplicates.** Delivery is at-least-once: after a reconnect, an event you already received can be sent again. The
  client skips events whose id it has seen recently.

Requires Python 3.9 or later. It uses only the Python standard library, so there is nothing to install.

## Credentials

Set the email address and password of your Rocsys Connected API account. The client logs in and reuses the access
token until it expires:

```bash
export ROCSYS_API_EMAIL=you@example.com
export ROCSYS_API_PASSWORD=...
# or, instead, an access token you already have:
export ROCSYS_API_TOKEN=...
```

The client talks to `https://api.rocsys.com` by default. Set `ROCSYS_API_URL` to use another environment.

## Run it

```bash
python3 sse_client.py create ACD_CYCLE ACD_STATUS   # create an SSE subscription; no message types: all
python3 sse_client.py create --acd <acd-uuid>       # only events of this ACD; repeat --acd for more
python3 sse_client.py list                          # your SSE subscriptions

python3 sse_client.py stream <uuid>                 # stream the events of a subscription
python3 sse_client.py stream <uuid> --state-file last-event-id   # resume where you left off after a restart
python3 sse_client.py stream <uuid> --quiet         # one line per event, without the payload
python3 sse_client.py -v stream <uuid>              # also show the ': ping' heartbeats

python3 sse_client.py disable <uuid>                # stop the subscription; its stream ends
python3 sse_client.py enable <uuid>
python3 sse_client.py delete <uuid>
```

Each event is printed with its type (the same value a webhook receives in the `X-Rocsys-Api-Type` header, for example
`ACDCycle`), its id, and its payload (the same JSON body a webhook receives).

## Use it in your own code

`events()` yields events for as long as you iterate over it, and reconnects by itself:

```python
from sse_client import RocsysApi, events

api = RocsysApi("https://api.rocsys.com", email="you@example.com", password="...")

last_event_id = load_last_event_id()          # None the first time
for event in events(api, "<subscription-uuid>", last_event_id):
    process(event.type, event.payload())      # your processing
    save_last_event_id(event.id)              # only after processing
```

- **Save the event id after processing it,** not before. Then a crash means the event is sent again on restart,
  rather than lost.
- **Make your processing safe to run twice for the same event.** The client only skips duplicates it remembers since
  it started, so after a restart an event can come through again. Use the event id to recognize it.
- **One subscription, several connections:** every open connection receives every event. If you run several
  consumers, either deduplicate on the event id, or give each consumer its own subscription.
- **Order:** events usually arrive in the order they happened, but not always. Use the timestamps in the payload when
  the order matters.

`events()` stops with `StreamRefused` when reconnecting can't help: the subscription doesn't exist (404), or it was
disabled (409). Enable it again with `PUT enabled: true`, or `python3 sse_client.py enable <uuid>`.

## Long disconnects

Events are kept for 3 days, and a subscription nobody is connected to for 3 days is disabled automatically
(`disabledReason: INACTIVITY`); no events are stored for it while it is disabled. Connecting to it again enables it
again, silently, but the events from that period can't be replayed. After a disconnect longer than 3 days, get the
current state through the REST API before relying on the stream again.

## Tests

```bash
python3 -m unittest -v
```
