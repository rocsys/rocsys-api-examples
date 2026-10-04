#!/usr/bin/env python3
"""Example client for Rocsys Connected API SSE subscriptions (/rocsys-api/v3/sse-subscriptions).

Shows how to receive events reliably over Server-Sent Events:

- reconnect after the server's ``retry:`` delay, plus random jitter, when the stream ends;
- resume with ``Last-Event-ID``, so events stored while disconnected are replayed;
- treat the stream as dead when nothing (no event, no ``: ping``) arrives for 60 seconds;
- log in again when the access token expires;
- skip events received twice: delivery is at-least-once, so a replay may re-send an event.

Python 3.9 or later, standard library only. See README.md for usage.
"""

import argparse
import json
import logging
import os
import random
import sys
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Iterator, Optional

DEFAULT_BASE_URL = "https://api.rocsys.com"
# the server sends a ': ping' every 20 seconds; nothing for this long means the connection is dead
WATCHDOG_SECONDS = 60
# used until the server sends its own 'retry:' value
DEFAULT_RETRY_MS = 5000
# the longest wait between reconnect attempts while the server can't be reached
MAX_RECONNECT_DELAY_SECONDS = 60
# how many event ids to remember for skipping duplicates
REMEMBERED_EVENT_IDS = 10_000

log = logging.getLogger("sse-client")


class ApiError(Exception):
    def __init__(self, status: int, body: str):
        super().__init__(f"HTTP {status}: {body}")
        self.status = status
        self.body = body


class StreamRefused(Exception):
    """The stream can't be opened, and reconnecting won't change that (404 unknown, 409 disabled)."""


class RocsysApi:
    """Minimal Rocsys Connected API client: logs in when needed and reuses the token until it expires."""

    def __init__(self, base_url: str, email: Optional[str] = None, password: Optional[str] = None,
                 token: Optional[str] = None):
        self.api_url = base_url.rstrip("/") + "/rocsys-api/v3"
        self._email = email
        self._password = password
        self._token = token
        # a token passed in directly is used until the server rejects it
        self._token_valid_until = float("inf") if token else 0.0

    def token(self) -> str:
        if self._token and time.time() < self._token_valid_until:
            return self._token
        if not (self._email and self._password):
            raise RuntimeError("set ROCSYS_API_TOKEN, or ROCSYS_API_EMAIL and ROCSYS_API_PASSWORD")
        # logins are rate limited, so the token is reused until shortly before it expires
        response = self._send("POST", "/login", {"email": self._email, "password": self._password},
                              authenticate=False)
        self._token = response["accessKey"]
        expires_in = int(response["expiresIn"])
        self._token_valid_until = time.time() + expires_in - 60
        log.info("logged in as %s, token valid for %ss", self._email, expires_in)
        return self._token

    def forget_token(self) -> None:
        self._token = None
        self._token_valid_until = 0.0

    def request(self, method: str, path: str, body: Any = None) -> Any:
        return self._send(method, path, body)

    def open_stream(self, subscription_uuid: str, last_event_id: Optional[str]):
        """Opens the event stream; the read timeout doubles as the watchdog."""
        headers = {"Authorization": f"Bearer {self.token()}", "Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        request = urllib.request.Request(
            f"{self.api_url}/sse-subscriptions/{subscription_uuid}/stream", headers=headers)
        return urllib.request.urlopen(request, timeout=WATCHDOG_SECONDS)

    def _send(self, method: str, path: str, body: Any, authenticate: bool = True) -> Any:
        headers = {"Accept": "application/json"}
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body).encode()
        if authenticate:
            headers["Authorization"] = f"Bearer {self.token()}"
        request = urllib.request.Request(self.api_url + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                text = response.read().decode()
        except urllib.error.HTTPError as error:
            raise ApiError(error.code, error.read().decode()) from None
        return json.loads(text) if text else None


@dataclass
class Event:
    id: Optional[str]
    type: str
    data: str

    def payload(self) -> Any:
        """The event body: the same JSON a webhook would receive."""
        return json.loads(self.data)


class SseParser:
    """Turns the lines of a text/event-stream into events (https://html.spec.whatwg.org/#event-stream-interpretation)."""

    def __init__(self):
        self.retry_ms: Optional[int] = None
        self._id: Optional[str] = None
        self._type = ""
        self._data: list = []

    def feed(self, line: str) -> Optional[Event]:
        """Feeds one line (without its line ending); returns an event when a blank line completes one."""
        if line == "":
            event = None
            if self._data:
                event = Event(self._id, self._type or "message", "\n".join(self._data))
            self._id, self._type, self._data = None, "", []
            return event
        if line.startswith(":"):
            # a comment, e.g. the server's ': ping' heartbeat
            log.debug("%s", line)
            return None
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "data":
            self._data.append(value)
        elif field == "event":
            self._type = value
        elif field == "id":
            self._id = value
        elif field == "retry" and value.isdigit():
            self.retry_ms = int(value)
        return None


class RecentIds:
    """Remembers the most recent event ids, to skip an event that is delivered twice."""

    def __init__(self, size: int = REMEMBERED_EVENT_IDS):
        self._ids: OrderedDict = OrderedDict()
        self._size = size

    def seen_before(self, event_id: str) -> bool:
        if event_id in self._ids:
            return True
        self._ids[event_id] = None
        if len(self._ids) > self._size:
            self._ids.popitem(last=False)
        return False


def events(api: RocsysApi, subscription_uuid: str, last_event_id: Optional[str] = None) -> Iterator[Event]:
    """
    Yields the subscription's events for as long as you iterate, reconnecting whenever the stream ends.

    Store ``event.id`` once you have processed an event, and pass it as ``last_event_id`` when you start again:
    the server then replays what you missed (events are kept for at least 3 days).
    """
    retry_ms = DEFAULT_RETRY_MS
    failures = 0
    recent = RecentIds()
    while True:
        try:
            with api.open_stream(subscription_uuid, last_event_id) as response:
                log.info("connected to %s%s", subscription_uuid,
                         f" (Last-Event-ID {last_event_id})" if last_event_id else "")
                failures = 0
                parser = SseParser()
                for raw_line in response:
                    event = parser.feed(raw_line.decode("utf-8").rstrip("\r\n"))
                    if parser.retry_ms is not None:
                        retry_ms = parser.retry_ms
                    if event is None:
                        continue
                    if event.id:
                        last_event_id = event.id
                        if recent.seen_before(event.id):
                            log.info("skipping duplicate event %s", event.id)
                            continue
                    yield event
            # a normal end: a deploy, the token expired, the subscription was disabled or deleted, ...
            log.info("stream closed by the server")
        except urllib.error.HTTPError as error:
            body = error.read().decode(errors="replace")
            if error.code == 401:
                log.info("token rejected, logging in again")
                api.forget_token()
            elif error.code in (404, 409):
                # unknown or deleted (404), or disabled with enabled: false (409)
                raise StreamRefused(f"HTTP {error.code}: {body}") from None
            else:
                failures += 1
                log.warning("HTTP %s: %s", error.code, body)
        except OSError as error:
            # connection refused, reset, or the watchdog timeout: nothing arrived for WATCHDOG_SECONDS
            failures += 1
            log.warning("connection lost: %s", error)
        # wait as the server asked, longer after repeated failures, plus jitter so that not every client
        # reconnects at the same moment after a deploy
        backoff = 2 ** max(failures - 1, 0)
        delay = min(retry_ms / 1000 * backoff, MAX_RECONNECT_DELAY_SECONDS) + random.uniform(0, 1)
        log.info("reconnecting in %.1fs", delay)
        time.sleep(delay)


# ---------------------------------------------------------------------------------------------------------------
# Command line


def handle_event(event: Event, quiet: bool) -> None:
    """Replace this with your own processing. It must be safe to receive an event twice."""
    print(f"{event.type} id={event.id}", flush=True)
    if not quiet:
        print(json.dumps(event.payload(), indent=2), flush=True)


def read_last_event_id(path: Optional[str]) -> Optional[str]:
    if path and os.path.exists(path):
        with open(path) as file:
            return file.read().strip() or None
    return None


def write_last_event_id(path: str, event_id: str) -> None:
    # write and rename, so a crash never leaves a half-written file
    temporary = path + ".tmp"
    with open(temporary, "w") as file:
        file.write(event_id)
    os.replace(temporary, path)


def stream(api: RocsysApi, args: argparse.Namespace) -> None:
    subscription_uuid = args.uuid
    if not subscription_uuid:
        existing = api.request("GET", "/sse-subscriptions?limit=1")
        subscription_uuid = existing[0]["uuid"] if existing else api.request("POST", "/sse-subscriptions", {})["uuid"]
        log.info("using SSE subscription %s", subscription_uuid)
    last_event_id = args.last_event_id or read_last_event_id(args.state_file)
    for event in events(api, subscription_uuid, last_event_id):
        handle_event(event, args.quiet)
        # only after processing: a crash before this line means the event is replayed, not lost
        if args.state_file and event.id:
            write_last_event_id(args.state_file, event.id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Example client for Rocsys Connected API SSE subscriptions.")
    parser.add_argument("-v", "--verbose", action="store_true", help="also show the ': ping' heartbeats")
    commands = parser.add_subparsers(dest="command")

    stream_parser = commands.add_parser("stream", help="stream a subscription's events (the default)")
    stream_parser.add_argument("uuid", nargs="?", help="the subscription; default: your first one, created if needed")
    stream_parser.add_argument("--last-event-id", help="start with a replay of the events after this one")
    stream_parser.add_argument("--state-file", help="keep the last processed event id here, to resume after a restart")
    stream_parser.add_argument("-q", "--quiet", action="store_true", help="one line per event, no payloads")

    commands.add_parser("list", help="list your SSE subscriptions")
    create_parser = commands.add_parser("create", help="create an SSE subscription")
    create_parser.add_argument("message_types", nargs="*", help="e.g. ACD_CYCLE ACD_STATUS; none: all")
    create_parser.add_argument("--acd", action="append", default=[], help="an ACD UUID; repeat for more; none: all")
    for name in ("enable", "disable", "delete"):
        commands.add_parser(name, help=f"{name} an SSE subscription").add_argument("uuid")

    args = parser.parse_args()
    if args.command is None:
        args = parser.parse_args(sys.argv[1:] + ["stream"])

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="[%(asctime)s] %(message)s", stream=sys.stderr)
    api = RocsysApi(os.environ.get("ROCSYS_API_URL", DEFAULT_BASE_URL),
                    email=os.environ.get("ROCSYS_API_EMAIL"),
                    password=os.environ.get("ROCSYS_API_PASSWORD"),
                    token=os.environ.get("ROCSYS_API_TOKEN"))
    try:
        if args.command == "stream":
            stream(api, args)
        elif args.command == "list":
            print(json.dumps(api.request("GET", "/sse-subscriptions?limit=100"), indent=2))
        elif args.command == "create":
            body = {"messageTypes": args.message_types, "acdUUIDs": args.acd}
            print(json.dumps(api.request("POST", "/sse-subscriptions", body), indent=2))
        elif args.command in ("enable", "disable"):
            body = {"enabled": args.command == "enable"}
            print(json.dumps(api.request("PUT", f"/sse-subscriptions/{args.uuid}", body), indent=2))
        elif args.command == "delete":
            print(json.dumps(api.request("DELETE", f"/sse-subscriptions/{args.uuid}"), indent=2))
    except (ApiError, StreamRefused, RuntimeError) as error:
        log.error("%s", error)
        sys.exit(1)
    except KeyboardInterrupt:
        log.info("bye")


if __name__ == "__main__":
    main()
