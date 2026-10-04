"""Tests for sse_client.py, against a small fake server. Run with: python3 -m unittest -v"""

import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

import sse_client
from sse_client import Event, RecentIds, RocsysApi, SseParser, StreamRefused


def parse(text):
    parser = SseParser()
    found = []
    for line in text.split("\n"):
        event = parser.feed(line)
        if event:
            found.append(event)
    return parser, found


class SseParserTest(unittest.TestCase):
    def test_parses_frames(self):
        parser, found = parse(
            "retry: 5000\n\n"
            ": ping\n\n"
            "id: e1\nevent: ACDCycle\ndata: {\"n\": 1}\n\n"
            "data: line 1\ndata: line 2\n\n"
        )
        self.assertEqual(parser.retry_ms, 5000)
        self.assertEqual(found, [
            Event("e1", "ACDCycle", '{"n": 1}'),
            Event(None, "message", "line 1\nline 2"),
        ])
        self.assertEqual(found[0].payload(), {"n": 1})

    def test_ignores_frames_without_data_and_unknown_fields(self):
        _, found = parse("id: e1\nevent: x\n\nfoo: bar\n\n")
        self.assertEqual(found, [])


class RecentIdsTest(unittest.TestCase):
    def test_remembers_a_limited_number_of_ids(self):
        recent = RecentIds(size=2)
        self.assertFalse(recent.seen_before("a"))
        self.assertTrue(recent.seen_before("a"))
        recent.seen_before("b")
        recent.seen_before("c")
        # "a" was forgotten to make room
        self.assertFalse(recent.seen_before("a"))


class FakeServer:
    """Serves one scripted response per stream request, and records the requests."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                server.requests.append(("POST", self.path, self.headers))
                body = json.dumps({"accessKey": f"token-{len(server.requests)}", "expiresIn": "86400"}).encode()
                self.send_response(201)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                server.requests.append(("GET", self.path, self.headers))
                status, body, hang = server.responses.pop(0) if server.responses else (404, "done", 0)
                self.send_response(status)
                self.send_header("Content-Type", "text/event-stream" if status == 200 else "application/json")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body.encode())
                self.wfile.flush()
                # not time.sleep: the tests mock that to skip the client's reconnect delays
                threading.Event().wait(hang)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def stream_requests(self):
        return [headers for method, _, headers in self.requests if method == "GET"]

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class EventsTest(unittest.TestCase):
    def setUp(self):
        # no real waiting between reconnects
        patcher = mock.patch.object(sse_client.time, "sleep")
        self.sleep = patcher.start()
        self.addCleanup(patcher.stop)

    def serve(self, *responses):
        server = FakeServer(responses)
        self.addCleanup(server.close)
        return server

    def take(self, api, count, last_event_id=None):
        stream = sse_client.events(api, "sub-1", last_event_id)
        return [next(stream) for _ in range(count)]

    def test_reconnects_with_the_last_event_id_and_skips_duplicates(self):
        server = self.serve(
            (200, "retry: 3000\n\nid: e1\nevent: ACDCycle\ndata: {}\n\n", 0),
            # the replay re-sends e1 (at-least-once delivery)
            (200, "id: e1\nevent: ACDCycle\ndata: {}\n\nid: e2\nevent: ACDCycle\ndata: {}\n\n", 0),
        )
        api = RocsysApi(server.url, token="t")
        self.assertEqual([event.id for event in self.take(api, 2, "e0")], ["e1", "e2"])
        first, second = server.stream_requests()
        self.assertEqual(first["Last-Event-ID"], "e0")
        self.assertEqual(second["Last-Event-ID"], "e1")
        self.assertEqual(first["Authorization"], "Bearer t")
        # the server's retry: (3s) plus jitter of up to 1s
        delay = self.sleep.call_args_list[0].args[0]
        self.assertTrue(3 <= delay <= 4, delay)

    def test_logs_in_again_when_the_token_is_rejected(self):
        server = self.serve(
            (401, "{}", 0),
            (200, "id: e1\ndata: {}\n\n", 0),
        )
        api = RocsysApi(server.url, email="a@example.com", password="secret", token="expired")
        self.take(api, 1)
        self.assertEqual(
            [(method, path) for method, path, _ in server.requests],
            [("GET", "/rocsys-api/v3/sse-subscriptions/sub-1/stream"),
             ("POST", "/rocsys-api/v3/login"),
             ("GET", "/rocsys-api/v3/sse-subscriptions/sub-1/stream")],
        )
        self.assertEqual(server.stream_requests()[1]["Authorization"], "Bearer token-2")

    def test_stops_on_404_and_409(self):
        for status in (404, 409):
            with self.subTest(status=status):
                server = self.serve((status, '{"code": "SUBSCRIPTION_DISABLED"}', 0))
                with self.assertRaises(StreamRefused):
                    self.take(RocsysApi(server.url, token="t"), 1)

    def test_backs_off_after_repeated_failures(self):
        server = self.serve(
            (503, "{}", 0),
            (503, "{}", 0),
            (503, "{}", 0),
            (200, "id: e1\ndata: {}\n\n", 0),
        )
        self.take(RocsysApi(server.url, token="t"), 1)
        delays = [call.args[0] for call in self.sleep.call_args_list]
        # the default retry (5s), then doubling, each plus up to 1s of jitter
        self.assertEqual(len(delays), 3)
        for delay, base in zip(delays, (5, 10, 20)):
            self.assertTrue(base <= delay <= base + 1, (delay, base))

    def test_reconnects_when_nothing_arrives_within_the_watchdog_time(self):
        server = self.serve(
            (200, "retry: 1000\n\n", 3),
            (200, "id: e1\ndata: {}\n\n", 0),
        )
        with mock.patch.object(sse_client, "WATCHDOG_SECONDS", 0.5):
            started = time.monotonic()
            self.take(RocsysApi(server.url, token="t"), 1)
        self.assertLess(time.monotonic() - started, 2.5)
        self.assertEqual(len(server.stream_requests()), 2)


if __name__ == "__main__":
    unittest.main()
