import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

API_SECRET = os.environ.get('ROCSYS_WEBHOOK_SECRET', 'TOPSECRET')
PORT = int(os.environ.get('PORT', 9000))


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != '/webhook':
            self.send_response(404)
            self.end_headers()
            return

        print('------ Received a webhook message')

        if self.headers.get('Authorization') != f'Bearer {API_SECRET}':
            print('Unauthorized')
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b'Unauthorized')
            return

        print('Authorization OK')

        event_type = self.headers.get('X-Rocsys-Api-Type')
        if event_type:
            print(f'Got a {event_type} message from Rocsys API:')

        length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        print(json.dumps(body, indent=2))

        print('------ Done processing the webhook message')
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'ok')

    def log_message(self, format, *args):
        pass  # silence the default request log; our own prints are enough


if __name__ == '__main__':
    server = HTTPServer(('', PORT), WebhookHandler)
    print(f'Webhook listener listening on port {PORT}')
    server.serve_forever()
