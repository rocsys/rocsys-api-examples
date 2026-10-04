# Rocsys API Examples

Example clients and tools for the [Rocsys Connected API](https://api.rocsys.com).

## Sub-projects

### [sse-client-javascript](sse-client-javascript/)

A minimalistic Node.js (22+) SSE client for the `/rocsys-api/v3/sse-subscriptions` event stream. Handles reconnection with `Last-Event-ID`, dead-connection detection, token expiry, and duplicate suppression. No dependencies.

### [sse-client-python](sse-client-python/)

A Python 3.9+ SSE client for the same event stream. Designed to be used as a starting point for integrations — import `events()` to consume events reliably in your own code. Standard library only, nothing to install.

### [webhook-listener-javascript](webhook-listener-javascript/)

A minimalistic Node.js webhook listener for testing purposes.

### [webhook-listener-python](webhook-listener-python/)

A minimalistic Python 3.9+ webhook listener for testing purposes. Standard library only, nothing to install.
