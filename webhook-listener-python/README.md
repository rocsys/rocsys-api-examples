# Webhook Listener (Python)

Minimalistic webhook listener for testing purposes. Python 3.9 or later, standard library only.

## credentials

Set the expected bearer token (defaults to `TOPSECRET`):

```bash
export ROCSYS_WEBHOOK_SECRET=...
```

## run

```bash
python3 webhook_listener.py
```

Listens on port 9000 by default. Override with `PORT=<n>`.
