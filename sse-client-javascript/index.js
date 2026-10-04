// Minimalistic SSE client for testing /rocsys-api/v3/sse-subscriptions. No dependencies (Node 22+ fetch).
// See README.md.

const BASE_URL = (process.env.ROCSYS_API_URL || 'https://api.rocsys.com').replace(/\/$/, '');
const API = `${BASE_URL}/rocsys-api/v3`;
// a client treats the stream as dead when nothing (no event, no ping) arrives for this long
const WATCHDOG_MS = 60_000;

let token = process.env.ROCSYS_API_TOKEN || null;
let tokenExpiresAt = token ? Infinity : 0;

const log = (...args) => console.log(`[${new Date().toISOString()}]`, ...args);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function getToken() {
  if (token && Date.now() < tokenExpiresAt) {
    return token;
  }
  const email = process.env.ROCSYS_API_EMAIL;
  const password = process.env.ROCSYS_API_PASSWORD;
  if (!email || !password) {
    throw new Error('set ROCSYS_API_TOKEN, or ROCSYS_API_EMAIL and ROCSYS_API_PASSWORD');
  }
  // the login is limited to 5 calls per minute, so the token is reused until (just before) it expires
  const res = await fetch(`${API}/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) {
    throw new Error(`login failed: ${res.status} ${await res.text()}`);
  }
  const { accessKey, expiresIn } = await res.json();
  token = accessKey;
  tokenExpiresAt = Date.now() + (expiresIn - 60) * 1000;
  log(`logged in as ${email}, token valid for ${expiresIn}s`);
  return token;
}

async function api(method, path, body) {
  const res = await fetch(`${API}${path}`, {
    method,
    headers: {
      authorization: `Bearer ${await getToken()}`,
      ...(body ? { 'content-type': 'application/json' } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`${method} ${path}: ${res.status} ${text}`);
  }
  return { body: text ? JSON.parse(text) : null, headers: res.headers };
}

// Parses SSE frames out of a growing text buffer; returns the unparsed rest.
function parseFrames(buffer, onFrame) {
  const normalized = buffer.replace(/\r\n?/g, '\n');
  const frames = normalized.split('\n\n');
  const rest = frames.pop();
  for (const raw of frames) {
    const frame = { comments: [], data: [] };
    for (const line of raw.split('\n')) {
      if (line.startsWith(':')) {
        frame.comments.push(line.slice(1).trim());
        continue;
      }
      const colon = line.indexOf(':');
      const field = colon === -1 ? line : line.slice(0, colon);
      const value = colon === -1 ? '' : line.slice(colon + 1).replace(/^ /, '');
      if (field === 'data') frame.data.push(value);
      else if (field === 'id' || field === 'event' || field === 'retry') frame[field] = value;
    }
    onFrame(frame);
  }
  return rest;
}

async function stream(uuid, options) {
  let lastEventId = options.lastEventId || null;
  let retryMs = 5000;
  let received = 0;
  const seen = new Set();

  for (;;) {
    const controller = new AbortController();
    let watchdog;
    const resetWatchdog = () => {
      clearTimeout(watchdog);
      watchdog = setTimeout(() => {
        log(`nothing received for ${WATCHDOG_MS / 1000}s, reconnecting`);
        controller.abort();
      }, WATCHDOG_MS);
    };

    try {
      const res = await fetch(`${API}/sse-subscriptions/${uuid}/stream`, {
        headers: {
          authorization: `Bearer ${await getToken()}`,
          accept: 'text/event-stream',
          ...(lastEventId ? { 'last-event-id': lastEventId } : {}),
        },
        signal: controller.signal,
      });
      if (res.status === 401) {
        log('401: token rejected, logging in again');
        token = null;
        tokenExpiresAt = 0;
      } else if (res.status === 404 || res.status === 409) {
        // not something a reconnect fixes
        log(`${res.status}: ${await res.text()}`);
        process.exit(1);
      } else if (!res.ok) {
        log(`${res.status}: ${await res.text()}`);
      } else {
        log(`connected to ${uuid}${lastEventId ? ` (Last-Event-ID ${lastEventId})` : ''}`);
        resetWatchdog();
        const decoder = new TextDecoder();
        let buffer = '';
        for await (const chunk of res.body) {
          resetWatchdog();
          buffer = parseFrames(buffer + decoder.decode(chunk, { stream: true }), (frame) => {
            if (frame.retry) {
              retryMs = Number(frame.retry);
            }
            if (frame.comments.length && options.verbose) {
              log(`: ${frame.comments.join(' ')}`);
            }
            if (frame.data.length === 0) {
              return;
            }
            received++;
            if (frame.id) {
              lastEventId = frame.id;
            }
            // the replay overlap window may re-send an event: deduplicate on its id
            const duplicate = frame.id && seen.has(frame.id);
            if (frame.id) seen.add(frame.id);
            const data = frame.data.join('\n');
            log(
              `#${received} ${frame.event || 'message'} id=${frame.id}${duplicate ? ' (DUPLICATE)' : ''}`,
            );
            if (!options.quiet) {
              try {
                console.log(JSON.stringify(JSON.parse(data), null, 2));
              } catch {
                console.log(data);
              }
            }
          });
        }
        log('stream closed by the server (deploy, token expiry, or the safety cap on connections per subscription)');
      }
    } catch (error) {
      if (!controller.signal.aborted) {
        log(`connection failed: ${error.cause?.code || error.message}`);
      }
    } finally {
      clearTimeout(watchdog);
    }
    // the server's retry: plus a random jitter, so many clients don't reconnect at the same moment
    const delay = retryMs + Math.floor(Math.random() * 1000);
    log(`reconnecting in ${delay}ms`);
    await sleep(delay);
  }
}

function parseArgs(argv) {
  const positional = [];
  const options = { acdUUIDs: [] };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--last-event-id') options.lastEventId = argv[++i];
    else if (arg === '--acd') options.acdUUIDs.push(argv[++i]);
    else if (arg === '--version') options.version = argv[++i];
    else if (arg === '--quiet' || arg === '-q') options.quiet = true;
    else if (arg === '--verbose' || arg === '-v') options.verbose = true;
    else positional.push(arg);
  }
  return { command: positional[0] || 'stream', args: positional.slice(1), options };
}

async function main() {
  const { command, args, options } = parseArgs(process.argv.slice(2));
  switch (command) {
    case 'list': {
      const { body, headers } = await api('GET', '/sse-subscriptions?limit=100');
      log(`${headers.get('x-total-count')} SSE subscription(s)`);
      console.log(JSON.stringify(body, null, 2));
      break;
    }
    case 'create': {
      const { body } = await api('POST', '/sse-subscriptions', {
        messageTypes: args,
        acdUUIDs: options.acdUUIDs,
        ...(options.version ? { version: options.version } : {}),
      });
      console.log(JSON.stringify(body, null, 2));
      break;
    }
    case 'enable':
    case 'disable': {
      const { body } = await api('PUT', `/sse-subscriptions/${args[0]}`, {
        enabled: command === 'enable',
      });
      console.log(JSON.stringify(body, null, 2));
      break;
    }
    case 'delete': {
      const { body } = await api('DELETE', `/sse-subscriptions/${args[0]}`);
      console.log(JSON.stringify(body, null, 2));
      break;
    }
    case 'stream': {
      let uuid = args[0];
      if (!uuid) {
        const { body } = await api('GET', '/sse-subscriptions?limit=1');
        uuid = body[0]?.uuid ?? (await api('POST', '/sse-subscriptions', {})).body.uuid;
        log(`using SSE subscription ${uuid}`);
      }
      await stream(uuid, options);
      break;
    }
    default:
      console.log('usage: node index.js [list | create [MESSAGE_TYPE...] | enable|disable|delete <uuid> | stream [<uuid>]]');
      process.exit(1);
  }
}

process.on('SIGINT', () => {
  log('bye');
  process.exit(0);
});

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
