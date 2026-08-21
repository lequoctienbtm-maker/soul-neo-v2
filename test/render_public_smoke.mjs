import WebSocket from 'ws';

const RELAY_URL = process.env.RELAY_URL || 'wss://neon-soul-realtime.onrender.com';
const TIMEOUT_MS = 20_000;
const STAGGER_MS = Number(process.env.STAGGER_MS || 0);

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class RelayClient {
  constructor(label) {
    this.label = label;
    this.messages = [];
    this.waiters = [];
    this.socket = null;
  }

  async connect() {
    this.socket = new WebSocket(RELAY_URL);
    this.socket.on('message', (raw) => {
      const message = JSON.parse(raw.toString('utf8'));
      this.messages.push(message);
      for (const waiter of [...this.waiters]) {
        if (waiter.predicate(message)) {
          clearTimeout(waiter.timeout);
          this.waiters.splice(this.waiters.indexOf(waiter), 1);
          waiter.resolve(message);
        }
      }
    });
    await new Promise((resolve, reject) => {
      this.socket.once('open', resolve);
      this.socket.once('error', reject);
    });
  }

  send(type, payload = {}, requestId = null) {
    this.socket.send(JSON.stringify({ type, payload, requestId }));
  }

  waitFor(type, predicate = () => true) {
    const existing = this.messages.find((message) => message.type === type && predicate(message));
    if (existing) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const waiter = {
        predicate: (message) => message.type === type && predicate(message),
        resolve,
        timeout: setTimeout(() => {
          this.waiters.splice(this.waiters.indexOf(waiter), 1);
          reject(new Error(`${this.label} timed out waiting for ${type}`));
        }, TIMEOUT_MS),
      };
      this.waiters.push(waiter);
    });
  }

  close() {
    this.socket?.close();
  }
}

async function main() {
  const host = new RelayClient('host');
  const guest = new RelayClient('guest');
  const unique = Date.now().toString().slice(-6);
  try {
    await host.connect();
    host.send('hello', { nickname: `HOST${unique}`, characterIndex: 0, maxStoryStage: 2 }, 'host-hello');
    const hostWelcome = await host.waitFor('welcome');
    assert(hostWelcome.payload.playerId, 'Host received no player ID');

    host.send('create_room', { name: `SMOKE ${unique}`, privacy: 'PUBLIC' }, 'create');
    const created = await host.waitFor('room_state', (message) => message.payload.name === `SMOKE ${unique}`);
    const roomId = created.payload.id;
    assert(roomId, 'Created room has no ID');

    if (STAGGER_MS > 0) await delay(STAGGER_MS);
    await guest.connect();
    guest.send('hello', { nickname: `GUEST${unique}`, characterIndex: 1, maxStoryStage: 2 }, 'guest-hello');
    const guestWelcome = await guest.waitFor('welcome');
    assert(guestWelcome.payload.playerId, 'Guest received no player ID');
    guest.send('join_room', { roomId }, 'join');
    await Promise.all([
      host.waitFor('room_state', (message) => message.payload.id === roomId && message.payload.playerCount === 2),
      guest.waitFor('room_state', (message) => message.payload.id === roomId && message.payload.playerCount === 2),
    ]);

    const chatText = `RELAY CHECK ${unique}`;
    host.send('world_chat', { text: chatText }, 'world-chat');
    const chat = await guest.waitFor('world_chat', (message) => message.payload.text === chatText);
    assert(chat.payload.nickname === `HOST${unique}`, 'World chat sender identity is incorrect');

    host.send('set_map', { mapId: 'story_02' }, 'set-map');
    await Promise.all([
      host.waitFor('room_state', (message) => message.payload.id === roomId && message.payload.selectedMap?.id === 'story_02'),
      guest.waitFor('room_state', (message) => message.payload.id === roomId && message.payload.selectedMap?.id === 'story_02'),
    ]);

    host.send('set_ready', { ready: true }, 'host-ready');
    await host.waitFor('room_state', (message) => message.payload.id === roomId && message.payload.members.find((member) => member.id === hostWelcome.payload.playerId)?.ready);
    guest.send('set_ready', { ready: true }, 'guest-ready');
    await host.waitFor('room_state', (message) => message.payload.id === roomId && message.payload.members.every((member) => member.ready));

    host.send('start_match', {}, 'start');
    const [hostMatch, guestMatch] = await Promise.all([
      host.waitFor('match_started'), guest.waitFor('match_started'),
    ]);
    assert(hostMatch.payload.players.length === 2, 'Match snapshot does not contain both players');
    assert(guestMatch.payload.players.length === 2, 'Guest match snapshot does not contain both players');

    host.send('input', { move: { x: 1, y: 0 }, aim: { x: 1, y: 0 } }, 'input');
    await delay(240);
    host.send('action_request', { action: 'attack', aim: { x: 1, y: 0 } }, 'attack');
    const [action, snapshot] = await Promise.all([
      guest.waitFor('action_result', (message) => message.payload.playerId === hostWelcome.payload.playerId && message.payload.action === 'attack'),
      guest.waitFor('world_snapshot', (message) => message.payload.roomId === roomId && message.payload.players.length === 2 && Array.isArray(message.payload.projectiles) && message.payload.projectiles.length > 0),
    ]);
    assert(Number.isFinite(action.payload.serverTime), 'Action did not include authoritative server time');
    assert(snapshot.payload.status === 'PLAYING', 'World snapshot is not playing');

    console.log(JSON.stringify({ ok: true, relay: RELAY_URL, roomId, players: snapshot.payload.players.length, map: 'story_02', worldChat: true }));
  } finally {
    host.close();
    guest.close();
  }
}

main().catch((error) => {
  console.error(`PUBLIC RELAY SMOKE FAILED: ${error.message}`);
  process.exitCode = 1;
});
