import assert from 'node:assert/strict';
import test from 'node:test';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import WebSocket from 'ws';

const PORT = 8791;
const SOCKET_URL = `ws://127.0.0.1:${PORT}`;

function makeClient(name, characterIndex, socketUrl = SOCKET_URL) {
  const ws = new WebSocket(socketUrl);
  const inbox = [];
  let notify = null;
  ws.on('message', (raw) => {
    inbox.push(JSON.parse(raw.toString()));
    if (notify) notify();
  });
  const waitFor = async (type, predicate = () => true, timeoutMs = 2500) => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const index = inbox.findIndex((message) => message.type === type && predicate(message));
      if (index >= 0) return inbox.splice(index, 1)[0];
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${type}`)), Math.max(1, deadline - Date.now()));
        notify = () => { clearTimeout(timer); notify = null; resolve(); };
      });
    }
    throw new Error(`Timed out waiting for ${type}`);
  };
  const request = (type, payload = {}, requestId = crypto.randomUUID()) => ws.send(JSON.stringify({ type, payload, requestId }));
  return { ws, name, characterIndex, waitFor, request };
}

test('room capacity, ready flow and authoritative match start', async (t) => {
  const serverProcess = spawn(globalThis.process.execPath, ['server.mjs'], {
    cwd: new URL('..', import.meta.url),
    env: { ...globalThis.process.env, PORT: String(PORT) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  t.after(() => serverProcess.kill('SIGTERM'));
  await once(serverProcess.stdout, 'data');

  const clients = Array.from({ length: 5 }, (_, index) => makeClient(`P${index + 1}`, index));
  t.after(() => clients.forEach((client) => client.ws.close()));
  await Promise.all(clients.map(async (client) => {
    await once(client.ws, 'open');
    client.request('hello', { nickname: client.name, characterIndex: client.characterIndex });
    await client.waitFor('welcome');
  }));

  clients[0].request('create_room', { name: 'Dungeon Run', privacy: 'PUBLIC' });
  const room = await clients[0].waitFor('room_state', (message) => message.payload.name === 'Dungeon Run');
  const roomId = room.payload.id;
  for (const client of clients.slice(1, 4)) {
    client.request('join_room', { roomId });
    await client.waitFor('room_state', (message) => message.payload.playerCount >= 2);
  }
  clients[4].request('join_room', { roomId });
  const full = await clients[4].waitFor('error');
  assert.equal(full.payload.code, 'ROOM_FULL');

  for (const client of clients.slice(0, 4)) client.request('set_ready', { ready: true });
  await clients[0].waitFor('room_state', (message) => message.payload.members.every((member) => member.ready));
  clients[0].request('start_match');
  const starts = await Promise.all(clients.slice(0, 4).map((client) => client.waitFor('match_started')));
  assert.equal(starts[0].payload.players.length, 4);
  clients[0].request('input', { move: { x: 1, y: 0 }, aim: { x: 1, y: 0 } });
  const snapshot = await clients[1].waitFor('world_snapshot', (message) => message.payload.players.length === 4);
  assert.equal(snapshot.payload.players.length, 4);
});

test('lobby host migrates deterministically after host disconnect', async (t) => {
  const migrationPort = 8792;
  const serverProcess = spawn(globalThis.process.execPath, ['server.mjs'], {
    cwd: new URL('..', import.meta.url),
    env: { ...globalThis.process.env, PORT: String(migrationPort) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  t.after(() => serverProcess.kill('SIGTERM'));
  await once(serverProcess.stdout, 'data');

  const connect = (name, characterIndex) => {
    const ws = new WebSocket(`ws://127.0.0.1:${migrationPort}`);
    const inbox = [];
    let notify = null;
    ws.on('message', (raw) => {
      inbox.push(JSON.parse(raw.toString()));
      if (notify) notify();
    });
    const waitFor = async (type, predicate = () => true, timeoutMs = 2500) => {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        const index = inbox.findIndex((message) => message.type === type && predicate(message));
        if (index >= 0) return inbox.splice(index, 1)[0];
        await new Promise((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${type}`)), Math.max(1, deadline - Date.now()));
          notify = () => { clearTimeout(timer); notify = null; resolve(); };
        });
      }
      throw new Error(`Timed out waiting for ${type}`);
    };
    const request = (type, payload = {}, requestId = crypto.randomUUID()) => ws.send(JSON.stringify({ type, payload, requestId }));
    return { ws, waitFor, request, name, characterIndex };
  };

  const host = connect('Host', 0);
  const peer = connect('Peer', 1);
  t.after(() => { host.ws.close(); peer.ws.close(); });
  await Promise.all([host, peer].map(async (client) => {
    await once(client.ws, 'open');
    client.request('hello', { nickname: client.name, characterIndex: client.characterIndex });
    await client.waitFor('welcome');
  }));

  host.request('create_room', { name: 'Host Switch', privacy: 'PUBLIC' });
  const created = await host.waitFor('room_state', (message) => message.payload.name === 'Host Switch');
  const originalHostId = created.payload.hostId;
  peer.request('join_room', { roomId: created.payload.id });
  await peer.waitFor('room_state', (message) => message.payload.members.length === 2);
  host.ws.close();

  await peer.waitFor('member_disconnected', (message) => message.payload.playerId === originalHostId);
  const migrated = await peer.waitFor('room_state', (message) => message.payload.members.length === 1 && message.payload.hostId !== originalHostId);
  assert.equal(migrated.payload.hostId, migrated.payload.members[0].id);
});

test('host map selection enforces squad eligibility and world chat is validated', async (t) => {
  const port = 8793;
  const socketUrl = `ws://127.0.0.1:${port}`;
  const serverProcess = spawn(globalThis.process.execPath, ['server.mjs'], {
    cwd: new URL('..', import.meta.url),
    env: { ...globalThis.process.env, PORT: String(port) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  t.after(() => serverProcess.kill('SIGTERM'));
  await once(serverProcess.stdout, 'data');

  const host = makeClient('Map Host', 0, socketUrl);
  const guest = makeClient('Map Guest', 1, socketUrl);
  t.after(() => { host.ws.close(); guest.ws.close(); });
  await Promise.all([host, guest].map(async (client) => {
    await once(client.ws, 'open');
    client.request('hello', { nickname: client.name, characterIndex: client.characterIndex, maxStoryStage: 1 });
    await client.waitFor('welcome');
  }));

  host.request('create_room', { name: 'Map Guard', privacy: 'PUBLIC' });
  const created = await host.waitFor('room_state', (message) => message.payload.name === 'Map Guard');
  guest.request('join_room', { roomId: created.payload.id });
  await host.waitFor('room_state', (message) => message.payload.members.length === 2);

  guest.request('set_map', { mapId: 'story_02' });
  const notHost = await guest.waitFor('error');
  assert.equal(notHost.payload.code, 'NOT_HOST');

  host.request('set_map', { mapId: 'story_02' });
  const eligibility = await host.waitFor('error');
  assert.equal(eligibility.payload.code, 'MAP_NOT_ELIGIBLE');

  host.request('world_chat', { text: 'Signal online.' });
  const received = await guest.waitFor('world_chat', (message) => message.payload.text === 'Signal online.');
  assert.equal(received.payload.nickname, 'Map Host');

  host.request('world_chat', { text: 'Too fast.' });
  const rateLimited = await host.waitFor('error');
  assert.equal(rateLimited.payload.code, 'CHAT_RATE_LIMITED');

  guest.request('world_chat', { text: 'fuck' });
  const blocked = await guest.waitFor('error');
  assert.equal(blocked.payload.code, 'INVALID_CHAT');
});
