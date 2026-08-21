import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = Number(process.env.PORT || 8787);
const MAX_PLAYERS = 4;
const TICK_RATE = 12;
const WORLD = { width: 2600, height: 1650, margin: 60 };
const ROOM_NAME = /^[\p{L}\p{N} _-]{3,24}$/u;
const ACTION_COOLDOWNS = { attack: 0.18, dash: 0.75, skill1: 4.0, skill2: 7.0, ultimate: 14.0 };
const ACTION_EFFECTS = {
  attack: { damage: 24, range: 245, duration: 180 },
  dash: { damage: 0, range: 0, duration: 240 },
  skill1: { damage: 68, range: 180, duration: 420 },
  skill2: { damage: 106, range: 235, duration: 560 },
  ultimate: { damage: 165, range: 330, duration: 820 },
};
const CHARACTER_STATS = [
  { hp: 100, speed: 310 }, { hp: 92, speed: 330 }, { hp: 118, speed: 275 }, { hp: 88, speed: 350 },
  { hp: 132, speed: 255 }, { hp: 106, speed: 295 }, { hp: 96, speed: 365 }, { hp: 148, speed: 240 },
];
const ENEMY_TYPES = [
  { type: 'Null Wraith', hp: 70, speed: 88, damage: 8, radius: 28 },
  { type: 'Arc Husk', hp: 110, speed: 62, damage: 13, radius: 34 },
  { type: 'Void Breaker', hp: 170, speed: 45, damage: 19, radius: 42 },
];
const MAP_DEFS = [
  { id: 'story_01', label: 'VOID DISTRICT // 01', stage: 1, requiredStage: 1 },
  { id: 'story_02', label: 'NEON CANYON // 02', stage: 2, requiredStage: 2 },
  { id: 'story_03', label: 'ECLIPSE VAULT // 03', stage: 3, requiredStage: 3 },
  { id: 'story_04', label: 'PRISM RAIL // 04', stage: 4, requiredStage: 4 },
  { id: 'story_05', label: 'WARDEN CORE // 05', stage: 5, requiredStage: 5 },
];
const MAX_ENEMIES = 16;
const MAX_PROJECTILES = 96;
const ENEMY_SPAWN_INTERVAL_MS = 2100;
const WORLD_CHAT_MAX_MESSAGES = 40;
const WORLD_CHAT_COOLDOWN_MS = 1500;

function sanitizeName(value, fallback = 'PLAYER') {
  const raw = String(value || '').trim().replace(/\s+/g, ' ').slice(0, 16);
  return raw || fallback;
}

function sanitizeChat(value) {
  const raw = String(value || '').replace(/[\r\n\t]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 120);
  const blocked = ['cặc', 'cack', 'lồn', 'lon', 'đụ', 'địt', 'đéo', 'fuck', 'shit', 'bitch', 'asshole'];
  return blocked.some((word) => raw.toLowerCase().includes(word)) ? '' : raw;
}

function mapById(mapId) {
  return MAP_DEFS.find((entry) => entry.id === mapId) || null;
}

function playerCanAccessMap(player, map) {
  return Boolean(map) && Number(player.maxStoryStage || 1) >= Number(map.requiredStage || 1);
}

function send(ws, type, payload = {}, requestId = null) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type, requestId, payload }));
  }
}

function clamp(value, low, high) {
  return Math.max(low, Math.min(high, value));
}

function normalizedVector(raw) {
  const x = Number(raw?.x || 0);
  const y = Number(raw?.y || 0);
  const length = Math.hypot(x, y);
  if (!Number.isFinite(x) || !Number.isFinite(y) || length < 0.0001) return { x: 0, y: 0 };
  const scale = length > 1 ? 1 / length : 1;
  return { x: x * scale, y: y * scale };
}

class RoomRegistry {
  constructor() {
    this.rooms = new Map();
  }

  summaries() {
    return [...this.rooms.values()]
      .filter((room) => room.status !== 'CLOSED')
      .map((room) => ({
        id: room.id,
        name: room.name,
        privacy: room.privacy,
        status: room.status,
        playerCount: room.members.size,
        maxPlayers: MAX_PLAYERS,
      }))
      .sort((a, b) => a.status.localeCompare(b.status) || a.name.localeCompare(b.name));
  }

  create(owner, name, privacy) {
    if (!ROOM_NAME.test(name)) throw new DomainError('INVALID_ROOM_NAME', 'ROOM NAME MUST HAVE 3–24 VALID CHARACTERS');
    const room = {
      id: randomUUID(),
      name,
      privacy: privacy === 'PRIVATE' ? 'PRIVATE' : 'PUBLIC',
      status: 'WAITING',
      hostId: owner.id,
      members: new Map([[owner.id, owner]]),
      createdAt: Date.now(),
      mapId: MAP_DEFS[0].id,
      match: null,
    };
    this.rooms.set(room.id, room);
    owner.roomId = room.id;
    return room;
  }

  join(player, roomId) {
    const room = this.rooms.get(roomId);
    if (!room) throw new DomainError('ROOM_NOT_FOUND', 'ROOM NO LONGER EXISTS');
    if (room.status !== 'WAITING') throw new DomainError('ROOM_PLAYING', 'ROOM IS ALREADY PLAYING');
    if (room.members.has(player.id)) return room;
    // This process owns all room mutations. Capacity is checked and reserved in one operation.
    if (room.members.size >= MAX_PLAYERS) throw new DomainError('ROOM_FULL', 'ROOM IS FULL');
    if (!playerCanAccessMap(player, mapById(room.mapId))) throw new DomainError('MAP_LOCKED', 'SELECTED MAP IS NOT UNLOCKED FOR THIS OPERATIVE');
    if (player.roomId) this.leave(player);
    room.members.set(player.id, player);
    player.roomId = room.id;
    return room;
  }

  leave(player) {
    if (!player.roomId) return null;
    const room = this.rooms.get(player.roomId);
    player.roomId = null;
    player.ready = false;
    if (!room) return null;
    room.members.delete(player.id);
    if (room.match) room.match.players.delete(player.id);
    if (room.members.size === 0) {
      room.status = 'CLOSED';
      this.rooms.delete(room.id);
      return room;
    }
    if (room.hostId === player.id) {
      room.hostId = room.members.keys().next().value;
    }
    return room;
  }

  state(room) {
    const selectedMap = mapById(room.mapId) || MAP_DEFS[0];
    return {
      id: room.id,
      name: room.name,
      privacy: room.privacy,
      status: room.status,
      hostId: room.hostId,
      playerCount: room.members.size,
      maxPlayers: MAX_PLAYERS,
      selectedMap,
      maps: MAP_DEFS,
      members: [...room.members.values()].map((member) => ({
        id: member.id,
        nickname: member.nickname,
        characterIndex: member.characterIndex,
        maxStoryStage: member.maxStoryStage,
        ready: member.ready,
        connected: member.connected,
      })),
    };
  }
}

class DomainError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

const registry = new RoomRegistry();
const clients = new Map();
const worldChatMessages = [];

function broadcastRoom(room, type = 'room_state') {
  const payload = registry.state(room);
  for (const member of room.members.values()) send(member.ws, type, payload);
}

function broadcastRooms() {
  const payload = { rooms: registry.summaries() };
  for (const client of clients.values()) send(client.ws, 'room_list', payload);
}

function sendError(ws, error, requestId) {
  send(ws, 'error', { code: error.code || 'INVALID_REQUEST', message: error.message || 'REQUEST REJECTED' }, requestId);
}

function createMatch(room) {
  const players = new Map();
  const spawnPoints = [
    { x: 1120, y: 760 }, { x: 1240, y: 760 }, { x: 1360, y: 760 }, { x: 1480, y: 760 },
  ];
  [...room.members.values()].forEach((member, index) => {
    players.set(member.id, {
      id: member.id,
      nickname: member.nickname,
      characterIndex: member.characterIndex,
      position: spawnPoints[index],
      facing: { x: 1, y: 0 },
      input: { x: 0, y: 0 },
      hp: CHARACTER_STATS[member.characterIndex]?.hp ?? 100,
      maxHp: CHARACTER_STATS[member.characterIndex]?.hp ?? 100,
      speed: CHARACTER_STATS[member.characterIndex]?.speed ?? 310,
      alive: true,
      animation: 'idle',
      animationUntil: 0,
      invulnerableUntil: 0,
      cooldowns: {},
    });
  });
  return {
    id: randomUUID(), seed: Math.floor(Math.random() * 0x7fffffff), startedAt: Date.now(),
    map: mapById(room.mapId) || MAP_DEFS[0], players, enemies: new Map(), enemyCounter: 1, projectiles: new Map(), projectileCounter: 1, nextEnemySpawnAt: Date.now() + 800,
    status: 'PLAYING',
  };
}

function snapshot(room) {
  const match = room.match;
  return {
    roomId: room.id,
    matchId: match.id,
    status: match.status,
    map: match.map,
    players: [...match.players.values()].map((player) => ({
      id: player.id,
      nickname: player.nickname,
      characterIndex: player.characterIndex,
      position: player.position,
      facing: player.facing,
      hp: player.hp,
      maxHp: player.maxHp,
      alive: player.alive,
      animation: player.animation,
    })),
    enemies: [...match.enemies.values()].map((enemy) => ({
      id: enemy.id,
      type: enemy.type,
      position: enemy.position,
      hp: enemy.hp,
      maxHp: enemy.maxHp,
      elite: enemy.elite,
    })),
    projectiles: [...match.projectiles.values()].map((projectile) => ({
      id: projectile.id,
      ownerId: projectile.ownerId,
      characterIndex: projectile.characterIndex,
      position: projectile.position,
      velocity: projectile.velocity,
      radius: projectile.radius,
      life: projectile.life,
      effect: projectile.effect,
    })),
  };
}

function spawnProjectile(match, entity, direction, speed, life, damage, radius, effect = 'damage', piercing = false, lateral = 0) {
  if (match.projectiles.size >= MAX_PROJECTILES) return false;
  const d = normalizedVector(direction);
  const side = { x: -d.y, y: d.x };
  const origin = {
    x: clamp(entity.position.x + d.x * 30 + side.x * lateral, WORLD.margin, WORLD.width - WORLD.margin),
    y: clamp(entity.position.y + d.y * 30 + side.y * lateral, 140, WORLD.height - WORLD.margin),
  };
  const id = match.projectileCounter++;
  match.projectiles.set(id, {
    id,
    ownerId: entity.id,
    characterIndex: entity.characterIndex,
    position: origin,
    velocity: { x: d.x * speed, y: d.y * speed },
    life,
    damage,
    radius,
    effect,
    piercing,
  });
  return true;
}

function spawnAttackProjectiles(match, entity) {
  const d = entity.facing.x || entity.facing.y ? entity.facing : { x: 1, y: 0 };
  switch (entity.characterIndex) {
    case 0:
      spawnProjectile(match, entity, d, 900, 1.1, 22, 7, 'damage', false, 10);
      spawnProjectile(match, entity, d, 900, 1.1, 22, 7, 'damage', false, -10);
      break;
    case 1:
      spawnProjectile(match, entity, d, 1100, 1.0, 30, 6, 'mark', true);
      break;
    case 2:
      spawnProjectile(match, entity, d, 560, 1.7, 74, 12, 'explosion');
      break;
    case 3:
      spawnProjectile(match, entity, d, 780, 1.1, 24, 7, 'chain_stun');
      break;
    case 4:
      spawnProjectile(match, entity, d, 520, 1.5, 62, 15, 'knockback');
      break;
    case 5:
      for (const enemy of match.enemies.values()) {
        if (Math.hypot(enemy.position.x - entity.position.x, enemy.position.y - entity.position.y) <= 455) enemy.hp -= 36;
      }
      break;
    case 6:
      for (let index = 0; index < 4; index += 1) {
        const angle = (index - 1.5) * 0.08;
        const rotated = { x: d.x * Math.cos(angle) - d.y * Math.sin(angle), y: d.x * Math.sin(angle) + d.y * Math.cos(angle) };
        spawnProjectile(match, entity, rotated, 820, 1.0, 17, 5, 'split');
      }
      break;
    case 7:
      spawnProjectile(match, entity, d, 450, 1.8, 58, 15, 'pull');
      break;
    default:
      spawnProjectile(match, entity, d, 850, 1.0, 24, 7);
  }
  return { hits: 0, defeated: 0 };
}

function applyProjectileImpact(match, projectile, enemy) {
  const impacted = projectile.effect === 'explosion'
    ? [...match.enemies.values()].filter((candidate) => Math.hypot(candidate.position.x - enemy.position.x, candidate.position.y - enemy.position.y) <= 82)
    : [enemy];
  let hits = 0;
  let defeated = 0;
  for (const target of impacted) {
    target.hp -= projectile.damage;
    hits += 1;
    if (projectile.effect === 'knockback') {
      target.position.x = clamp(target.position.x + projectile.velocity.x * 0.04, WORLD.margin, WORLD.width - WORLD.margin);
      target.position.y = clamp(target.position.y + projectile.velocity.y * 0.04, 140, WORLD.height - WORLD.margin);
    }
    if (target.hp <= 0) {
      match.enemies.delete(target.id);
      defeated += 1;
    }
  }
  if (projectile.effect === 'pull') {
    for (const target of match.enemies.values()) {
      const distance = Math.hypot(target.position.x - enemy.position.x, target.position.y - enemy.position.y);
      if (distance < 130) {
        target.position.x += (enemy.position.x - target.position.x) * 0.18;
        target.position.y += (enemy.position.y - target.position.y) * 0.18;
      }
    }
  }
  return { hits, defeated };
}

function tickProjectiles(match, dt) {
  for (const projectile of [...match.projectiles.values()]) {
    projectile.life -= dt;
    projectile.position.x += projectile.velocity.x * dt;
    projectile.position.y += projectile.velocity.y * dt;
    if (projectile.life <= 0 || projectile.position.x < -120 || projectile.position.x > WORLD.width + 120 || projectile.position.y < 60 || projectile.position.y > WORLD.height + 120) {
      match.projectiles.delete(projectile.id);
      continue;
    }
    for (const enemy of [...match.enemies.values()]) {
      if (Math.hypot(projectile.position.x - enemy.position.x, projectile.position.y - enemy.position.y) > projectile.radius + enemy.radius) continue;
      applyProjectileImpact(match, projectile, enemy);
      if (!projectile.piercing) {
        match.projectiles.delete(projectile.id);
        break;
      }
    }
  }
}

function spawnEnemy(match) {
  if (match.enemies.size >= MAX_ENEMIES || !match.players.size) return;
  const template = ENEMY_TYPES[Math.floor(Math.random() * ENEMY_TYPES.length)];
  const angle = Math.random() * Math.PI * 2;
  const distance = 510 + Math.random() * 240;
  const anchor = [...match.players.values()][Math.floor(Math.random() * match.players.size)];
  const elite = match.enemyCounter % 9 === 0;
  const mapScale = 1 + (Number(match.map?.stage || 1) - 1) * 0.18;
  match.enemies.set(match.enemyCounter, {
    id: match.enemyCounter,
    type: elite ? `Elite ${template.type}` : template.type,
    position: {
      x: clamp(anchor.position.x + Math.cos(angle) * distance, WORLD.margin, WORLD.width - WORLD.margin),
      y: clamp(anchor.position.y + Math.sin(angle) * distance, 140, WORLD.height - WORLD.margin),
    },
    hp: template.hp * mapScale * (elite ? 2 : 1),
    maxHp: template.hp * mapScale * (elite ? 2 : 1),
    speed: template.speed * (1 + (Number(match.map?.stage || 1) - 1) * 0.04) * (elite ? 1.1 : 1),
    damage: template.damage * mapScale * (elite ? 1.35 : 1),
    radius: template.radius,
    elite,
    nextAttackAt: 0,
  });
  match.enemyCounter += 1;
}

function livingPlayers(match) {
  return [...match.players.values()].filter((entity) => entity.alive);
}

function closestLivingPlayer(match, from) {
  let closest = null;
  let closestDistance = Infinity;
  for (const entity of livingPlayers(match)) {
    const dx = entity.position.x - from.x;
    const dy = entity.position.y - from.y;
    const distance = Math.hypot(dx, dy);
    if (distance < closestDistance) {
      closest = entity;
      closestDistance = distance;
    }
  }
  return { entity: closest, distance: closestDistance };
}

function applyAction(room, entity, action) {
  const match = room.match;
  const effect = ACTION_EFFECTS[action];
  if (!effect) return { hits: 0, defeated: 0 };
  const now = Date.now();
  entity.animation = action;
  entity.animationUntil = now + effect.duration;
  if (action === 'dash') {
    entity.invulnerableUntil = now + 380;
    entity.position = {
      x: clamp(entity.position.x + entity.facing.x * 170, WORLD.margin, WORLD.width - WORLD.margin),
      y: clamp(entity.position.y + entity.facing.y * 170, 140, WORLD.height - WORLD.margin),
    };
  }
  if (action === 'attack') return spawnAttackProjectiles(match, entity);
  let hits = 0;
  let defeated = 0;
  if (effect.damage > 0) {
    for (const enemy of match.enemies.values()) {
      const distance = Math.hypot(enemy.position.x - entity.position.x, enemy.position.y - entity.position.y);
      if (distance > effect.range) continue;
      enemy.hp -= effect.damage;
      hits += 1;
      if (enemy.hp <= 0) {
        match.enemies.delete(enemy.id);
        defeated += 1;
      }
    }
  }
  return { hits, defeated };
}

function tickMatch(room, dt, now) {
  const match = room.match;
  if (!match || match.status !== 'PLAYING') return;
  if (now >= match.nextEnemySpawnAt) {
    spawnEnemy(match);
    match.nextEnemySpawnAt = now + ENEMY_SPAWN_INTERVAL_MS;
  }
  for (const entity of match.players.values()) {
    if (!entity.alive) continue;
    entity.position = {
      x: clamp(entity.position.x + entity.input.x * entity.speed * dt, WORLD.margin, WORLD.width - WORLD.margin),
      y: clamp(entity.position.y + entity.input.y * entity.speed * dt, 140, WORLD.height - WORLD.margin),
    };
    if (now >= entity.animationUntil) entity.animation = entity.input.x || entity.input.y ? 'run' : 'idle';
  }
  tickProjectiles(match, dt);
  for (const enemy of match.enemies.values()) {
    const { entity: target, distance } = closestLivingPlayer(match, enemy.position);
    if (!target) break;
    if (distance > enemy.radius + 27) {
      const dx = (target.position.x - enemy.position.x) / Math.max(distance, 0.001);
      const dy = (target.position.y - enemy.position.y) / Math.max(distance, 0.001);
      enemy.position.x += dx * enemy.speed * dt;
      enemy.position.y += dy * enemy.speed * dt;
    } else if (now >= enemy.nextAttackAt && now >= target.invulnerableUntil) {
      target.hp = Math.max(0, target.hp - enemy.damage);
      target.alive = target.hp > 0;
      enemy.nextAttackAt = now + 850;
    }
  }
  if (livingPlayers(match).length === 0) {
    match.status = 'FINISHED';
    room.status = 'WAITING';
    for (const member of room.members.values()) member.ready = false;
    broadcastRoom(room);
    broadcastRooms();
  }
}

function handleMessage(player, message, requestId) {
  switch (message.type) {
    case 'list_rooms':
      send(player.ws, 'room_list', { rooms: registry.summaries() }, requestId);
      return;
    case 'create_room': {
      if (player.roomId) registry.leave(player);
      const room = registry.create(player, String(message.payload?.name || '').trim(), String(message.payload?.privacy || 'PUBLIC'));
      broadcastRoom(room);
      broadcastRooms();
      return;
    }
    case 'join_room': {
      const room = registry.join(player, String(message.payload?.roomId || ''));
      broadcastRoom(room);
      broadcastRooms();
      return;
    }
    case 'leave_room': {
      const room = registry.leave(player);
      if (room && registry.rooms.has(room.id)) broadcastRoom(room);
      send(player.ws, 'left_room', {});
      broadcastRooms();
      return;
    }
    case 'set_ready': {
      const room = registry.rooms.get(player.roomId);
      if (!room || room.status !== 'WAITING') throw new DomainError('INVALID_ROOM', 'NOT IN A WAITING ROOM');
      player.ready = Boolean(message.payload?.ready);
      broadcastRoom(room);
      return;
    }
    case 'update_nickname': {
      player.nickname = sanitizeName(message.payload?.nickname, player.nickname);
      const room = registry.rooms.get(player.roomId);
      if (room && room.status === 'WAITING') broadcastRoom(room);
      send(player.ws, 'nickname_updated', { nickname: player.nickname });
      return;
    }
    case 'set_map': {
      const room = registry.rooms.get(player.roomId);
      if (!room || room.status !== 'WAITING') throw new DomainError('INVALID_ROOM', 'ROOM CANNOT CHANGE MAP');
      if (room.hostId !== player.id) throw new DomainError('NOT_HOST', 'ONLY HOST CAN SELECT A MAP');
      const map = mapById(String(message.payload?.mapId || ''));
      if (!map) throw new DomainError('INVALID_MAP', 'UNKNOWN MAP');
      const blocked = [...room.members.values()].find((member) => !playerCanAccessMap(member, map));
      if (blocked) throw new DomainError('MAP_NOT_ELIGIBLE', 'ALL SQUAD MEMBERS MUST UNLOCK THIS MAP');
      room.mapId = map.id;
      for (const member of room.members.values()) member.ready = false;
      broadcastRoom(room);
      return;
    }
    case 'start_match': {
      const room = registry.rooms.get(player.roomId);
      if (!room || room.status !== 'WAITING') throw new DomainError('INVALID_ROOM', 'ROOM CANNOT START');
      if (room.hostId !== player.id) throw new DomainError('NOT_HOST', 'ONLY HOST CAN START');
      if (![...room.members.values()].every((member) => member.ready && member.connected)) throw new DomainError('NOT_READY', 'ALL PRESENT PLAYERS MUST BE READY');
      room.status = 'PLAYING';
      room.match = createMatch(room);
      const initial = snapshot(room);
      for (const member of room.members.values()) send(member.ws, 'match_started', initial);
      broadcastRooms();
      return;
    }
    case 'input': {
      const room = registry.rooms.get(player.roomId);
      const entity = room?.match?.players.get(player.id);
      if (!entity) throw new DomainError('NOT_IN_MATCH', 'MATCH NOT ACTIVE');
      entity.input = normalizedVector(message.payload?.move);
      const aim = normalizedVector(message.payload?.aim);
      if (aim.x || aim.y) entity.facing = aim;
      return;
    }
    case 'action_request': {
      const room = registry.rooms.get(player.roomId);
      const entity = room?.match?.players.get(player.id);
      if (!entity || !entity.alive) throw new DomainError('NOT_IN_MATCH', 'MATCH NOT ACTIVE');
      const action = String(message.payload?.action || '');
      const cooldown = ACTION_COOLDOWNS[action];
      if (!cooldown) throw new DomainError('INVALID_ACTION', 'UNKNOWN ACTION');
      const now = Date.now();
      if ((entity.cooldowns[action] || 0) > now) throw new DomainError('RATE_LIMITED', 'ACTION ON COOLDOWN');
      entity.cooldowns[action] = now + cooldown * 1000;
      const aim = normalizedVector(message.payload?.aim);
      if (aim.x || aim.y) entity.facing = aim;
      const outcome = applyAction(room, entity, action);
      const payload = {
        playerId: player.id, action, aim: entity.facing, position: entity.position,
        hits: outcome.hits, defeated: outcome.defeated, serverTime: now,
      };
      for (const member of room.members.values()) send(member.ws, 'action_result', payload);
      return;
    }
    case 'world_chat': {
      const now = Date.now();
      if (now - Number(player.lastWorldChatAt || 0) < WORLD_CHAT_COOLDOWN_MS) throw new DomainError('CHAT_RATE_LIMITED', 'WAIT BEFORE SENDING ANOTHER MESSAGE');
      const text = sanitizeChat(message.payload?.text);
      if (!text) throw new DomainError('INVALID_CHAT', 'MESSAGE NOT ACCEPTED');
      player.lastWorldChatAt = now;
      const entry = { id: randomUUID(), nickname: player.nickname, characterIndex: player.characterIndex, text, createdAt: now };
      worldChatMessages.push(entry);
      while (worldChatMessages.length > WORLD_CHAT_MAX_MESSAGES) worldChatMessages.shift();
      for (const client of clients.values()) send(client.ws, 'world_chat', entry);
      return;
    }
    default:
      throw new DomainError('INVALID_REQUEST', 'UNKNOWN MESSAGE TYPE');
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: registry.rooms.size, clients: clients.size }));
    return;
  }
  res.writeHead(404, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ error: 'NOT_FOUND' }));
});

const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
wss.on('connection', (ws) => {
  let player = null;
  ws.on('message', (buffer) => {
    let message;
    try {
      message = JSON.parse(buffer.toString('utf8'));
      if (!message || typeof message.type !== 'string') throw new Error('Malformed envelope');
      if (message.type === 'hello') {
        if (player) throw new DomainError('INVALID_REQUEST', 'ALREADY AUTHENTICATED');
        player = { id: randomUUID(), ws, roomId: null, ready: false, connected: true, nickname: sanitizeName(message.payload?.nickname), characterIndex: clamp(Number(message.payload?.characterIndex || 0), 0, 7), maxStoryStage: clamp(Number(message.payload?.maxStoryStage || 1), 1, 10), lastWorldChatAt: 0 };
        clients.set(player.id, player);
        send(ws, 'welcome', { playerId: player.id, maxPlayers: MAX_PLAYERS }, message.requestId || null);
        send(ws, 'room_list', { rooms: registry.summaries() });
        send(ws, 'world_chat_history', { messages: worldChatMessages });
        return;
      }
      if (!player) throw new DomainError('UNAUTHENTICATED', 'SEND HELLO FIRST');
      handleMessage(player, message, message.requestId || null);
    } catch (error) {
      sendError(ws, error, message?.requestId || null);
    }
  });
  ws.on('close', () => {
    if (!player) return;
    player.connected = false;
    const room = registry.leave(player);
    clients.delete(player.id);
    if (room && registry.rooms.has(room.id)) {
      for (const member of room.members.values()) send(member.ws, 'member_disconnected', { playerId: player.id });
      broadcastRoom(room);
    }
    broadcastRooms();
  });
});

setInterval(() => {
  const dt = 1 / TICK_RATE;
  const now = Date.now();
  for (const room of registry.rooms.values()) {
    if (room.status !== 'PLAYING' || !room.match) continue;
    tickMatch(room, dt, now);
    const state = snapshot(room);
    for (const member of room.members.values()) send(member.ws, 'world_snapshot', state);
  }
}, 1000 / TICK_RATE);

server.listen(PORT, '0.0.0.0', () => console.log(`Neon Soul realtime listening on :${PORT}`));
