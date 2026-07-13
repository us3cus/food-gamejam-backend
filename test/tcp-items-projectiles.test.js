const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const net = require('node:net');
const { after, before, test } = require('node:test');

const FOOD_DAMAGE = { tomato: 1, cheese: 1, pumpkin: 2, watermelon: 1 };

class TcpClient {
  constructor(socket) {
    this.socket = socket;
    this.buffer = '';
    this.messages = [];
    this.waiters = [];
    socket.setEncoding('utf8');
    socket.on('data', (chunk) => this.onData(chunk));
  }

  onData(chunk) {
    this.buffer += chunk;
    let end;
    while ((end = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, end);
      this.buffer = this.buffer.slice(end + 1);
      if (line) this.push(JSON.parse(line));
    }
  }

  push(packet) {
    const waiterIndex = this.waiters.findIndex((waiter) => waiter.predicate(packet));
    if (waiterIndex === -1) return void this.messages.push(packet);
    const [waiter] = this.waiters.splice(waiterIndex, 1);
    clearTimeout(waiter.timer);
    waiter.resolve(packet);
  }

  send(type, payload, requestId = null) {
    this.socket.write(`${JSON.stringify({ type, request_id: requestId, payload })}\n`);
  }

  waitFor(type, payloadPredicate = () => true, timeoutMs = 4_000) {
    const predicate = (packet) => packet.type === type && payloadPredicate(packet.payload);
    const queuedIndex = this.messages.findIndex(predicate);
    if (queuedIndex !== -1) return Promise.resolve(this.messages.splice(queuedIndex, 1)[0]);
    return new Promise((resolve, reject) => {
      const waiter = { predicate, resolve, timer: null };
      waiter.timer = setTimeout(() => {
        this.waiters = this.waiters.filter((candidate) => candidate !== waiter);
        reject(new Error(`Timed out waiting for ${type}`));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }

  close() { this.socket.destroy(); }
}

function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      server.close(() => resolve(address.port));
    });
  });
}

async function connect(port) {
  const deadline = Date.now() + 8_000;
  while (Date.now() < deadline) {
    try {
      const socket = await new Promise((resolve, reject) => {
        const candidate = net.createConnection({ host: '127.0.0.1', port });
        candidate.once('connect', () => resolve(candidate));
        candidate.once('error', reject);
      });
      return new TcpClient(socket);
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  throw new Error(`Server did not start on port ${port}`);
}

let serverProcess;
let tcpPort;
let serverOutput = '';

before(async () => {
  tcpPort = await getFreePort();
  const httpPort = await getFreePort();
  serverProcess = spawn(process.execPath, ['dist/main.js'], {
    cwd: process.cwd(),
    env: { ...process.env, TCP_HOST: '127.0.0.1', TCP_PORT: String(tcpPort), HTTP_PORT: String(httpPort) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  serverProcess.stdout.on('data', (chunk) => { serverOutput += chunk; });
  serverProcess.stderr.on('data', (chunk) => { serverOutput += chunk; });
});

after(() => {
  if (serverProcess && !serverProcess.killed) serverProcess.kill();
});

test('two clients synchronize items and projectiles', async (context) => {
  const host = await connect(tcpPort).catch((error) => { throw new Error(`${error.message}\n${serverOutput}`); });
  const guest = await connect(tcpPort);
  context.after(() => { host.close(); guest.close(); });

  host.send('session.hello', { protocol: 1, player_name: 'Host' }, 1);
  guest.send('session.hello', { protocol: 1, player_name: 'Guest' }, 2);
  const hostId = (await host.waitFor('session.welcome')).payload.player_id;
  const guestId = (await guest.waitFor('session.welcome')).payload.player_id;

  host.send('lobby.create', { settings: { max_players: 2, round_time: 90, wins_to_match: 2 } }, 3);
  const lobbyId = (await host.waitFor('lobby.created')).payload.lobby_id;
  guest.send('lobby.join', { lobby_id: lobbyId }, 4);
  await guest.waitFor('lobby.joined');
  guest.send('lobby.ready', { lobby_id: lobbyId, ready: true }, 5);
  await host.waitFor('lobby.updated', (payload) => payload.players.some((player) => player.id === guestId && player.ready));

  host.send('lobby.start', { lobby_id: lobbyId }, 6);
  const [hostStarted, guestStarted] = await Promise.all([
    host.waitFor('lobby.started'),
    guest.waitFor('lobby.started'),
  ]);
  assert.equal(hostStarted.payload.match_id, guestStarted.payload.match_id);
  assert.equal(hostStarted.payload.items.length, 1);
  assert.equal(hostStarted.payload.items[0].item_id, guestStarted.payload.items[0].item_id);

  const matchId = hostStarted.payload.match_id;
  const firstItem = hostStarted.payload.items[0];
  const hostPosition = { x: firstItem.position.x, y: 0.1, z: firstItem.position.z };
  host.send('game.input', { match_id: matchId, position: hostPosition, velocity: { x: 0, y: 0, z: 0 }, yaw: 0 });
  host.send('game.item.pickup', { match_id: matchId, item_id: firstItem.item_id }, 7);
  const [hostPicked, guestPicked] = await Promise.all([
    host.waitFor('game.item.picked', (payload) => payload.item_id === firstItem.item_id),
    guest.waitFor('game.item.picked', (payload) => payload.item_id === firstItem.item_id),
  ]);
  assert.equal(hostPicked.payload.item_id, guestPicked.payload.item_id);

  const fakeFoodType = hostPicked.payload.food_type === 'tomato' ? 'cheese' : 'tomato';
  const dropPosition = { ...hostPosition, y: 0.5 };
  host.send('game.item.drop', { match_id: matchId, food_type: fakeFoodType, position: dropPosition }, 8);
  const [hostDrop, guestDrop] = await Promise.all([
    host.waitFor('game.item.spawn', (payload) => payload.position.y === 0.5),
    guest.waitFor('game.item.spawn', (payload) => payload.position.y === 0.5),
  ]);
  assert.equal(hostDrop.payload.item_id, guestDrop.payload.item_id);
  assert.equal(hostDrop.payload.food_type, hostPicked.payload.food_type);

  await new Promise((resolve) => setTimeout(resolve, 800));
  host.send('game.item.pickup', { match_id: matchId, item_id: hostDrop.payload.item_id }, 9);
  await Promise.all([
    host.waitFor('game.item.picked', (payload) => payload.item_id === hostDrop.payload.item_id),
    guest.waitFor('game.item.picked', (payload) => payload.item_id === hostDrop.payload.item_id),
  ]);

  host.send('game.projectile.spawn', {
    match_id: matchId,
    food_type: hostDrop.payload.food_type,
    origin: dropPosition,
    velocity: { x: 10, y: 3, z: 0 },
    knockback_multiplier: 1.25,
  }, 10);
  const [hostProjectile, guestProjectile] = await Promise.all([
    host.waitFor('game.projectile.spawn'),
    guest.waitFor('game.projectile.spawn'),
  ]);
  assert.equal(hostProjectile.payload.projectile_id, guestProjectile.payload.projectile_id);

  host.send('game.projectile.hit', {
    match_id: matchId,
    projectile_id: hostProjectile.payload.projectile_id,
    target_id: guestId,
    damage: 99,
    knockback: { x: 5, y: 1, z: 0 },
  }, 11);
  const hit = await guest.waitFor('game.hit', (payload) => payload.projectile_id === hostProjectile.payload.projectile_id);
  assert.equal(hit.payload.source_id, hostId);
  assert.equal(hit.payload.damage, FOOD_DAMAGE[hostProjectile.payload.food_type]);
  assert.notEqual(hit.payload.damage, 99);

  host.send('game.projectile.despawn', { match_id: matchId, projectile_id: hostProjectile.payload.projectile_id }, 12);
  const [hostDespawn, guestDespawn] = await Promise.all([
    host.waitFor('game.projectile.despawn', (payload) => payload.projectile_id === hostProjectile.payload.projectile_id),
    guest.waitFor('game.projectile.despawn', (payload) => payload.projectile_id === hostProjectile.payload.projectile_id),
  ]);
  assert.equal(hostDespawn.payload.projectile_id, guestDespawn.payload.projectile_id);

  host.send('game.round.reset', { match_id: matchId, reset_match: false }, 13);
  const [hostRound, guestRound] = await Promise.all([
    host.waitFor('game.round.started'),
    guest.waitFor('game.round.started'),
  ]);
  assert.equal(hostRound.payload.items.length, 1);
  assert.equal(hostRound.payload.items[0].item_id, guestRound.payload.items[0].item_id);
  assert.notEqual(hostRound.payload.items[0].item_id, hostDrop.payload.item_id);
  assert.ok(hostRound.payload.players.every((player) => player.health === 3 && player.held_food === ''));
});
