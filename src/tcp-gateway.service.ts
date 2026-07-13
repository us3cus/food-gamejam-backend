import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { randomInt, randomUUID } from 'node:crypto';
import { createServer, Server, Socket } from 'node:net';

const MAX_LINE_BYTES = 1_048_576;
const MAX_PACKETS_PER_SECOND = 30;
const MAX_NAME_LENGTH = 32;
const LOBBY_CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const ITEM_SPAWN_INTERVAL_MS = 2_500;
const MAX_ITEMS = 6;
const PROJECTILE_TTL_MS = 6_000;

type FoodType = 'tomato' | 'cheese' | 'pumpkin' | 'watermelon';
const FOOD: Record<FoodType, { weight: number; damage: number; aoe: boolean }> = {
  tomato: { weight: 3, damage: 1, aoe: false },
  cheese: { weight: 3, damage: 1, aoe: false },
  pumpkin: { weight: 2, damage: 2, aoe: false },
  watermelon: { weight: 1, damage: 1, aoe: true },
};

type Packet = { type?: unknown; request_id?: unknown; payload?: unknown };
type Settings = { max_players: number; round_time: number; wins_to_match: number };
type Player = {
  id: string;
  name: string;
  socket: Socket;
  lobbyId: string | null;
  ready: boolean;
  packetTimes: number[];
};
type Lobby = {
  id: string;
  hostId: string;
  settings: Settings;
  players: Map<string, Player>;
  started: boolean;
  matchId: string | null;
};
type VectorState = { x: number; y: number; z: number };
type MatchPlayer = {
  id: string;
  name: string;
  spawnSlot: number;
  position: VectorState;
  velocity: VectorState;
  yaw: number;
  health: number;
  heldFood: FoodType | null;
  lastHitAt: number;
};
type FoodItemState = {
  id: string;
  foodType: FoodType;
  position: VectorState;
  pickupAvailableAt: number;
  bonked: boolean;
};
type ProjectileState = {
  id: string;
  sourceId: string;
  foodType: FoodType;
  origin: VectorState;
  velocity: VectorState;
  knockbackMultiplier: number;
  damage: number;
  aoe: boolean;
  createdAt: number;
  hitTargets: Set<string>;
};
type Match = {
  id: string;
  lobbyId: string;
  hostId: string;
  players: Map<string, MatchPlayer>;
  items: Map<string, FoodItemState>;
  projectiles: Map<string, ProjectileState>;
  nextItemSpawnAt: number;
};

@Injectable()
export class TcpGatewayService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TcpGatewayService.name);
  private readonly lobbies = new Map<string, Lobby>();
  private readonly matches = new Map<string, Match>();
  private server?: Server;
  private gameTimer?: NodeJS.Timeout;
  readonly port = Number(process.env.TCP_PORT ?? 7778);
  readonly host = process.env.TCP_HOST ?? '127.0.0.1';

  get lobbyCount() { return this.lobbies.size; }

  onModuleInit() {
    this.server = createServer((socket) => this.handleConnection(socket));
    this.server.listen(this.port, this.host, () => this.logger.log(`TCP lobby server listening on ${this.host}:${this.port}`));
    this.gameTimer = setInterval(() => this.broadcastGameStates(), 50);
    this.gameTimer.unref();
  }

  onModuleDestroy() {
    if (this.gameTimer) clearInterval(this.gameTimer);
    this.server?.close();
  }

  private handleConnection(socket: Socket) {
    socket.setNoDelay(true);
    let buffer = Buffer.alloc(0);
    const player: Player = { id: randomUUID(), name: 'Player', socket, lobbyId: null, ready: false, packetTimes: [] };

    socket.on('data', (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (buffer.length > MAX_LINE_BYTES) return socket.destroy();
      let lineEnd: number;
      while ((lineEnd = buffer.indexOf(0x0a)) !== -1) {
        const line = buffer.subarray(0, lineEnd);
        buffer = buffer.subarray(lineEnd + 1);
        if (line.length > MAX_LINE_BYTES) return socket.destroy();
        this.handleLine(player, line);
      }
    });
    socket.on('error', () => undefined);
    socket.on('close', () => this.leaveLobby(player));
  }

  private handleLine(player: Player, line: Buffer) {
    const now = Date.now();
    player.packetTimes = player.packetTimes.filter((at) => now - at < 1_000);
    if (player.packetTimes.length >= MAX_PACKETS_PER_SECOND) return this.fail(player, 'RATE_LIMITED', 'Too many packets');
    player.packetTimes.push(now);

    let packet: Packet;
    try { packet = JSON.parse(line.toString('utf8')) as Packet; } catch { return this.fail(player, 'INVALID_JSON', 'Invalid JSON'); }
    if (!packet || typeof packet.type !== 'string') return this.fail(player, 'INVALID_PACKET', 'Packet type is required');

    const payload = this.object(packet.payload) ?? {};
    const requestId = packet.request_id ?? null;
    switch (packet.type) {
      case 'session.hello': return this.hello(player, payload, requestId);
      case 'lobby.create': return this.createLobby(player, payload, requestId);
      case 'lobby.join': return this.joinLobby(player, payload, requestId);
      case 'lobby.settings': return this.updateSettings(player, payload, requestId);
      case 'lobby.ready': return this.setReady(player, payload, requestId);
      case 'lobby.invite': return this.invite(player, payload, requestId);
      case 'lobby.start': return this.start(player, payload, requestId);
      case 'lobby.leave': return this.leave(player, payload, requestId);
      case 'game.input': return this.gameInput(player, payload, requestId);
      case 'game.hit': return this.gameHit(player, payload, requestId);
      case 'game.round.reset': return this.resetRound(player, payload, requestId);
      case 'game.item.pickup': return this.pickupItem(player, payload, requestId);
      case 'game.item.drop': return this.dropItem(player, payload, requestId);
      case 'game.item.hit': return this.itemHit(player, payload, requestId);
      case 'game.item.despawn': return this.despawnItem(player, payload, requestId);
      case 'game.projectile.spawn': return this.spawnProjectile(player, payload, requestId);
      case 'game.projectile.hit': return this.projectileHit(player, payload, requestId);
      case 'game.projectile.despawn': return this.despawnProjectile(player, payload, requestId);
      default: return this.fail(player, 'UNKNOWN_TYPE', 'Unknown packet type', requestId);
    }
  }

  private hello(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    if (payload.protocol !== 1) return this.fail(player, 'UNSUPPORTED_PROTOCOL', 'Protocol 1 is required', requestId);
    if (typeof payload.player_name === 'string') player.name = payload.player_name.trim().slice(0, MAX_NAME_LENGTH) || 'Player';
    this.send(player, 'session.welcome', { player_id: player.id }, requestId);
  }

  private createLobby(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    this.leaveLobby(player);
    const settings = this.validateSettings(this.object(payload.settings) ?? {}, this.defaultSettings());
    if (!settings) return this.fail(player, 'INVALID_SETTINGS', 'Invalid lobby settings', requestId);

    const lobby: Lobby = { id: this.newLobbyId(), hostId: player.id, settings, players: new Map(), started: false, matchId: null };
    lobby.players.set(player.id, player);
    player.lobbyId = lobby.id;
    player.ready = false;
    this.lobbies.set(lobby.id, lobby);
    this.send(player, 'lobby.created', this.lobbyPayload(lobby), requestId);
    this.broadcastUpdate(lobby);
  }

  private joinLobby(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const lobbyId = typeof payload.lobby_id === 'string' ? payload.lobby_id.toUpperCase() : '';
    const lobby = this.lobbies.get(lobbyId);
    if (!lobby) return this.fail(player, 'LOBBY_NOT_FOUND', 'Lobby does not exist', requestId);
    if (lobby.started) return this.fail(player, 'LOBBY_STARTED', 'Game already started', requestId);
    if (lobby.players.size >= lobby.settings.max_players) return this.fail(player, 'LOBBY_FULL', 'Lobby is full', requestId);

    this.leaveLobby(player);
    lobby.players.set(player.id, player);
    player.lobbyId = lobby.id;
    player.ready = false;
    this.send(player, 'lobby.joined', this.lobbyPayload(lobby), requestId);
    this.broadcastUpdate(lobby);
  }

  private updateSettings(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const lobby = this.lobbyFor(player, payload.lobby_id);
    if (!lobby) return this.fail(player, 'NOT_IN_LOBBY', 'Player is not in this lobby', requestId);
    if (lobby.hostId !== player.id) return this.fail(player, 'NOT_HOST', 'Only the host can change settings', requestId);
    if (lobby.started) return this.fail(player, 'LOBBY_STARTED', 'Game already started', requestId);
    const settings = this.validateSettings(this.object(payload.settings) ?? {}, lobby.settings);
    if (!settings || settings.max_players < lobby.players.size) return this.fail(player, 'INVALID_SETTINGS', 'Invalid lobby settings', requestId);
    lobby.settings = settings;
    this.send(player, 'lobby.settings', this.lobbyPayload(lobby), requestId);
    this.broadcastUpdate(lobby);
  }

  private setReady(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const lobby = this.lobbyFor(player, payload.lobby_id);
    if (!lobby) return this.fail(player, 'NOT_IN_LOBBY', 'Player is not in this lobby', requestId);
    if (lobby.started) return this.fail(player, 'LOBBY_STARTED', 'Game already started', requestId);
    if (typeof payload.ready !== 'boolean') return this.fail(player, 'INVALID_READY', 'ready must be boolean', requestId);
    if (lobby.hostId === player.id) return this.fail(player, 'HOST_CANNOT_READY', 'Host readiness is not required', requestId);
    player.ready = payload.ready;
    this.send(player, 'lobby.ready', this.lobbyPayload(lobby), requestId);
    this.broadcastUpdate(lobby);
  }

  private invite(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const lobby = this.lobbyFor(player, payload.lobby_id);
    if (!lobby) return this.fail(player, 'NOT_IN_LOBBY', 'Player is not in this lobby', requestId);
    this.send(player, 'lobby.invite', { lobby_id: lobby.id, invite_code: lobby.id, host: 'onk.temten.me', port: 7777 }, requestId);
  }

  private start(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const lobby = this.lobbyFor(player, payload.lobby_id);
    if (!lobby) return this.fail(player, 'NOT_IN_LOBBY', 'Player is not in this lobby', requestId);
    if (lobby.hostId !== player.id) return this.fail(player, 'NOT_HOST', 'Only the host can start', requestId);
    if (lobby.started) return this.fail(player, 'LOBBY_STARTED', 'Game already started', requestId);
    if (lobby.players.size !== 2) return this.fail(player, 'INVALID_PLAYER_COUNT', 'PvP requires exactly two players', requestId);
    if ([...lobby.players.values()].some((member) => member.id !== lobby.hostId && !member.ready)) return this.fail(player, 'PLAYERS_NOT_READY', 'All guests must be ready', requestId);

    lobby.started = true;
    const match = this.createMatch(lobby);
    lobby.matchId = match.id;
    this.matches.set(match.id, match);
    this.broadcast(lobby, 'lobby.started', {
      lobby_id: lobby.id,
      match_id: match.id,
      host_id: lobby.hostId,
      seed: randomInt(1, 2_147_483_647),
      settings: lobby.settings,
      players: this.matchPlayers(match),
      items: this.matchItems(match),
    });
    this.broadcastGameState(match);
  }

  private gameInput(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const match = this.matchFor(player, payload.match_id);
    if (!match) return this.fail(player, 'NOT_IN_MATCH', 'Player is not in this match', requestId);
    const state = match.players.get(player.id)!;
    const position = this.vector(payload.position);
    const velocity = this.vector(payload.velocity);
    const yaw = Number(payload.yaw);
    if (!position || !velocity || !Number.isFinite(yaw)) return this.fail(player, 'INVALID_INPUT', 'Invalid player state', requestId);
    state.position = {
      x: this.clamp(position.x, -12, 12),
      y: this.clamp(position.y, -8, 20),
      z: this.clamp(position.z, -12, 12),
    };
    state.velocity = {
      x: this.clamp(velocity.x, -25, 25),
      y: this.clamp(velocity.y, -25, 25),
      z: this.clamp(velocity.z, -25, 25),
    };
    state.yaw = this.clamp(yaw, -Math.PI * 4, Math.PI * 4);
  }

  private gameHit(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const match = this.matchFor(player, payload.match_id);
    if (!match) return this.fail(player, 'NOT_IN_MATCH', 'Player is not in this match', requestId);
    const source = match.players.get(player.id)!;
    const targetId = typeof payload.target_id === 'string' ? payload.target_id : '';
    const target = match.players.get(targetId);
    const damage = Number(payload.damage);
    const knockback = this.vector(payload.knockback);
    if (!target || target.id === source.id || !Number.isInteger(damage) || damage < 1 || damage > 2 || !knockback) {
      return this.fail(player, 'INVALID_HIT', 'Invalid hit', requestId);
    }
    const now = Date.now();
    if (now - source.lastHitAt < 120 || target.health <= 0) return;
    source.lastHitAt = now;
    target.health = Math.max(0, target.health - damage);
    this.broadcastMatch(match, 'game.hit', {
      match_id: match.id,
      source_id: source.id,
      target_id: target.id,
      damage,
      health: target.health,
      knockback: {
        x: this.clamp(knockback.x, -20, 20),
        y: this.clamp(knockback.y, -20, 20),
        z: this.clamp(knockback.z, -20, 20),
      },
    });
  }

  private pickupItem(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const match = this.matchFor(player, payload.match_id);
    if (!match) return this.fail(player, 'NOT_IN_MATCH', 'Player is not in this match', requestId);
    const state = match.players.get(player.id)!;
    const itemId = typeof payload.item_id === 'string' ? payload.item_id : '';
    const item = match.items.get(itemId);
    if (!item) return this.fail(player, 'ITEM_NOT_FOUND', 'Item does not exist', requestId);
    if (state.heldFood) return this.fail(player, 'ALREADY_HOLDING_ITEM', 'Player already holds food', requestId);
    if (Date.now() < item.pickupAvailableAt) return this.fail(player, 'ITEM_PICKUP_DELAY', 'Item cannot be picked up yet', requestId);
    if (this.distanceXZ(state.position, item.position) > 1.75) return this.fail(player, 'ITEM_TOO_FAR', 'Item is too far away', requestId);

    match.items.delete(item.id);
    state.heldFood = item.foodType;
    this.broadcastMatch(match, 'game.item.picked', {
      match_id: match.id,
      item_id: item.id,
      player_id: state.id,
      food_type: item.foodType,
    });
  }

  private dropItem(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const match = this.matchFor(player, payload.match_id);
    if (!match) return this.fail(player, 'NOT_IN_MATCH', 'Player is not in this match', requestId);
    const state = match.players.get(player.id)!;
    if (!state.heldFood) return this.fail(player, 'NOT_HOLDING_ITEM', 'Player does not hold food', requestId);
    if (match.items.size >= MAX_ITEMS) return this.fail(player, 'ITEM_LIMIT', 'Maximum item count reached', requestId);
    const position = this.vector(payload.position);
    if (!position) return this.fail(player, 'INVALID_POSITION', 'Invalid drop position', requestId);
    if (this.distance(state.position, position) > 3) return this.fail(player, 'DROP_TOO_FAR', 'Drop position is too far away', requestId);

    const foodType = state.heldFood;
    state.heldFood = null;
    const item = this.createItem(match, foodType, {
      x: this.clamp(position.x, -10, 10),
      y: this.clamp(position.y, 0.1, 2),
      z: this.clamp(position.z, -10, 10),
    }, 750);
    this.broadcastItemSpawn(match, item);
  }

  private itemHit(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const match = this.matchFor(player, payload.match_id);
    if (!match) return this.fail(player, 'NOT_IN_MATCH', 'Player is not in this match', requestId);
    const itemId = typeof payload.item_id === 'string' ? payload.item_id : '';
    const targetId = typeof payload.target_id === 'string' ? payload.target_id : '';
    const item = match.items.get(itemId);
    const target = match.players.get(targetId);
    const knockback = this.vector(payload.knockback);
    if (!item) return this.fail(player, 'ITEM_NOT_FOUND', 'Item does not exist', requestId);
    if (item.bonked) return this.fail(player, 'ITEM_ALREADY_BONKED', 'Item already caused damage', requestId);
    if (!target || target.id !== player.id) return this.fail(player, 'INVALID_TARGET', 'Hit must be reported by its target', requestId);
    if (target.health <= 0) return this.fail(player, 'TARGET_DEAD', 'Target is already dead', requestId);
    if (this.distanceXZ(target.position, item.position) > 1.75) return this.fail(player, 'ITEM_TOO_FAR', 'Item is too far from target', requestId);
    if (!knockback) return this.fail(player, 'INVALID_KNOCKBACK', 'Invalid knockback vector', requestId);

    item.bonked = true;
    target.health = Math.max(0, target.health - 1);
    this.broadcastMatch(match, 'game.hit', {
      match_id: match.id,
      source_id: '',
      item_id: item.id,
      target_id: target.id,
      damage: 1,
      health: target.health,
      knockback: this.clampVector(knockback, -20, 20),
    });
  }

  private despawnItem(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const match = this.matchFor(player, payload.match_id);
    if (!match) return this.fail(player, 'NOT_IN_MATCH', 'Player is not in this match', requestId);
    if (match.hostId !== player.id) return this.fail(player, 'NOT_HOST', 'Only the host can despawn items', requestId);
    const itemId = typeof payload.item_id === 'string' ? payload.item_id : '';
    if (!match.items.delete(itemId)) return this.fail(player, 'ITEM_NOT_FOUND', 'Item does not exist', requestId);
    this.broadcastMatch(match, 'game.item.despawn', { match_id: match.id, item_id: itemId });
  }

  private spawnProjectile(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const match = this.matchFor(player, payload.match_id);
    if (!match) return this.fail(player, 'NOT_IN_MATCH', 'Player is not in this match', requestId);
    const state = match.players.get(player.id)!;
    if (!state.heldFood) return this.fail(player, 'NOT_HOLDING_ITEM', 'Player does not hold food', requestId);
    if (payload.food_type !== state.heldFood) return this.fail(player, 'INVALID_FOOD_TYPE', 'Food type does not match held food', requestId);
    const origin = this.vector(payload.origin);
    const velocity = this.vector(payload.velocity);
    const multiplier = this.finiteNumber(payload.knockback_multiplier);
    if (!origin || !velocity) return this.fail(player, 'INVALID_PROJECTILE', 'Invalid projectile vectors', requestId);
    if (this.distance(state.position, origin) > 3.5) return this.fail(player, 'ORIGIN_TOO_FAR', 'Projectile origin is too far away', requestId);
    if (this.vectorLength(velocity) > 40) return this.fail(player, 'VELOCITY_TOO_HIGH', 'Projectile velocity is too high', requestId);
    if (multiplier === undefined) return this.fail(player, 'INVALID_KNOCKBACK_MULTIPLIER', 'Invalid knockback multiplier', requestId);

    const foodType = state.heldFood;
    const definition = FOOD[foodType];
    const projectile: ProjectileState = {
      id: randomUUID(),
      sourceId: state.id,
      foodType,
      origin: this.clampVector(origin, -20, 20),
      velocity: this.clampVector(velocity, -40, 40),
      knockbackMultiplier: this.clamp(multiplier, 1, 1.5),
      damage: definition.damage,
      aoe: definition.aoe,
      createdAt: Date.now(),
      hitTargets: new Set(),
    };
    state.heldFood = null;
    match.projectiles.set(projectile.id, projectile);
    this.broadcastMatch(match, 'game.projectile.spawn', this.projectilePayload(match, projectile));
  }

  private projectileHit(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const match = this.matchFor(player, payload.match_id);
    if (!match) return this.fail(player, 'NOT_IN_MATCH', 'Player is not in this match', requestId);
    const projectileId = typeof payload.projectile_id === 'string' ? payload.projectile_id : '';
    const targetId = typeof payload.target_id === 'string' ? payload.target_id : '';
    const projectile = match.projectiles.get(projectileId);
    const target = match.players.get(targetId);
    const knockback = this.vector(payload.knockback);
    if (!projectile) return this.fail(player, 'PROJECTILE_NOT_FOUND', 'Projectile does not exist', requestId);
    if (projectile.sourceId !== player.id) return this.fail(player, 'NOT_PROJECTILE_OWNER', 'Only projectile owner can report hits', requestId);
    if (!target || target.health <= 0) return this.fail(player, 'INVALID_TARGET', 'Target does not exist or is dead', requestId);
    if (projectile.hitTargets.has(target.id)) return this.fail(player, 'TARGET_ALREADY_HIT', 'Projectile already hit this target', requestId);
    if (!projectile.aoe && projectile.hitTargets.size > 0) return this.fail(player, 'PROJECTILE_ALREADY_HIT', 'Projectile can hit only one target', requestId);
    if (!knockback) return this.fail(player, 'INVALID_KNOCKBACK', 'Invalid knockback vector', requestId);

    projectile.hitTargets.add(target.id);
    target.health = Math.max(0, target.health - projectile.damage);
    this.broadcastMatch(match, 'game.hit', {
      match_id: match.id,
      source_id: projectile.sourceId,
      projectile_id: projectile.id,
      target_id: target.id,
      damage: projectile.damage,
      health: target.health,
      knockback: this.clampVector(knockback, -20, 20),
    });
  }

  private despawnProjectile(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const match = this.matchFor(player, payload.match_id);
    if (!match) return this.fail(player, 'NOT_IN_MATCH', 'Player is not in this match', requestId);
    const projectileId = typeof payload.projectile_id === 'string' ? payload.projectile_id : '';
    const projectile = match.projectiles.get(projectileId);
    if (!projectile) return this.fail(player, 'PROJECTILE_NOT_FOUND', 'Projectile does not exist', requestId);
    if (projectile.sourceId !== player.id) return this.fail(player, 'NOT_PROJECTILE_OWNER', 'Only projectile owner can despawn it', requestId);
    this.removeProjectile(match, projectile.id);
  }

  private resetRound(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const match = this.matchFor(player, payload.match_id);
    if (!match) return this.fail(player, 'NOT_IN_MATCH', 'Player is not in this match', requestId);
    if (match.hostId !== player.id) return this.fail(player, 'NOT_HOST', 'Only the host can reset the round', requestId);
    for (const state of match.players.values()) {
      state.position = this.spawnPosition(state.spawnSlot);
      state.velocity = { x: 0, y: 0, z: 0 };
      state.health = 3;
      state.heldFood = null;
      state.lastHitAt = 0;
    }
    match.items.clear();
    match.projectiles.clear();
    const item = this.createItem(match);
    match.nextItemSpawnAt = Date.now() + ITEM_SPAWN_INTERVAL_MS;
    this.broadcastMatch(match, 'game.round.started', {
      match_id: match.id,
      reset_match: payload.reset_match === true,
      players: this.matchPlayers(match),
      items: [this.itemPayload(match, item)],
    });
  }

  private leave(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    if (typeof payload.lobby_id === 'string' && payload.lobby_id.toUpperCase() !== player.lobbyId) return this.fail(player, 'NOT_IN_LOBBY', 'Player is not in this lobby', requestId);
    const lobbyId = player.lobbyId;
    this.leaveLobby(player);
    this.send(player, 'lobby.left', { lobby_id: lobbyId }, requestId);
  }

  private leaveLobby(player: Player) {
    if (!player.lobbyId) return;
    const lobby = this.lobbies.get(player.lobbyId);
    player.lobbyId = null;
    player.ready = false;
    if (!lobby) return;
    if (lobby.matchId) {
      const match = this.matches.get(lobby.matchId);
      if (match) this.broadcastMatch(match, 'game.player_left', {
        match_id: match.id,
        player_id: player.id,
        message: 'Второй игрок отключился',
      });
      this.matches.delete(lobby.matchId);
      for (const member of lobby.players.values()) {
        member.lobbyId = null;
        member.ready = false;
      }
      this.lobbies.delete(lobby.id);
      return;
    }
    lobby.players.delete(player.id);
    if (!lobby.players.size) return void this.lobbies.delete(lobby.id);
    if (lobby.hostId === player.id) lobby.hostId = lobby.players.keys().next().value as string;
    this.broadcastUpdate(lobby);
  }

  private lobbyFor(player: Player, suppliedId: unknown) {
    if (!player.lobbyId || (typeof suppliedId === 'string' && suppliedId.toUpperCase() !== player.lobbyId)) return undefined;
    return this.lobbies.get(player.lobbyId);
  }

  private defaultSettings(): Settings { return { max_players: 2, round_time: 90, wins_to_match: 2 }; }

  private validateSettings(raw: Record<string, unknown>, base: Settings): Settings | undefined {
    const maxPlayers = raw.max_players === undefined ? base.max_players : Number(raw.max_players);
    const roundTime = raw.round_time === undefined ? base.round_time : Number(raw.round_time);
    const winsToMatch = raw.wins_to_match === undefined ? base.wins_to_match : Number(raw.wins_to_match);
    if (!Number.isInteger(maxPlayers) || maxPlayers !== 2 || !Number.isInteger(roundTime) || roundTime < 30 || roundTime > 300 || !Number.isInteger(winsToMatch) || winsToMatch < 1 || winsToMatch > 9) return undefined;
    return { max_players: 2, round_time: roundTime, wins_to_match: winsToMatch };
  }

  private newLobbyId() {
    do {
      let id = '';
      for (let index = 0; index < 6; index++) id += LOBBY_CODE_ALPHABET[randomInt(LOBBY_CODE_ALPHABET.length)];
      if (!this.lobbies.has(id)) return id;
    } while (true);
  }

  private lobbyPayload(lobby: Lobby) {
    const canStart = lobby.players.size === 2 && [...lobby.players.values()].every(({ id, ready }) => id === lobby.hostId || ready);
    return { lobby_id: lobby.id, host_id: lobby.hostId, settings: lobby.settings, players: [...lobby.players.values()].map(({ id, name, ready }) => ({ id, name, ready: id === lobby.hostId || ready, is_host: id === lobby.hostId })), can_start: canStart };
  }

  private createMatch(lobby: Lobby): Match {
    const match: Match = { id: randomUUID(), lobbyId: lobby.id, hostId: lobby.hostId, players: new Map(), items: new Map(), projectiles: new Map(), nextItemSpawnAt: Date.now() + ITEM_SPAWN_INTERVAL_MS };
    [...lobby.players.values()].forEach((member, spawnSlot) => match.players.set(member.id, {
      id: member.id,
      name: member.name,
      spawnSlot,
      position: this.spawnPosition(spawnSlot),
      velocity: { x: 0, y: 0, z: 0 },
      yaw: spawnSlot === 0 ? -Math.PI / 2 : Math.PI / 2,
      health: 3,
      heldFood: null,
      lastHitAt: 0,
    }));
    this.createItem(match);
    return match;
  }

  private matchFor(player: Player, suppliedId: unknown) {
    if (!player.lobbyId || typeof suppliedId !== 'string') return undefined;
    const lobby = this.lobbies.get(player.lobbyId);
    if (!lobby?.started || lobby.matchId !== suppliedId) return undefined;
    const match = this.matches.get(suppliedId);
    return match?.players.has(player.id) ? match : undefined;
  }

  private matchPlayers(match: Match) {
    return [...match.players.values()].map((state) => ({
      id: state.id,
      name: state.name,
      spawn_slot: state.spawnSlot,
      spawn: this.spawnPosition(state.spawnSlot),
      position: state.position,
      velocity: state.velocity,
      yaw: state.yaw,
      health: state.health,
      held_food: state.heldFood ?? '',
    }));
  }

  private broadcastGameStates() {
    const now = Date.now();
    for (const match of this.matches.values()) {
      if (now >= match.nextItemSpawnAt) {
        match.nextItemSpawnAt = now + ITEM_SPAWN_INTERVAL_MS;
        if (match.items.size < MAX_ITEMS) this.broadcastItemSpawn(match, this.createItem(match));
      }
      for (const projectile of match.projectiles.values()) {
        if (now - projectile.createdAt >= PROJECTILE_TTL_MS) this.removeProjectile(match, projectile.id);
      }
      this.broadcastGameState(match);
    }
  }
  private broadcastGameState(match: Match) { this.broadcastMatch(match, 'game.state', { match_id: match.id, server_time: Date.now(), players: this.matchPlayers(match), items: this.matchItems(match) }); }
  private broadcastMatch(match: Match, type: string, payload: object) {
    const lobby = this.lobbies.get(match.lobbyId);
    if (lobby) this.broadcast(lobby, type, payload);
  }
  private spawnPosition(slot: number): VectorState { return { x: slot === 0 ? -4.5 : 4.5, y: 0.1, z: 0 }; }
  private vector(value: unknown): VectorState | undefined {
    const raw = this.object(value);
    if (!raw) return undefined;
    const x = Number(raw.x); const y = Number(raw.y); const z = Number(raw.z);
    return Number.isFinite(x) && Number.isFinite(y) && Number.isFinite(z) ? { x, y, z } : undefined;
  }
  private finiteNumber(value: unknown) { return typeof value === 'number' && Number.isFinite(value) ? value : undefined; }
  private vectorLength(value: VectorState) { return Math.hypot(value.x, value.y, value.z); }
  private distance(a: VectorState, b: VectorState) { return Math.hypot(a.x - b.x, a.y - b.y, a.z - b.z); }
  private distanceXZ(a: VectorState, b: VectorState) { return Math.hypot(a.x - b.x, a.z - b.z); }
  private clampVector(value: VectorState, min: number, max: number): VectorState { return { x: this.clamp(value.x, min, max), y: this.clamp(value.y, min, max), z: this.clamp(value.z, min, max) }; }
  private clamp(value: number, min: number, max: number) { return Math.max(min, Math.min(max, value)); }

  private randomFoodType(): FoodType {
    const totalWeight = Object.values(FOOD).reduce((sum, food) => sum + food.weight, 0);
    let roll = randomInt(totalWeight);
    for (const [foodType, definition] of Object.entries(FOOD) as [FoodType, typeof FOOD[FoodType]][]) {
      roll -= definition.weight;
      if (roll < 0) return foodType;
    }
    return 'tomato';
  }

  private randomItemPosition(): VectorState {
    const angle = Math.random() * Math.PI * 2;
    const radius = Math.sqrt(Math.random()) * 7;
    return { x: Math.cos(angle) * radius, y: 9, z: Math.sin(angle) * radius };
  }

  private createItem(match: Match, foodType = this.randomFoodType(), position = this.randomItemPosition(), pickupDelayMs = 0) {
    const item: FoodItemState = { id: randomUUID(), foodType, position, pickupAvailableAt: Date.now() + pickupDelayMs, bonked: false };
    match.items.set(item.id, item);
    return item;
  }

  private itemPayload(match: Match, item: FoodItemState) {
    return { match_id: match.id, item_id: item.id, food_type: item.foodType, position: item.position, pickup_delay_ms: Math.max(0, item.pickupAvailableAt - Date.now()) };
  }

  private matchItems(match: Match) { return [...match.items.values()].map((item) => this.itemPayload(match, item)); }
  private broadcastItemSpawn(match: Match, item: FoodItemState) { this.broadcastMatch(match, 'game.item.spawn', this.itemPayload(match, item)); }
  private projectilePayload(match: Match, projectile: ProjectileState) {
    return { match_id: match.id, projectile_id: projectile.id, source_id: projectile.sourceId, food_type: projectile.foodType, origin: projectile.origin, velocity: projectile.velocity, knockback_multiplier: projectile.knockbackMultiplier, damage: projectile.damage, aoe: projectile.aoe, server_time: projectile.createdAt };
  }
  private removeProjectile(match: Match, projectileId: string) {
    if (!match.projectiles.delete(projectileId)) return;
    this.broadcastMatch(match, 'game.projectile.despawn', { match_id: match.id, projectile_id: projectileId });
  }

  private broadcastUpdate(lobby: Lobby) { this.broadcast(lobby, 'lobby.updated', this.lobbyPayload(lobby)); }
  private broadcast(lobby: Lobby, type: string, payload: object) { for (const player of lobby.players.values()) this.send(player, type, payload); }
  private send(player: Player, type: string, payload: object, requestId: unknown = null) { if (!player.socket.destroyed) player.socket.write(JSON.stringify({ type, request_id: requestId, payload }) + '\n'); }
  private fail(player: Player, code: string, message: string, requestId: unknown = null) { this.send(player, 'error', { code, message }, requestId); }
  private object(value: unknown): Record<string, unknown> | undefined { return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : undefined; }
}
