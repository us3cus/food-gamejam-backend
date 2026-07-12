import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { createServer, Server, Socket } from 'node:net';

const MAX_LINE_BYTES = 1_048_576;
const MAX_PACKETS_PER_SECOND = 30;
const TICK_RATE = 20;
const MAX_NAME_LENGTH = 32;
const MAX_INPUT = 1;

type Packet = { type?: unknown; request_id?: unknown; payload?: unknown };
type PlayerView = { id: string; name: string };
type Input = { moveX: number; moveY: number; action: boolean };
type Player = { id: string; name: string; socket: Socket; lobbyId: string | null; input: Input; x: number; y: number; packetTimes: number[] };
type Lobby = { id: string; hostId: string; maxMembers: number; players: Map<string, Player>; started: boolean; tick: number };

@Injectable()
export class TcpGatewayService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TcpGatewayService.name);
  private readonly lobbies = new Map<string, Lobby>();
  private server?: Server;
  private timer?: NodeJS.Timeout;
  readonly port = Number(process.env.TCP_PORT ?? 7777);

  get lobbyCount() { return this.lobbies.size; }

  onModuleInit() {
    this.server = createServer((socket) => this.handleConnection(socket));
    this.server.listen(this.port, process.env.TCP_HOST ?? '0.0.0.0', () => this.logger.log(`TCP lobby server listening on ${this.port}`));
    this.timer = setInterval(() => this.simulate(), 1000 / TICK_RATE);
  }

  onModuleDestroy() {
    if (this.timer) clearInterval(this.timer);
    this.server?.close();
  }

  private handleConnection(socket: Socket) {
    socket.setNoDelay(true);
    let buffer = Buffer.alloc(0);
    const player: Player = { id: randomUUID(), name: 'Player', socket, lobbyId: null, input: { moveX: 0, moveY: 0, action: false }, x: 0, y: 0, packetTimes: [] };
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
    player.packetTimes = player.packetTimes.filter((at) => now - at < 1000);
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
      case 'lobby.leave': return this.leave(player, payload, requestId);
      case 'lobby.invite': return this.invite(player, payload, requestId);
      case 'lobby.start': return this.start(player, payload, requestId);
      case 'game.input': return this.input(player, payload, requestId);
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
    const requested = Number(payload.max_members);
    const lobby: Lobby = { id: randomUUID(), hostId: player.id, maxMembers: Number.isInteger(requested) ? Math.max(1, Math.min(requested, 4)) : 4, players: new Map(), started: false, tick: 0 };
    lobby.players.set(player.id, player); player.lobbyId = lobby.id; this.lobbies.set(lobby.id, lobby);
    this.send(player, 'lobby.created', { lobby_id: lobby.id, can_start: true, players: this.members(lobby) }, requestId);
  }

  private joinLobby(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const lobby = typeof payload.lobby_id === 'string' ? this.lobbies.get(payload.lobby_id) : undefined;
    if (!lobby) return this.fail(player, 'LOBBY_NOT_FOUND', 'Lobby does not exist', requestId);
    if (lobby.started) return this.fail(player, 'LOBBY_STARTED', 'Game already started', requestId);
    if (lobby.players.size >= lobby.maxMembers) return this.fail(player, 'LOBBY_FULL', 'Lobby is full', requestId);
    this.leaveLobby(player); lobby.players.set(player.id, player); player.lobbyId = lobby.id;
    this.send(player, 'lobby.joined', { lobby_id: lobby.id, can_start: player.id === lobby.hostId, players: this.members(lobby) }, requestId);
    this.broadcast(lobby, 'lobby.members', { lobby_id: lobby.id, players: this.members(lobby) });
  }

  private leave(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    if (typeof payload.lobby_id === 'string' && payload.lobby_id !== player.lobbyId) return this.fail(player, 'NOT_IN_LOBBY', 'Player is not in this lobby', requestId);
    const lobbyId = player.lobbyId; this.leaveLobby(player);
    this.send(player, 'lobby.left', { lobby_id: lobbyId }, requestId);
  }

  private invite(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const lobby = this.ownLobby(player, payload.lobby_id);
    if (!lobby) return this.fail(player, 'LOBBY_NOT_FOUND', 'Lobby does not exist', requestId);
    this.send(player, 'lobby.invite', { invite_code: lobby.id, url: `lobby.join:${lobby.id}` }, requestId);
  }

  private start(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const lobby = this.ownLobby(player, payload.lobby_id);
    if (!lobby) return this.fail(player, 'LOBBY_NOT_FOUND', 'Lobby does not exist', requestId);
    if (lobby.hostId !== player.id) return this.fail(player, 'NOT_HOST', 'Only the host can start', requestId);
    lobby.started = true; this.broadcast(lobby, 'lobby.started', { lobby_id: lobby.id }, requestId);
  }

  private input(player: Player, payload: Record<string, unknown>, requestId: unknown) {
    const lobby = player.lobbyId ? this.lobbies.get(player.lobbyId) : undefined;
    if (!lobby?.started) return this.fail(player, 'GAME_NOT_STARTED', 'Game has not started', requestId);
    const input = this.object(payload.input);
    if (!input || !Number.isFinite(input.move_x) || !Number.isFinite(input.move_y) || typeof input.action !== 'boolean') return this.fail(player, 'INVALID_INPUT', 'Invalid game input', requestId);
    player.input = { moveX: Math.max(-MAX_INPUT, Math.min(MAX_INPUT, Number(input.move_x))), moveY: Math.max(-MAX_INPUT, Math.min(MAX_INPUT, Number(input.move_y))), action: input.action };
  }

  private simulate() {
    for (const lobby of this.lobbies.values()) {
      if (!lobby.started) continue;
      lobby.tick++;
      for (const player of lobby.players.values()) { player.x += player.input.moveX * 5; player.y += player.input.moveY * 5; }
      this.broadcast(lobby, 'game.state', { tick: lobby.tick, players: [...lobby.players.values()].map(({ id, x, y }) => ({ id, x, y })) });
    }
  }

  private leaveLobby(player: Player) {
    if (!player.lobbyId) return;
    const lobby = this.lobbies.get(player.lobbyId); player.lobbyId = null;
    if (!lobby) return;
    lobby.players.delete(player.id);
    if (lobby.hostId === player.id && lobby.players.size) lobby.hostId = lobby.players.keys().next().value as string;
    if (!lobby.players.size) this.lobbies.delete(lobby.id);
    else this.broadcast(lobby, 'lobby.members', { lobby_id: lobby.id, players: this.members(lobby) });
  }

  private ownLobby(player: Player, suppliedId: unknown) { return player.lobbyId && (!suppliedId || suppliedId === player.lobbyId) ? this.lobbies.get(player.lobbyId) : undefined; }
  private members(lobby: Lobby): PlayerView[] { return [...lobby.players.values()].map(({ id, name }) => ({ id, name })); }
  private broadcast(lobby: Lobby, type: string, payload: object, requestId: unknown = null) { for (const player of lobby.players.values()) this.send(player, type, payload, requestId); }
  private send(player: Player, type: string, payload: object, requestId: unknown = null) { if (!player.socket.destroyed) player.socket.write(JSON.stringify({ type, request_id: requestId, payload }) + '\n'); }
  private fail(player: Player, code: string, message: string, requestId: unknown = null) { this.send(player, 'error', { code, message }, requestId); }
  private object(value: unknown): Record<string, any> | undefined { return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, any> : undefined; }
}
