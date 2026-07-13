import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { randomInt, randomUUID } from 'node:crypto';
import { createServer, Server, Socket } from 'node:net';

const MAX_LINE_BYTES = 1_048_576;
const MAX_PACKETS_PER_SECOND = 30;
const MAX_NAME_LENGTH = 32;
const LOBBY_CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

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
};

@Injectable()
export class TcpGatewayService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TcpGatewayService.name);
  private readonly lobbies = new Map<string, Lobby>();
  private server?: Server;
  readonly port = Number(process.env.TCP_PORT ?? 7778);
  readonly host = process.env.TCP_HOST ?? '127.0.0.1';

  get lobbyCount() { return this.lobbies.size; }

  onModuleInit() {
    this.server = createServer((socket) => this.handleConnection(socket));
    this.server.listen(this.port, this.host, () => this.logger.log(`TCP lobby server listening on ${this.host}:${this.port}`));
  }

  onModuleDestroy() {
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

    const lobby: Lobby = { id: this.newLobbyId(), hostId: player.id, settings, players: new Map(), started: false };
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
    if (lobby.players.size < 2) return this.fail(player, 'NOT_ENOUGH_PLAYERS', 'At least two players are required', requestId);
    if ([...lobby.players.values()].some((member) => member.id !== lobby.hostId && !member.ready)) return this.fail(player, 'PLAYERS_NOT_READY', 'All guests must be ready', requestId);

    lobby.started = true;
    this.broadcast(lobby, 'lobby.started', { lobby_id: lobby.id, match_id: randomUUID(), seed: randomInt(0, 2 ** 32), settings: lobby.settings });
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
    if (!Number.isInteger(maxPlayers) || maxPlayers < 2 || maxPlayers > 4 || !Number.isInteger(roundTime) || roundTime < 30 || roundTime > 300 || !Number.isInteger(winsToMatch) || winsToMatch < 1 || winsToMatch > 9) return undefined;
    return { max_players: maxPlayers, round_time: roundTime, wins_to_match: winsToMatch };
  }

  private newLobbyId() {
    do {
      let id = '';
      for (let index = 0; index < 6; index++) id += LOBBY_CODE_ALPHABET[randomInt(LOBBY_CODE_ALPHABET.length)];
      if (!this.lobbies.has(id)) return id;
    } while (true);
  }

  private lobbyPayload(lobby: Lobby) {
    return { lobby_id: lobby.id, host_id: lobby.hostId, settings: lobby.settings, players: [...lobby.players.values()].map(({ id, name, ready }) => ({ id, name, ready, is_host: id === lobby.hostId })) };
  }

  private broadcastUpdate(lobby: Lobby) { this.broadcast(lobby, 'lobby.updated', this.lobbyPayload(lobby)); }
  private broadcast(lobby: Lobby, type: string, payload: object) { for (const player of lobby.players.values()) this.send(player, type, payload); }
  private send(player: Player, type: string, payload: object, requestId: unknown = null) { if (!player.socket.destroyed) player.socket.write(JSON.stringify({ type, request_id: requestId, payload }) + '\n'); }
  private fail(player: Player, code: string, message: string, requestId: unknown = null) { this.send(player, 'error', { code, message }, requestId); }
  private object(value: unknown): Record<string, unknown> | undefined { return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : undefined; }
}
