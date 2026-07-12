# Food Gamejam backend

NestJS-приложение поднимает два интерфейса:

- HTTP JSON: `GET /health` (по умолчанию `http://localhost:3000/health`);
- сырой TCP: `0.0.0.0:7777` для Godot-клиента.

TCP — это не WebSocket. Пакет представляет собой один UTF-8 JSON-объект с завершающим `\n`:

```json
{"type":"session.hello","request_id":1,"payload":{"protocol":1,"player_name":"Player"}}
```

## Запуск

```bash
npm install
npm run build
npm start
```

Для разработки можно использовать `npm run start:dev`.

Порты настраиваются переменными окружения: `HTTP_PORT` (по умолчанию `3000`), `TCP_PORT` (`7777`) и `TCP_HOST` (`0.0.0.0`).

## Реализованный протокол

Поддержаны `session.hello`, `lobby.create`, `lobby.join`, `lobby.leave`, `lobby.invite`, `lobby.start` и `game.input`, а также ответы и рассылки `session.welcome`, `lobby.created`, `lobby.joined`, `lobby.left`, `lobby.members`, `lobby.started`, `game.state` и `error`.

Сервер буферизует TCP-поток до новой строки, отвергает строки свыше 1 МиБ и ограничивает клиента 30 пакетами в секунду. Состояние лобби и позиции хранятся в памяти; игровые координаты меняет только сервер на фиксированных 20 TPS.
