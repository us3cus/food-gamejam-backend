# Food Gamejam backend

NestJS-приложение поднимает два интерфейса:

- HTTP JSON: `GET /health` (по умолчанию `http://localhost:3002/health`);
- сырой TCP: `127.0.0.1:7778`, опубликованный nginx как `onk.temten.me:7777`.

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

Порты настраиваются переменными окружения: `HTTP_PORT` (по умолчанию `3002`), `TCP_PORT` (по умолчанию `7778`) и `TCP_HOST` (по умолчанию `127.0.0.1`).

## Реализованный протокол

Поддержаны `session.hello`, `lobby.create`, `lobby.join`, `lobby.settings`, `lobby.ready`, `lobby.leave`, `lobby.invite` и `lobby.start`, а также ответы и рассылки `session.welcome`, `lobby.created`, `lobby.joined`, `lobby.left`, `lobby.updated`, `lobby.started` и `error`.

Сервер буферизует TCP-поток до новой строки, отвергает строки свыше 1 МиБ и ограничивает клиента 30 пакетами в секунду. Состояние лобби хранится в памяти; после старта клиенты получают единые `match_id`, `seed` и настройки матча.
