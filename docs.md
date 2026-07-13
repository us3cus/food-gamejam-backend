# TCP-сервер лобби

## Схема подключения

Godot-клиент подключается raw TCP к `onk.temten.me:7777`. TLS на игровом TCP не используется; HTTPS-сайт работает отдельно на `https://onk.temten.me`.

```text
Godot -> onk.temten.me:7777 -> nginx stream -> 127.0.0.1:7778 -> Node.js
```

```ini
[network]
lobby/host="onk.temten.me"
lobby/port=7777
```

```env
TCP_HOST=127.0.0.1
TCP_PORT=7778
```

## Запуск

Требуется Node.js 20 или новее.

```bash
npm install
npm run build
TCP_HOST=127.0.0.1 TCP_PORT=7778 HTTP_PORT=3002 npm start
npm test
```

## Nginx stream

Конфигурация располагается в контексте `stream`, отдельно от HTTP virtual host. Внешний firewall открывает только `7777/tcp`; `7778` остаётся доступным только на loopback.

```nginx
stream {
    upstream food_gamejam_tcp {
        server 127.0.0.1:7778;
    }

    server {
        listen 7777;
        proxy_connect_timeout 5s;
        proxy_timeout 1h;
        proxy_pass food_gamejam_tcp;
    }
}
```

После изменения конфигурации: `sudo nginx -t` и `sudo systemctl reload nginx`.

## Протокол

Каждый пакет — UTF-8 JSON-объект и символ `\n`. Максимальный размер одного пакета — 1 MiB. Оболочка пакета:

```json
{"type":"lobby.create","request_id":2,"payload":{}}
```

1. Клиент отправляет `session.hello` с версией протокола и именем.
2. Затем создаёт лобби или входит по шестизначному коду.
3. Сервер отправляет `lobby.updated` при изменении состава, готовности и настроек.
4. Гости отправляют `lobby.ready`.
5. Хост отправляет `lobby.start`.
6. Сервер проверяет минимум двух игроков и готовность всех гостей, после чего рассылает `lobby.started` с общими `match_id`, `seed` и настройками.

| Тип | Payload |
| --- | --- |
| `session.hello` | `{ "protocol": 1, "player_name": "Игрок" }` |
| `lobby.create` | `{ "settings": { "max_players": 2, "round_time": 90, "wins_to_match": 2 } }` |
| `lobby.join` | `{ "lobby_id": "ABC234" }` |
| `lobby.settings` | `{ "lobby_id": "ABC234", "settings": { ... } }` |
| `lobby.ready` | `{ "lobby_id": "ABC234", "ready": true }` |
| `lobby.invite` | `{ "lobby_id": "ABC234" }` |
| `lobby.start` | `{ "lobby_id": "ABC234" }` |
| `lobby.leave` | `{ "lobby_id": "ABC234" }` |

Настройки проверяются сервером: `max_players` — от 2 до 4, `round_time` — от 30 до 300 секунд, `wins_to_match` — от 1 до 9. Лобби хранятся в памяти. При выходе хоста права получает следующий игрок, пустое лобби удаляется.

После `lobby.started` клиенты открывают арену и применяют одинаковые настройки и seed. Синхронизация движений, снарядов и результатов раунда не входит в текущий протокол.
