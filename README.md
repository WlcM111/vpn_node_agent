# vpn-node-agent

Агент VPN-ноды и входного relay-сервера: слушает команды из Kafka центрального
сервера, управляет пользователями Xray через gRPC API (`vless-reality-in`,
`vless-xhttp-in`, `hysteria-in`, `vless-xhttp-cdn-in`), обслуживает локальный
HTTP-эндпоинт аутентификации Hysteria2 (`/hysteria/auth`) без логирования
клиентских IP-адресов, шлёт heartbeat и статистику трафика обратно в центр.

## Сборка

    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" \
      -o bin/vpn-node-agent ./cmd/vpn-node-agent

## Запуск

См. `deploy/docker-compose.node.yml` (Docker) или systemd-юнит (нативно).
Переменные окружения — в `.env` (шаблон `deploy/.env.node.example`).

## Переменные окружения

- `NODE_ID`, `SERVER_KEY` — идентификаторы ноды или relay-сервера (например, `ee-relay-1`, `cas-main-1`)
- `KAFKA_BROKERS` — адрес Kafka центрального сервера (`race-src.com:19092`)
- `XRAY_APPLY_MODE` — `api` | `dry-run`
- `XRAY_API_ADDR` — адрес gRPC Xray (`127.0.0.1:10085`)
- `NODE_AGENT_STATE_PATH`, `NODE_AGENT_PENDING_PATH` — пути файлов состояния
- `NODE_AGENT_HEARTBEAT_INTERVAL`, `NODE_AGENT_TRAFFIC_INTERVAL`, `NODE_AGENT_APPLY_TIMEOUT`
- `NODE_AGENT_HTTP_ADDR` — адрес healthz/metrics и `/hysteria/auth` (`127.0.0.1:9091`)
- `NODE_AGENT_INBOUND_TAGS` — инбаунды Xray через запятую, по которым считается
  скорость трафика ноды для heartbeat (на relay рекомендуется `vless-reality-in,vless-xhttp-in,hysteria-in` или оставить пустым для автоопределения по активным профилям)
- `KAFKA_TLS_ENABLED` — `true`/`false`; пусто — TLS включается сам, если задан любой путь ниже
- `KAFKA_TLS_CA_CERT` — PEM-сертификат CA брокера центра
- `KAFKA_TLS_CLIENT_CERT`, `KAFKA_TLS_CLIENT_KEY` — клиентский сертификат и ключ (mTLS)
- `KAFKA_TLS_SERVER_NAME` — имя сервера в сертификате брокера, если отличается от адреса в `KAFKA_BROKERS`
- `KAFKA_TLS_HOST_DIR` — каталог сертификатов на хосте для `docker-compose.node.yml`
  (по умолчанию `/etc/vpn-node-agent/kafka-tls`, монтируется в тот же путь контейнера)

Неверная настройка TLS — ошибка запуска агента, а не откат на соединение без
шифрования.
