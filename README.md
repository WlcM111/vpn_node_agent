# vpn-node-agent

Агент VPN-ноды: слушает команды из Kafka центрального сервера, управляет
пользователями Xray через gRPC API, шлёт heartbeat и результаты обратно.

## Сборка

    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" \
      -o bin/vpn-node-agent ./cmd/vpn-node-agent

## Запуск

См. docker-compose.node.yml (Docker) или systemd-юнит (нативно).
Переменные окружения — в .env.node (шаблон ниже).

## Переменные окружения

- NODE_ID, SERVER_KEY — идентификаторы ноды
- KAFKA_BROKERS — адрес Kafka центрального сервера (race-src.com:19092)
- XRAY_APPLY_MODE — api | dry-run
- XRAY_API_ADDR — адрес gRPC Xray (127.0.0.1:10085)
- NODE_AGENT_STATE_PATH, NODE_AGENT_PENDING_PATH — пути файлов состояния
- NODE_AGENT_HEARTBEAT_INTERVAL, NODE_AGENT_APPLY_TIMEOUT
- NODE_AGENT_HTTP_ADDR — адрес healthz/metrics (:9091)
