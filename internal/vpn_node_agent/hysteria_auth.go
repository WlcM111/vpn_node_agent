package vpn_node_agent

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
)

// ============================================================================
// HTTP-аутентификация Hysteria2.
//
// Hysteria при каждом подключении клиента шлёт POST на этот эндпоинт:
//   {"addr": "1.2.3.4:44556", "auth": "<payload>", "tx": 123456}
// и ожидает в ответ:
//   {"ok": true, "id": "<идентификатор клиента>"}
// HTTP-статус ОБЯЗАН быть 200 — иначе аутентификация считается неуспешной,
// даже если в теле ok=true.
//
// Паролем выступает VLESS-UUID пользователя: он уже синхронизирован центром
// в состояние агента, поэтому отдельных учёток для Hysteria не требуется.
// Отзыв доступа работает автоматически — центр выключает профиль, и UUID
// перестаёт проходить проверку.
//
// Приватность: клиентский IP-адрес (req.Addr) намеренно НЕ записывается в логи,
// Этот обработчик не пишет IP, UUID или идентификатор пользователя.
// Настройки журналов Xray, nginx и Hysteria проверяются отдельно.
// ============================================================================

type hysteriaAuthRequest struct {
	Addr string `json:"addr"`
	Auth string `json:"auth"`
	TX   int64  `json:"tx"`
}

type hysteriaAuthResponse struct {
	OK bool   `json:"ok"`
	ID string `json:"id,omitempty"`
}

// AuthorizeHysteria ищет включённый профиль с указанным UUID.
// Возвращает идентификатор клиента (email) для статистики Hysteria.
func (a *Agent) AuthorizeHysteria(authPayload string) (string, bool) {
	uuid := strings.TrimSpace(authPayload)
	if uuid == "" {
		return "", false
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, p := range a.state.Profiles {
		if !p.Enabled {
			continue
		}
		if strings.EqualFold(p.VLESSUUID, uuid) {
			id := p.Email
			if id == "" {
				id = uuid
			}
			return id, true
		}
	}
	return "", false
}

// HysteriaAuthHandler — HTTP-обработчик для auth.type=http в конфиге Hysteria.
func (a *Agent) HysteriaAuthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req hysteriaAuthRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192)).Decode(&req); err != nil {
			// Статус 200 с ok=false: для Hysteria это корректный отказ.
			writeHysteriaAuth(w, hysteriaAuthResponse{OK: false})
			return
		}

		id, ok := a.AuthorizeHysteria(req.Auth)
		if !ok {
			slog.Debug("hysteria auth rejected")
			writeHysteriaAuth(w, hysteriaAuthResponse{OK: false})
			return
		}

		slog.Debug("hysteria auth accepted")
		writeHysteriaAuth(w, hysteriaAuthResponse{OK: true, ID: id})
	}
}

// writeHysteriaAuth всегда отвечает статусом 200 — этого требует протокол
// Hysteria: решение передаётся полем ok, а не HTTP-кодом.
func writeHysteriaAuth(w http.ResponseWriter, resp hysteriaAuthResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
