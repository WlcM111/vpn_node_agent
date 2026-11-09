package vpn_node_agent

import "time"

type AppliedProfile struct {
	TelegramID  int64      `json:"telegram_id"`
	AccessRev   int64      `json:"access_rev"`
	ItemKey     string     `json:"item_key"`
	InboundTag  string     `json:"inbound_tag"`
	Email       string     `json:"email"`
	VLESSUUID   string     `json:"vless_uuid"`
	Flow        string     `json:"flow,omitempty"`
	Level       uint32     `json:"level"`
	AccessUntil *time.Time `json:"access_until,omitempty"`
	Enabled     bool       `json:"enabled"`
	UpdatedAt   time.Time  `json:"updated_at"`
	// Protocol — протокол инбаунда (пусто — VLESS, "hysteria" — Hysteria2
	// внутри Xray). Нужен, чтобы после рестарта агента вернуть учётку в
	// инбаунд тем же типом. Старые state.json читаются как VLESS.
	Protocol string `json:"protocol,omitempty"`
}

// TrafficCounter — накопленный трафик одного пользователя на ЭТОМ узле.
//
// Xray отдаёт счётчик, накопленный с момента СВОЕГО старта, и обнуляет его при
// перезапуске. Если отдавать это значение как есть, центр видит откат счётчика
// и «замирает» на прежнем максимуме. Поэтому агент хранит две части:
//
//	Base — сумма всех предыдущих «жизней» Xray;
//	Last — последнее показание текущей.
//
// Наружу уходит Base+Last, то есть монотонно неубывающая величина.
type TrafficCounter struct {
	BaseUplink   int64     `json:"base_uplink"`
	BaseDownlink int64     `json:"base_downlink"`
	LastUplink   int64     `json:"last_uplink"`
	LastDownlink int64     `json:"last_downlink"`
	UpdatedAt    time.Time `json:"updated_at"`
}

const currentStateVersion = 2

type AgentState struct {
	Version      int                       `json:"version"`
	NodeID       string                    `json:"node_id"`
	Profiles     map[string]AppliedProfile `json:"profiles"`
	SeenCommands map[string]time.Time      `json:"seen_commands,omitempty"`
	// Traffic — счётчики трафика по email пользователя. Поле опциональное:
	// версия состояния не меняется, старые state.json читаются как есть
	// (карта просто инициализируется пустой).
	Traffic   map[string]TrafficCounter `json:"traffic,omitempty"`
	UpdatedAt time.Time                 `json:"updated_at"`
}

func profileKey(inboundTag, email string) string {
	return inboundTag + "|" + email
}
