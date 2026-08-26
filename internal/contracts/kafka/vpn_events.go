package kafka

import "time"

type VPNEventType string

const (
	VPNEventNodeHeartbeat   VPNEventType = "vpn.node_heartbeat"
	VPNEventNodeUserSynced  VPNEventType = "vpn.node_user_synced"
	VPNEventNodeUserRevoked VPNEventType = "vpn.node_user_revoked"
	VPNEventNodeError       VPNEventType = "vpn.node_error"
	VPNEventNodeTraffic     VPNEventType = "vpn.node_traffic"
)

type VPNNodeHeartbeatEvent struct {
	Type      VPNEventType `json:"type"`
	NodeID    string       `json:"node_id"`
	ServerKey string       `json:"server_key"`
	Online    bool         `json:"online"`

	// AppliedUsers — сколько учётных записей выдано на этой ноде.
	// Это ёмкостная величина, а не показатель нагрузки.
	AppliedUsers int `json:"applied_users"`

	// OnlineUsers — сколько пользователей реально передавали данные за
	// последний интервал сбора трафика. Именно это и есть нагрузка:
	// выданная учётка ничего не стоит, пока по ней не идёт трафик.
	OnlineUsers int `json:"online_users"`

	// UplinkBps/DownlinkBps — скорость трафика ноды, посчитанная как
	// разница суммарных счётчиков инбаундов между двумя heartbeat.
	// Нужна, чтобы аллокатор видел, что нода упирается в канал, даже
	// когда пользователей на ней немного.
	UplinkBps   int64 `json:"uplink_bps"`
	DownlinkBps int64 `json:"downlink_bps"`

	XrayAPIAddr  string    `json:"xray_api_addr,omitempty"`
	AgentVersion string    `json:"agent_version,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type VPNNodeUserSyncedEvent struct {
	Type       VPNEventType `json:"type"`
	CommandID  string       `json:"command_id"`
	NodeID     string       `json:"node_id"`
	ServerKey  string       `json:"server_key"`
	TelegramID int64        `json:"telegram_id"`
	AccessRev  int64        `json:"access_rev"`
	Profiles   int          `json:"profiles"`
	Success    bool         `json:"success"`
	Error      string       `json:"error,omitempty"`
	CreatedAt  time.Time    `json:"created_at"`
}

type VPNNodeUserRevokedEvent struct {
	Type       VPNEventType `json:"type"`
	CommandID  string       `json:"command_id"`
	NodeID     string       `json:"node_id"`
	ServerKey  string       `json:"server_key"`
	TelegramID int64        `json:"telegram_id"`
	AccessRev  int64        `json:"access_rev"`
	Profiles   int          `json:"profiles"`
	Success    bool         `json:"success"`
	Error      string       `json:"error,omitempty"`
	CreatedAt  time.Time    `json:"created_at"`
}

type VPNNodeErrorEvent struct {
	Type      VPNEventType `json:"type"`
	CommandID string       `json:"command_id,omitempty"`
	NodeID    string       `json:"node_id"`
	ServerKey string       `json:"server_key"`
	Error     string       `json:"error"`
	CreatedAt time.Time    `json:"created_at"`
}

// VPNNodeTrafficItem — кумулятивный трафик одной УЧЁТНОЙ ЗАПИСИ на узле (байты),
// как их отдаёт Xray Stats API (с момента старта Xray, без сброса).
//
// Ключ измерения — email: Xray ведёт счётчик user>>><email>>>>traffic>>>*, то
// есть по учётке, а не по инбаунду. Поэтому разделить CDN и не-CDN трафик можно
// только раздав им РАЗНЫЕ email (см. cdnEmail в оркестраторе).
type VPNNodeTrafficItem struct {
	TelegramID int64  `json:"telegram_id"`
	Email      string `json:"email"`
	Uplink     int64  `json:"uplink"`
	Downlink   int64  `json:"downlink"`

	// InboundTag — инбаунд, которому принадлежит эта учётка на узле.
	// Заполняется агентом из его собственного состояния (state.Profiles), а не
	// из данных клиента. Пустое значение = агент старой версии: центр обязан
	// считать такой трафик НЕклассифицированным и не относить его к CDN-квоте.
	InboundTag string `json:"inbound_tag,omitempty"`
}

// VPNNodeTrafficEvent — пачка трафика по всем активным пользователям узла за один
// проход сборщика. Публикуется node-agent периодически в vpn.events.
type VPNNodeTrafficEvent struct {
	Type      VPNEventType         `json:"type"`
	NodeID    string               `json:"node_id"`
	ServerKey string               `json:"server_key"`
	Items     []VPNNodeTrafficItem `json:"items"`
	CreatedAt time.Time            `json:"created_at"`
}
