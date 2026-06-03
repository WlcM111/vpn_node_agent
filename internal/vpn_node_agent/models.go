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
}

const currentStateVersion = 2

type AgentState struct {
	Version      int                       `json:"version"`
	NodeID       string                    `json:"node_id"`
	Profiles     map[string]AppliedProfile `json:"profiles"`
	SeenCommands map[string]time.Time      `json:"seen_commands,omitempty"`
	UpdatedAt    time.Time                 `json:"updated_at"`
}

func profileKey(inboundTag, email string) string {
	return inboundTag + "|" + email
}
