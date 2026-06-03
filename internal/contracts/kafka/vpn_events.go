package kafka

import "time"

type VPNEventType string

const (
	VPNEventNodeHeartbeat   VPNEventType = "vpn.node_heartbeat"
	VPNEventNodeUserSynced  VPNEventType = "vpn.node_user_synced"
	VPNEventNodeUserRevoked VPNEventType = "vpn.node_user_revoked"
	VPNEventNodeError       VPNEventType = "vpn.node_error"
)

type VPNNodeHeartbeatEvent struct {
	Type         VPNEventType `json:"type"`
	NodeID       string       `json:"node_id"`
	ServerKey    string       `json:"server_key"`
	Online       bool         `json:"online"`
	AppliedUsers int          `json:"applied_users"`
	XrayAPIAddr  string       `json:"xray_api_addr,omitempty"`
	AgentVersion string       `json:"agent_version,omitempty"`
	CreatedAt    time.Time    `json:"created_at"`
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
