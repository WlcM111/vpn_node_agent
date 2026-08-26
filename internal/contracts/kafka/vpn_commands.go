package kafka

import "time"

type VPNCommandType string

const (
	VPNCommandNodeSyncUser   VPNCommandType = "vpn.node_sync_user"
	VPNCommandNodeRevokeUser VPNCommandType = "vpn.node_revoke_user"
	VPNCommandNodePing       VPNCommandType = "vpn.node_ping"
)

type VPNNodeUserProfile struct {
	ItemKey     string     `json:"item_key"`
	CountryCode string     `json:"country_code"`
	Title       string     `json:"title"`
	ProfileType string     `json:"profile_type"`
	InboundTag  string     `json:"inbound_tag"`
	Email       string     `json:"email"`
	VLESSUUID   string     `json:"vless_uuid"`
	Flow        string     `json:"flow,omitempty"`
	Level       uint32     `json:"level"`
	AccessUntil *time.Time `json:"access_until,omitempty"`
	// Optional=true: профиль необязателен. Если на узле нет такого inbound
	// (например, CDN-inbound не настроен), node-agent пропускает его без
	// провала всей команды. Обязательные профили (основной доступ) при ошибке
	// валят команду, как и раньше.
	Optional bool `json:"optional,omitempty"`
}

type NodeSyncUserCommand struct {
	Type        VPNCommandType       `json:"type"`
	CommandID   string               `json:"command_id"`
	NodeID      string               `json:"node_id"`
	ServerKey   string               `json:"server_key"`
	TelegramID  int64                `json:"telegram_id"`
	AccessRev   int64                `json:"access_rev"`
	AccessUntil *time.Time           `json:"access_until,omitempty"`
	Profiles    []VPNNodeUserProfile `json:"profiles"`
	CreatedAt   time.Time            `json:"created_at"`
}

// RevokeScope — область действия команды отзыва.
//
// Пустое значение и RevokeScopeAll означают «пользователь теряет узел целиком»:
// агент объединяет присланный список со всеми известными ему профилями этого
// пользователя. Это поведение по умолчанию и оно не меняется.
//
// RevokeScopeListedProfiles отзывает РОВНО перечисленные профили и ничего
// больше. Нужен для частичных отключений — например, при исчерпании CDN-квоты
// снимается только CDN-учётка, а основной доступ продолжает работать.
type RevokeScope string

const (
	RevokeScopeAll            RevokeScope = ""
	RevokeScopeListedProfiles RevokeScope = "listed_profiles"
)

type NodeRevokeUserCommand struct {
	Type       VPNCommandType       `json:"type"`
	CommandID  string               `json:"command_id"`
	NodeID     string               `json:"node_id"`
	ServerKey  string               `json:"server_key"`
	TelegramID int64                `json:"telegram_id"`
	AccessRev  int64                `json:"access_rev"`
	Reason     string               `json:"reason,omitempty"`
	Scope      RevokeScope          `json:"scope,omitempty"`
	Profiles   []VPNNodeUserProfile `json:"profiles,omitempty"`
	CreatedAt  time.Time            `json:"created_at"`
}
