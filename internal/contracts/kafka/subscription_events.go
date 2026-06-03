package kafka

import "time"

type SubscriptionEventType string

const (
	SubscriptionEventTrialStarted SubscriptionEventType = "subscription.trial_started"
	SubscriptionEventActivated    SubscriptionEventType = "subscription.activated"
	SubscriptionEventCanceled     SubscriptionEventType = "subscription.canceled"
	SubscriptionEventLinksIssued  SubscriptionEventType = "subscription.links_issued"
	SubscriptionEventGraceStarted SubscriptionEventType = "subscription.grace_started"
	SubscriptionEventSuspended    SubscriptionEventType = "subscription.suspended"
)

type SubscriptionAccessLink struct {
	Kind  string `json:"kind"`
	Title string `json:"title"`
	URL   string `json:"url"`
}

type SubscriptionTrialStartedEvent struct {
	Type       SubscriptionEventType `json:"type"`
	TelegramID int64                 `json:"telegram_id"`
	TrialUntil time.Time             `json:"trial_until"`
	DaysLeft   int                   `json:"days_left"`
	Country    string                `json:"country"`
	AccessRev  int64                 `json:"access_rev,omitempty"`
}

type SubscriptionActivatedEvent struct {
	Type             SubscriptionEventType `json:"type"`
	TelegramID       int64                 `json:"telegram_id"`
	PlanCode         string                `json:"plan_code"`
	ActivatedAt      time.Time             `json:"activated_at"`
	ActiveUntil      time.Time             `json:"active_until"`
	DaysLeft         int                   `json:"days_left"`
	Country          string                `json:"country"`
	Source           string                `json:"source"`
	PaymentID        string                `json:"payment_id,omitempty"`
	AutoRenewEnabled bool                  `json:"auto_renew_enabled"`
	AccessRev        int64                 `json:"access_rev,omitempty"`
}

type SubscriptionCanceledEvent struct {
	Type              SubscriptionEventType `json:"type"`
	TelegramID        int64                 `json:"telegram_id"`
	CanceledAt        time.Time             `json:"canceled_at"`
	AccessUntil       *time.Time            `json:"access_until,omitempty"`
	CancelAtPeriodEnd bool                  `json:"cancel_at_period_end"`
	AccessRev         int64                 `json:"access_rev,omitempty"`
}

type SubscriptionGraceStartedEvent struct {
	Type       SubscriptionEventType `json:"type"`
	TelegramID int64                 `json:"telegram_id"`
	GraceUntil time.Time             `json:"grace_until"`
	Reason     string                `json:"reason,omitempty"`
	AccessRev  int64                 `json:"access_rev,omitempty"`
}

type SubscriptionSuspendedEvent struct {
	Type        SubscriptionEventType `json:"type"`
	TelegramID  int64                 `json:"telegram_id"`
	SuspendedAt time.Time             `json:"suspended_at"`
	Reason      string                `json:"reason,omitempty"`
	AccessRev   int64                 `json:"access_rev,omitempty"`
}

type SubscriptionLinksIssuedEvent struct {
	Type       SubscriptionEventType    `json:"type"`
	TelegramID int64                    `json:"telegram_id"`
	ExpiresAt  time.Time                `json:"expires_at"`
	Links      []SubscriptionAccessLink `json:"links"`
}
