package kafka

type SubscriptionCommandType string

const (
	SubscriptionCommandStartTrial SubscriptionCommandType = "subscription.start_trial"
	SubscriptionCommandGetStatus  SubscriptionCommandType = "subscription.get_status"
	SubscriptionCommandGetLinks   SubscriptionCommandType = "subscription.get_links"
	SubscriptionCommandCancel     SubscriptionCommandType = "subscription.cancel"
)

type StartTrialCommand struct {
	Type       SubscriptionCommandType `json:"type"`
	CommandID  string                  `json:"command_id"`
	TelegramID int64                   `json:"telegram_id"`
}

type GetStatusCommand struct {
	Type       SubscriptionCommandType `json:"type"`
	CommandID  string                  `json:"command_id"`
	TelegramID int64                   `json:"telegram_id"`
}

type GetLinksCommand struct {
	Type       SubscriptionCommandType `json:"type"`
	CommandID  string                  `json:"command_id"`
	TelegramID int64                   `json:"telegram_id"`
}

type CancelSubscriptionCommand struct {
	Type       SubscriptionCommandType `json:"type"`
	CommandID  string                  `json:"command_id"`
	TelegramID int64                   `json:"telegram_id"`
}
