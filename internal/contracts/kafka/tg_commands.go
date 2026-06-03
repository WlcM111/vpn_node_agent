package kafka

type PlanCode string

const (
	PlanCodeMonthly   PlanCode = "monthly_30d"
	PlanCodeQuarterly PlanCode = "quarterly_90d"
)

type BillingCommandType string

const (
	BillingCommandCreateSubscriptionCheckout BillingCommandType = "billing.create_subscription_checkout"
	BillingCommandBindCard                   BillingCommandType = "billing.bind_card"
	BillingCommandUnbindCard                 BillingCommandType = "billing.unbind_card"
	BillingCommandDisableAutoRenew           BillingCommandType = "billing.disable_auto_renew"
)

type CreateSubscriptionCheckoutCommand struct {
	Type              BillingCommandType `json:"type"`
	CommandID         string             `json:"command_id"`
	TelegramID        int64              `json:"telegram_id"`
	PlanCode          PlanCode           `json:"plan_code"`
	SavePaymentMethod bool               `json:"save_payment_method,omitempty"`
	ReturnURL         string             `json:"return_url,omitempty"`
}

type BindCardCommand struct {
	Type       BillingCommandType `json:"type"`
	CommandID  string             `json:"command_id"`
	TelegramID int64              `json:"telegram_id"`
	ReturnURL  string             `json:"return_url,omitempty"`
}

type UnbindCardCommand struct {
	Type       BillingCommandType `json:"type"`
	CommandID  string             `json:"command_id"`
	TelegramID int64              `json:"telegram_id"`
}

type DisableAutoRenewCommand struct {
	Type       BillingCommandType `json:"type"`
	CommandID  string             `json:"command_id"`
	TelegramID int64              `json:"telegram_id"`
	Reason     string             `json:"reason,omitempty"`
}
