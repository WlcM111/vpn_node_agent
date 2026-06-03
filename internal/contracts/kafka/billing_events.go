package kafka

import "time"

type BillingCheckoutType string

const (
	BillingCheckoutTypeSubscription BillingCheckoutType = "subscription_purchase"
	BillingCheckoutTypeBindCard     BillingCheckoutType = "bind_card"
)

type BillingChargeSource string

const (
	BillingChargeSourceInitial   BillingChargeSource = "initial"
	BillingChargeSourceRecurring BillingChargeSource = "recurring"
)

type BillingEventType string

const (
	BillingEventPaymentSucceeded      BillingEventType = "billing.payment_succeeded"
	BillingEventPaymentCanceled       BillingEventType = "billing.payment_canceled"
	BillingEventPaymentMethodBound    BillingEventType = "billing.payment_method_bound"
	BillingEventPaymentMethodGone     BillingEventType = "billing.payment_method_unbound"
	BillingEventRenewalRetryScheduled BillingEventType = "billing.renewal_retry_scheduled"
	BillingEventGraceStarted          BillingEventType = "billing.grace_started"
	BillingEventAccessExpired         BillingEventType = "billing.access_expired"
	BillingEventAutoRenewDisabled     BillingEventType = "billing.auto_renew_disabled"
)

type BillingPaymentSucceededEvent struct {
	Type             BillingEventType    `json:"type"`
	CheckoutType     BillingCheckoutType `json:"checkout_type"`
	ChargeSource     BillingChargeSource `json:"charge_source"`
	TelegramID       int64               `json:"telegram_id"`
	OrderID          string              `json:"order_id"`
	PaymentID        string              `json:"payment_id"`
	PlanCode         PlanCode            `json:"plan_code,omitempty"`
	DurationDays     int                 `json:"duration_days,omitempty"`
	AmountValue      string              `json:"amount_value"`
	Currency         string              `json:"currency"`
	PaymentMethodID  string              `json:"payment_method_id,omitempty"`
	AttemptNo        int                 `json:"attempt_no,omitempty"`
	AutoRenewEnabled bool                `json:"auto_renew_enabled"`
	PaidAt           time.Time           `json:"paid_at"`
	PaymentProvider  string              `json:"payment_provider,omitempty"`
}

type BillingPaymentCanceledEvent struct {
	Type            BillingEventType    `json:"type"`
	CheckoutType    BillingCheckoutType `json:"checkout_type"`
	ChargeSource    BillingChargeSource `json:"charge_source"`
	TelegramID      int64               `json:"telegram_id"`
	OrderID         string              `json:"order_id"`
	PaymentID       string              `json:"payment_id"`
	Reason          string              `json:"reason,omitempty"`
	AttemptNo       int                 `json:"attempt_no,omitempty"`
	NextRetryAt     *time.Time          `json:"next_retry_at,omitempty"`
	GraceUntil      *time.Time          `json:"grace_until,omitempty"`
	CanceledAt      time.Time           `json:"canceled_at"`
	PaymentProvider string              `json:"payment_provider,omitempty"`
}

type BillingPaymentMethodBoundEvent struct {
	Type            BillingEventType `json:"type"`
	TelegramID      int64            `json:"telegram_id"`
	OrderID         string           `json:"order_id"`
	PaymentID       string           `json:"payment_id"`
	PaymentMethodID string           `json:"payment_method_id"`
	Last4           string           `json:"last4,omitempty"`
	BoundAt         time.Time        `json:"bound_at"`
}

type BillingPaymentMethodUnboundEvent struct {
	Type       BillingEventType `json:"type"`
	TelegramID int64            `json:"telegram_id"`
	UnboundAt  time.Time        `json:"unbound_at"`
}

type BillingRenewalRetryScheduledEvent struct {
	Type        BillingEventType `json:"type"`
	TelegramID  int64            `json:"telegram_id"`
	PlanCode    PlanCode         `json:"plan_code"`
	PaymentID   string           `json:"payment_id,omitempty"`
	Reason      string           `json:"reason,omitempty"`
	AttemptNo   int              `json:"attempt_no"`
	NextRetryAt time.Time        `json:"next_retry_at"`
	CreatedAt   time.Time        `json:"created_at"`
}

type BillingGraceStartedEvent struct {
	Type       BillingEventType `json:"type"`
	TelegramID int64            `json:"telegram_id"`
	PlanCode   PlanCode         `json:"plan_code"`
	PaymentID  string           `json:"payment_id,omitempty"`
	Reason     string           `json:"reason,omitempty"`
	GraceUntil time.Time        `json:"grace_until"`
	StartedAt  time.Time        `json:"started_at"`
}

type BillingAccessExpiredEvent struct {
	Type       BillingEventType `json:"type"`
	TelegramID int64            `json:"telegram_id"`
	Reason     string           `json:"reason,omitempty"`
	ExpiredAt  time.Time        `json:"expired_at"`
}

type BillingAutoRenewDisabledEvent struct {
	Type       BillingEventType `json:"type"`
	TelegramID int64            `json:"telegram_id"`
	Reason     string           `json:"reason,omitempty"`
	DisabledAt time.Time        `json:"disabled_at"`
}

type BillingPaymentProvider string

const (
	BillingPaymentProviderYooKassa  BillingPaymentProvider = "yookassa"
	BillingPaymentProviderCryptoBot BillingPaymentProvider = "cryptobot"
)
