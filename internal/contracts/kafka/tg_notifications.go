package kafka

type TgKeyboard string

const (
	TgKeyboardMainMenu             TgKeyboard = "main_menu"
	TgKeyboardBuyMenu              TgKeyboard = "buy_menu"
	TgKeyboardTrialOrBuy           TgKeyboard = "trial_or_buy"
	TgKeyboardMySubscriptionConfig TgKeyboard = "my_subscription_with_config"
	TgKeyboardMainMenuWithBack     TgKeyboard = "main_menu_with_back"
)

type TgNotification struct {
	TelegramID int64      `json:"telegram_id"`
	Message    string     `json:"message"`
	ParseMode  string     `json:"parse_mode,omitempty"`
	Keyboard   TgKeyboard `json:"keyboard,omitempty"`
}
