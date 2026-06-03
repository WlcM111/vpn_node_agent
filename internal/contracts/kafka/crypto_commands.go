package kafka

// CryptoCommandType — типизированные имена команд для crypto.commands.
// Сейчас только одна команда; новые добавлять как новые константы.
type CryptoCommandType string

const (
	CryptoCommandCreateCheckout CryptoCommandType = "crypto.create_checkout"
)

// CryptoAsset — поддерживаемые криптовалюты для оплаты.
// Названия совпадают с тем, что принимает CryptoBot Pay API в поле asset.
type CryptoAsset string

const (
	CryptoAssetUSDT CryptoAsset = "USDT"
	CryptoAssetTON  CryptoAsset = "TON"
	CryptoAssetBTC  CryptoAsset = "BTC"
	CryptoAssetETH  CryptoAsset = "ETH"
)

// CreateCryptoCheckoutCommand — команда от tg-bot-gateway к crypto-billing-service.
// Шлётся в топик crypto.commands с key=telegram_id для предсказуемой партиционности.
type CreateCryptoCheckoutCommand struct {
	Type       CryptoCommandType `json:"type"`
	CommandID  string            `json:"command_id"`
	TelegramID int64             `json:"telegram_id"`
	PlanCode   PlanCode          `json:"plan_code"`
	// Asset — необязателен. Если пусто, сервис подставит CRYPTOBOT_DEFAULT_ASSET.
	Asset CryptoAsset `json:"asset,omitempty"`
}
