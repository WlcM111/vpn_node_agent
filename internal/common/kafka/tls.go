package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// LoadTLSConfigFromEnv собирает *tls.Config для подключения к Kafka по TLS/mTLS.
//
// Переменные окружения:
//
//	KAFKA_TLS_ENABLED     — true/1/yes включает TLS, false/0/no выключает явно;
//	                        пусто — TLS включается сам, если задан любой из путей ниже
//	KAFKA_TLS_CA_CERT     — путь к PEM-сертификату корневого CA брокера
//	KAFKA_TLS_CLIENT_CERT — путь к PEM-сертификату клиента (для mTLS)
//	KAFKA_TLS_CLIENT_KEY  — путь к приватному ключу клиента (для mTLS)
//	KAFKA_TLS_SERVER_NAME — ожидаемое имя сервера в сертификате (SNI), опционально
//
// (nil, nil) — TLS не настроен: подключение без шифрования, как раньше.
func LoadTLSConfigFromEnv() (*tls.Config, error) {
	enabledRaw := strings.ToLower(strings.TrimSpace(os.Getenv("KAFKA_TLS_ENABLED")))
	caPath := strings.TrimSpace(os.Getenv("KAFKA_TLS_CA_CERT"))
	certPath := strings.TrimSpace(os.Getenv("KAFKA_TLS_CLIENT_CERT"))
	keyPath := strings.TrimSpace(os.Getenv("KAFKA_TLS_CLIENT_KEY"))
	serverName := strings.TrimSpace(os.Getenv("KAFKA_TLS_SERVER_NAME"))

	var enabled bool
	switch enabledRaw {
	case "true", "1", "yes":
		enabled = true
	case "false", "0", "no":
		return nil, nil
	case "":
		enabled = caPath != "" || certPath != "" || keyPath != ""
	default:
		// Опечатка не должна молча оставлять соединение без шифрования.
		return nil, fmt.Errorf("KAFKA_TLS_ENABLED=%q: expected true/false", enabledRaw)
	}
	if !enabled {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
	}

	if caPath != "" {
		caPEM, err := os.ReadFile(filepath.Clean(caPath))
		if err != nil {
			return nil, fmt.Errorf("read KAFKA_TLS_CA_CERT (%s): %w", caPath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("parse KAFKA_TLS_CA_CERT (%s): no valid certificates found", caPath)
		}
		cfg.RootCAs = pool
	}

	if certPath != "" || keyPath != "" {
		if certPath == "" || keyPath == "" {
			return nil, errors.New("both KAFKA_TLS_CLIENT_CERT and KAFKA_TLS_CLIENT_KEY must be set together")
		}
		cert, err := tls.LoadX509KeyPair(filepath.Clean(certPath), filepath.Clean(keyPath))
		if err != nil {
			return nil, fmt.Errorf("load kafka client keypair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

// envTLS — настройки TLS процесса. Окружение читается один раз: все Reader'ы,
// Writer'ы и клиенты процесса получают одну и ту же конфигурацию, а файлы
// сертификатов не перечитываются на каждое подключение.
var envTLS struct {
	mu        sync.Mutex
	loaded    bool
	cfg       *tls.Config
	err       error
	transport kafkago.RoundTripper
}

func loadEnvTLSLocked() {
	if envTLS.loaded {
		return
	}
	envTLS.loaded = true
	envTLS.cfg, envTLS.err = LoadTLSConfigFromEnv()
	switch {
	case envTLS.err != nil:
		slog.Error("kafka tls: invalid configuration, connections to Kafka will be refused until it is fixed", "err", envTLS.err)
	case envTLS.cfg != nil:
		slog.Info("kafka tls: enabled",
			"ca", os.Getenv("KAFKA_TLS_CA_CERT") != "",
			"client_cert", len(envTLS.cfg.Certificates) > 0,
			"server_name", envTLS.cfg.ServerName)
	}
}

// TLSConfigError — ошибка настройки TLS из окружения; nil — TLS не настроен
// или настроен корректно. Сервис, которому шифрование обязательно, проверяет
// её при старте и завершается с понятной ошибкой.
func TLSConfigError() error {
	envTLS.mu.Lock()
	defer envTLS.mu.Unlock()
	loadEnvTLSLocked()
	return envTLS.err
}

// TLSEnabled сообщает, что TLS для Kafka включён в окружении.
func TLSEnabled() bool {
	envTLS.mu.Lock()
	defer envTLS.mu.Unlock()
	loadEnvTLSLocked()
	return envTLS.cfg != nil || envTLS.err != nil
}

// clientTLSLocked — конфигурация для клиентов Kafka. При ошибке настройки
// подключения отклоняются (fail-closed): тихий откат на соединение без
// шифрования отправил бы данные открытым текстом туда, где ждали TLS.
func clientTLSLocked() *tls.Config {
	loadEnvTLSLocked()
	if envTLS.err != nil {
		cause := envTLS.err
		return &tls.Config{
			MinVersion: tls.VersionTLS12,
			VerifyConnection: func(tls.ConnectionState) error {
				return fmt.Errorf("kafka tls misconfigured: %w", cause)
			},
		}
	}
	return envTLS.cfg
}

// DialerFromEnv возвращает *kafkago.Dialer для Reader'а. Без TLS — те же
// параметры, что у kafkago.DefaultDialer.
func DialerFromEnv() *kafkago.Dialer {
	envTLS.mu.Lock()
	defer envTLS.mu.Unlock()
	return &kafkago.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       clientTLSLocked(),
	}
}

// TransportFromEnv возвращает общий для процесса транспорт Writer'ов и
// kafkago.Client. Без TLS это kafkago.DefaultTransport — поведение как до
// появления TLS.
//
// Транспорт создаётся ОДИН раз: каждый kafkago.Transport держит собственный
// пул соединений с фоновым обновлением метаданных, который освобождается
// только CloseIdleConnections. Новый транспорт на каждый вызов (сбор лага
// каждые 15 секунд, пересоздание writer'а) копил бы соединения с Kafka и
// горутины без ограничения.
func TransportFromEnv() kafkago.RoundTripper {
	envTLS.mu.Lock()
	defer envTLS.mu.Unlock()
	if envTLS.transport != nil {
		return envTLS.transport
	}
	tlsCfg := clientTLSLocked()
	if tlsCfg == nil {
		envTLS.transport = kafkago.DefaultTransport
	} else {
		envTLS.transport = &kafkago.Transport{
			DialTimeout: 10 * time.Second,
			TLS:         tlsCfg,
		}
	}
	return envTLS.transport
}
