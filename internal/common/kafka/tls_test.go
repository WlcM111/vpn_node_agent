package kafka

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// resetEnvTLS сбрасывает кэш настроек TLS процесса: тесты меняют окружение.
func resetEnvTLS(t *testing.T) {
	t.Helper()
	reset := func() {
		envTLS.mu.Lock()
		envTLS.loaded, envTLS.cfg, envTLS.err, envTLS.transport = false, nil, nil, nil
		envTLS.mu.Unlock()
	}
	reset()
	t.Cleanup(reset)
	for _, k := range []string{"KAFKA_TLS_ENABLED", "KAFKA_TLS_CA_CERT", "KAFKA_TLS_CLIENT_CERT", "KAFKA_TLS_CLIENT_KEY", "KAFKA_TLS_SERVER_NAME"} {
		t.Setenv(k, "")
	}
}

// writeTestPKI создаёт CA и подписанный им клиентский сертификат (PEM).
func writeTestPKI(t *testing.T) (caPath, certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "vpn-node-agent"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clDER, err := x509.CreateCertificate(rand.Reader, clTmpl, caTmpl, &clKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clKeyDER, err := x509.MarshalECPrivateKey(clKey)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	return write("ca.crt", "CERTIFICATE", caDER), write("client.crt", "CERTIFICATE", clDER), write("client.key", "EC PRIVATE KEY", clKeyDER)
}

func TestLoadTLSConfigFromEnv(t *testing.T) {
	caPath, certPath, keyPath := writeTestPKI(t)

	t.Run("не настроен — без TLS", func(t *testing.T) {
		resetEnvTLS(t)
		cfg, err := LoadTLSConfigFromEnv()
		if cfg != nil || err != nil {
			t.Fatalf("got cfg=%v err=%v, want nil nil", cfg, err)
		}
	})
	t.Run("пути включают TLS сами", func(t *testing.T) {
		resetEnvTLS(t)
		t.Setenv("KAFKA_TLS_CA_CERT", caPath)
		t.Setenv("KAFKA_TLS_CLIENT_CERT", certPath)
		t.Setenv("KAFKA_TLS_CLIENT_KEY", keyPath)
		t.Setenv("KAFKA_TLS_SERVER_NAME", "main.race-src.com")
		cfg, err := LoadTLSConfigFromEnv()
		if err != nil || cfg == nil {
			t.Fatalf("got cfg=%v err=%v", cfg, err)
		}
		if cfg.RootCAs == nil || len(cfg.Certificates) != 1 || cfg.ServerName != "main.race-src.com" || cfg.MinVersion != tls.VersionTLS12 {
			t.Fatalf("unexpected config: roots=%v certs=%d sni=%q min=%x", cfg.RootCAs != nil, len(cfg.Certificates), cfg.ServerName, cfg.MinVersion)
		}
	})
	t.Run("явный false выключает", func(t *testing.T) {
		resetEnvTLS(t)
		t.Setenv("KAFKA_TLS_ENABLED", "false")
		t.Setenv("KAFKA_TLS_CA_CERT", caPath)
		if cfg, err := LoadTLSConfigFromEnv(); cfg != nil || err != nil {
			t.Fatalf("got cfg=%v err=%v, want nil nil", cfg, err)
		}
	})
	for name, env := range map[string]map[string]string{
		"опечатка во флаге":            {"KAFKA_TLS_ENABLED": "on"},
		"сертификат без ключа":         {"KAFKA_TLS_CLIENT_CERT": certPath},
		"нет файла CA":                 {"KAFKA_TLS_CA_CERT": filepath.Join(t.TempDir(), "missing.crt")},
		"CA без сертификатов":          {"KAFKA_TLS_CA_CERT": keyPath},
		"ключ не от этого сертификата": {"KAFKA_TLS_CLIENT_CERT": caPath, "KAFKA_TLS_CLIENT_KEY": keyPath},
	} {
		t.Run(name, func(t *testing.T) {
			resetEnvTLS(t)
			for k, v := range env {
				t.Setenv(k, v)
			}
			if cfg, err := LoadTLSConfigFromEnv(); err == nil {
				t.Fatalf("want error, got cfg=%v", cfg)
			}
		})
	}
}

// Без TLS — те же объекты kafka-go, что и до появления TLS; транспорт общий
// для процесса и не создаётся заново на каждый вызов.
func TestTransportFromEnvWithoutTLSIsDefault(t *testing.T) {
	resetEnvTLS(t)
	if tr := TransportFromEnv(); tr != kafkago.DefaultTransport {
		t.Fatalf("TransportFromEnv() = %T, want kafkago.DefaultTransport", tr)
	}
	if d := DialerFromEnv(); d.TLS != nil {
		t.Fatal("DialerFromEnv().TLS must be nil without TLS settings")
	}
	if TLSEnabled() || TLSConfigError() != nil {
		t.Fatal("TLS must be reported as disabled without errors")
	}
}

func TestTransportFromEnvIsSharedWithTLS(t *testing.T) {
	resetEnvTLS(t)
	caPath, certPath, keyPath := writeTestPKI(t)
	t.Setenv("KAFKA_TLS_CA_CERT", caPath)
	t.Setenv("KAFKA_TLS_CLIENT_CERT", certPath)
	t.Setenv("KAFKA_TLS_CLIENT_KEY", keyPath)

	var wg sync.WaitGroup
	got := make([]kafkago.RoundTripper, 16)
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i] = TransportFromEnv()
		}(i)
	}
	wg.Wait()
	tr, ok := got[0].(*kafkago.Transport)
	if !ok || tr.TLS == nil || len(tr.TLS.Certificates) != 1 {
		t.Fatalf("want *kafkago.Transport with client certificate, got %T", got[0])
	}
	for i := range got {
		if got[i] != got[0] {
			t.Fatal("TransportFromEnv must return one shared transport per process")
		}
	}
	if !TLSEnabled() || TLSConfigError() != nil {
		t.Fatal("TLS must be reported as enabled without errors")
	}
	if d := DialerFromEnv(); d.TLS == nil || len(d.TLS.Certificates) != 1 {
		t.Fatal("DialerFromEnv must carry the client certificate")
	}
}

// Ошибка настройки не приводит к тихому подключению без шифрования: TLS
// включён, а каждое рукопожатие отклоняется с исходной ошибкой.
func TestBrokenTLSConfigFailsClosed(t *testing.T) {
	resetEnvTLS(t)
	t.Setenv("KAFKA_TLS_CA_CERT", filepath.Join(t.TempDir(), "missing.crt"))
	if TLSConfigError() == nil || !TLSEnabled() {
		t.Fatal("broken TLS settings must be reported")
	}
	d := DialerFromEnv()
	if d.TLS == nil || d.TLS.VerifyConnection == nil {
		t.Fatal("dialer must keep TLS on and reject connections")
	}
	if err := d.TLS.VerifyConnection(tls.ConnectionState{}); err == nil {
		t.Fatal("VerifyConnection must fail for broken TLS settings")
	}
	tr, ok := TransportFromEnv().(*kafkago.Transport)
	if !ok || tr.TLS == nil || tr.TLS.VerifyConnection == nil {
		t.Fatal("transport must keep TLS on and reject connections")
	}
}

// Close окончательный: Reset после него writer не воскрешает, публикация
// возвращает ErrDisabled. Reset и публикация безопасны конкурентно (-race).
func TestProducerResetAndClose(t *testing.T) {
	resetEnvTLS(t)
	p := NewProducer([]string{"127.0.0.1:1"})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			p.Reset()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
			_ = p.PublishJSON(ctx, "vpn.events", "k", map[string]string{"type": "test"})
			cancel()
		}
	}()
	wg.Wait()

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	p.Reset()
	if p.currentWriter() != nil {
		t.Fatal("Reset after Close must not create a new writer")
	}
	if err := p.PublishJSON(context.Background(), "vpn.events", "k", "x"); !errors.Is(err, ErrDisabled) {
		t.Fatalf("PublishJSON after Close = %v, want ErrDisabled", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
