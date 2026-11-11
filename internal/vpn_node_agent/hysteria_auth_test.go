package vpn_node_agent

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	kafkacontracts "vpn_node_agent/internal/contracts/kafka"
)

func TestHysteriaAuthHandlerPrivacyAndRevocation(t *testing.T) {
	var logBuf bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prevLogger)

	agent, _, _ := newTestAgent(t, "cas-main-1")
	if err := agent.syncUser(context.Background(), cascadeSyncCommand(1)); err != nil {
		t.Fatalf("syncUser: %v", err)
	}

	handler := agent.HysteriaAuthHandler()
	clientIP := "203.0.113.42:54321"

	// 1. Успешная авторизация по UUID
	req := httptest.NewRequest(http.MethodPost, "/hysteria/auth",
		strings.NewReader(`{"addr":"`+clientIP+`","auth":"11111111-2222-3333-4444-555555555555","tx":100}`))
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ok":true`) {
		t.Fatalf("ожидался статус 200 и ok=true, получено %d: %s", rec.Code, rec.Body.String())
	}

	// 2. Отказ для неизвестного UUID
	reqBad := httptest.NewRequest(http.MethodPost, "/hysteria/auth",
		strings.NewReader(`{"addr":"`+clientIP+`","auth":"wrong-uuid","tx":101}`))
	recBad := httptest.NewRecorder()
	handler(recBad, reqBad)

	if recBad.Code != http.StatusOK || !strings.Contains(recBad.Body.String(), `"ok":false`) {
		t.Fatalf("ожидался статус 200 и ok=false, получено %d: %s", recBad.Code, recBad.Body.String())
	}

	if _, err := agent.revokeUser(context.Background(), &kafkacontracts.NodeRevokeUserCommand{
		NodeID: "cas-main-1", TelegramID: 555, AccessRev: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := agent.AuthorizeHysteria("11111111-2222-3333-4444-555555555555"); ok {
		t.Fatal("revoked user remains authorized")
	}

	// 3. Проверка конфиденциальности: клиентский IP не должен попадать в логи
	if strings.Contains(logBuf.String(), clientIP) || strings.Contains(logBuf.String(), "203.0.113.42") {
		t.Fatalf("утечка клиентского IP в логах аутентификации Hysteria2: %s", logBuf.String())
	}
}
