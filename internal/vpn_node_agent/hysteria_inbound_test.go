package vpn_node_agent

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	kafkacontracts "vpn_node_agent/internal/contracts/kafka"
)

// ============================================================================
// Hysteria2 внутри Xray (нода-вход каскада): профиль с protocol=hysteria
// регистрируется в Hysteria2-инбаунде через EnsureHysteriaUser (пароль —
// VLESS-UUID), остальные — как раньше через EnsureVLESSUser. После рестарта
// агента протокол берётся из состояния.
// ============================================================================

func (x *recordingXray) EnsureHysteriaUser(_ context.Context, inboundTag, email, _ string, _ uint32) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.removed = append(x.removed, profileKey(inboundTag, email))
	x.ensured = append(x.ensured, "hysteria:"+profileKey(inboundTag, email))
	return nil
}

func cascadeSyncCommand(rev int64) *kafkacontracts.NodeSyncUserCommand {
	const (
		email = "tg-555-cas-main@vpn-platform.local"
		uuid  = "11111111-2222-3333-4444-555555555555"
	)
	return &kafkacontracts.NodeSyncUserCommand{
		CommandID: "c1", NodeID: "cas-main-1", ServerKey: "cas-main-1", TelegramID: 555, AccessRev: rev,
		Profiles: []kafkacontracts.VPNNodeUserProfile{
			{ItemKey: "cas-main", InboundTag: "vless-reality-in", Email: email, VLESSUUID: uuid, Flow: "xtls-rprx-vision"},
			{ItemKey: "cas-main", InboundTag: "vless-xhttp-in", Email: email, VLESSUUID: uuid, Optional: true},
			{ItemKey: "cas-main", InboundTag: "hysteria-in", Email: email, VLESSUUID: uuid, Optional: true,
				Protocol: kafkacontracts.ProfileProtocolHysteria},
		},
	}
}

func TestSyncUserRegistersHysteriaInbound(t *testing.T) {
	agent, xray, _ := newTestAgent(t, "cas-main-1")
	if err := agent.syncUser(context.Background(), cascadeSyncCommand(1)); err != nil {
		t.Fatalf("syncUser: %v", err)
	}
	_, ensured := xray.snapshot()
	want := []string{
		"vless-reality-in|tg-555-cas-main@vpn-platform.local",
		"vless-xhttp-in|tg-555-cas-main@vpn-platform.local",
		"hysteria:hysteria-in|tg-555-cas-main@vpn-platform.local",
	}
	if len(ensured) != len(want) {
		t.Fatalf("вызовы %v, ожидалось %v", ensured, want)
	}
	for i := range want {
		if ensured[i] != want[i] {
			t.Errorf("вызов %d = %q, ожидалось %q", i, ensured[i], want[i])
		}
	}
	p := agent.state.Profiles[profileKey("hysteria-in", "tg-555-cas-main@vpn-platform.local")]
	if p.Protocol != kafkacontracts.ProfileProtocolHysteria || !p.Enabled {
		t.Fatalf("профиль Hysteria2 в состоянии: %+v", p)
	}
}

// После рестарта агента учётка возвращается в Hysteria2-инбаунд тем же
// протоколом — он сохранён в state.json.
func TestReconcileRestoresHysteriaProtocolFromState(t *testing.T) {
	dir := t.TempDir()
	repo := NewStateRepository(filepath.Join(dir, "state.json"))
	queue, err := NewPendingQueue(filepath.Join(dir, "pending.json"), nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewAgent(Config{NodeID: "cas-main-1", ServerKey: "cas-main-1", ApplyMode: "api", AgentVersion: "test"}, nil, nil, newRecordingXray(), repo, queue)
	if err != nil {
		t.Fatal(err)
	}
	cmd := cascadeSyncCommand(1)
	until := time.Now().UTC().Add(time.Hour)
	for i := range cmd.Profiles {
		cmd.Profiles[i].AccessUntil = &until
	}
	if err := first.syncUser(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}

	xray := newRecordingXray()
	second, err := NewAgent(Config{NodeID: "cas-main-1", ServerKey: "cas-main-1", ApplyMode: "api", AgentVersion: "test"}, nil, nil, xray, repo, queue)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.reconcileState(context.Background()); err != nil {
		t.Fatalf("reconcileState: %v", err)
	}
	_, ensured := xray.snapshot()
	found := false
	for _, e := range ensured {
		if e == "hysteria:hysteria-in|tg-555-cas-main@vpn-platform.local" {
			found = true
		}
		if e == "hysteria-in|tg-555-cas-main@vpn-platform.local" {
			t.Fatalf("Hysteria2-учётка восстановлена как VLESS: %v", ensured)
		}
	}
	if !found {
		t.Fatalf("Hysteria2-учётка не восстановлена: %v", ensured)
	}
}

// Неизвестный протокол: опциональный профиль пропускается, обязательный
// валит команду.
func TestSyncUserUnsupportedProtocol(t *testing.T) {
	agent, xray, _ := newTestAgent(t, "cas-main-1")
	cmd := cascadeSyncCommand(1)
	cmd.Profiles[2].Protocol = "wireguard"
	if err := agent.syncUser(context.Background(), cmd); err != nil {
		t.Fatalf("опциональный профиль неизвестного протокола провалил команду: %v", err)
	}
	if _, ok := agent.state.Profiles[profileKey("hysteria-in", "tg-555-cas-main@vpn-platform.local")]; ok {
		t.Fatal("профиль неизвестного протокола попал в состояние")
	}
	if _, ensured := xray.snapshot(); len(ensured) != 2 {
		t.Fatalf("вызовы %v, ожидалось 2 (Reality и XHTTP)", ensured)
	}

	agent2, _, _ := newTestAgent(t, "cas-main-1")
	cmd2 := cascadeSyncCommand(1)
	cmd2.Profiles[0].Protocol = "wireguard"
	if err := agent2.syncUser(context.Background(), cmd2); err == nil {
		t.Fatal("обязательный профиль неизвестного протокола не провалил команду")
	}
}

// Отзыв снимает и Hysteria2-учётку: удаление в Xray идёт по email для любого
// протокола.
func TestRevokeRemovesHysteriaProfile(t *testing.T) {
	agent, xray, _ := newTestAgent(t, "cas-main-1")
	if err := agent.syncUser(context.Background(), cascadeSyncCommand(1)); err != nil {
		t.Fatal(err)
	}
	xray.reset()
	n, err := agent.revokeUser(context.Background(), &kafkacontracts.NodeRevokeUserCommand{
		CommandID: "r1", NodeID: "cas-main-1", TelegramID: 555, AccessRev: 2,
	})
	if err != nil || n != 3 {
		t.Fatalf("revokeUser: n=%d err=%v", n, err)
	}
	removed, _ := xray.snapshot()
	hit := false
	for _, r := range removed {
		if r == "hysteria-in|tg-555-cas-main@vpn-platform.local" {
			hit = true
		}
	}
	if !hit {
		t.Fatalf("Hysteria2-учётка не снята: %v", removed)
	}
	if p := agent.state.Profiles[profileKey("hysteria-in", "tg-555-cas-main@vpn-platform.local")]; p.Enabled {
		t.Fatal("профиль Hysteria2 остался включённым")
	}
}
