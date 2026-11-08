package vpn_node_agent

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	kafkacontracts "vpn_node_agent/internal/contracts/kafka"
)

// missingInboundXray — Xray, на котором инбаунда missingTag нет (как после
// удаления устаревшего WS-инбаунда из конфига узла).
type missingInboundXray struct {
	*recordingXray
	missingTag string
	mu2        sync.Mutex
	added      []string
}

func (x *missingInboundXray) EnsureVLESSUser(_ context.Context, inboundTag, email, _, _ string, _ uint32) error {
	if inboundTag == x.missingTag {
		return errors.New("rpc error: code = Unknown desc = app/proxyman/command: handler not found: " + inboundTag)
	}
	x.mu2.Lock()
	defer x.mu2.Unlock()
	x.added = append(x.added, profileKey(inboundTag, email))
	return nil
}

// После рестарта агента восстановление пользователей не обрывается на
// профиле, чей инбаунд убран с узла: остальные профили применяются.
func TestReconcileStateSkipsProfilesWithMissingInbound(t *testing.T) {
	dir := t.TempDir()
	repo := NewStateRepository(filepath.Join(dir, "state.json"))
	queue, err := NewPendingQueue(filepath.Join(dir, "pending.json"), nil)
	if err != nil {
		t.Fatal(err)
	}
	xray := &missingInboundXray{recordingXray: newRecordingXray(), missingTag: "vless-ws-tls"}
	agent, err := NewAgent(Config{NodeID: "ro-main-1", ServerKey: "ro-main-1", ApplyMode: "api", AgentVersion: "test"}, nil, nil, xray, repo, queue)
	if err != nil {
		t.Fatal(err)
	}

	until := time.Now().UTC().Add(24 * time.Hour)
	for i, tag := range []string{"vless-ws-tls", "vless-reality-in", "vless-ws-tls", "vless-xhttp-cdn-in"} {
		email := "tg-" + string(rune('a'+i)) + "@vpn-platform.local"
		agent.state.Profiles[profileKey(tag, email)] = AppliedProfile{
			TelegramID: int64(100 + i), AccessRev: 1, InboundTag: tag, Email: email,
			VLESSUUID:   "11111111-2222-3333-4444-55555555555" + string(rune('0'+i)),
			AccessUntil: &until, Enabled: true,
		}
	}

	if err := agent.reconcileState(context.Background()); err != nil {
		t.Fatalf("reconcileState: %v", err)
	}
	sort.Strings(xray.added)
	want := []string{"vless-reality-in|tg-b@vpn-platform.local", "vless-xhttp-cdn-in|tg-d@vpn-platform.local"}
	if len(xray.added) != len(want) || xray.added[0] != want[0] || xray.added[1] != want[1] {
		t.Fatalf("restored profiles = %v, want %v", xray.added, want)
	}

	// Профили убранного инбаунда выключены (и сохранены на диск), остальные
	// включены как были.
	reloaded, err := repo.Load("ro-main-1")
	if err != nil {
		t.Fatal(err)
	}
	for key, p := range reloaded.Profiles {
		wantEnabled := p.InboundTag != "vless-ws-tls"
		if p.Enabled != wantEnabled {
			t.Fatalf("%s: enabled = %v, want %v", key, p.Enabled, wantEnabled)
		}
	}
	if got := repo.CountEnabled(reloaded); got != 2 {
		t.Fatalf("enabled profiles = %d, want 2", got)
	}

	// Повторный запуск не трогает выключенные профили: в Xray уходят только
	// включённые.
	xray.added = nil
	if err := agent.reconcileState(context.Background()); err != nil {
		t.Fatalf("second reconcileState: %v", err)
	}
	sort.Strings(xray.added)
	if len(xray.added) != len(want) || xray.added[0] != want[0] || xray.added[1] != want[1] {
		t.Fatalf("second pass restored %v, want %v", xray.added, want)
	}
}

// Пустой NODE_AGENT_INBOUND_TAGS: трафик ноды суммируется по инбаундам
// включённых профилей, без встроенного списка тегов.
func TestNodeTrafficUsesInboundsOfEnabledProfiles(t *testing.T) {
	if got := inboundTagsFromEnv(""); len(got) != 0 {
		t.Fatalf("inboundTagsFromEnv(\"\") = %v, want empty", got)
	}
	if got := inboundTagsFromEnv(" a , ,b "); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("inboundTagsFromEnv = %v, want [a b]", got)
	}

	dir := t.TempDir()
	repo := NewStateRepository(filepath.Join(dir, "state.json"))
	queue, err := NewPendingQueue(filepath.Join(dir, "pending.json"), nil)
	if err != nil {
		t.Fatal(err)
	}
	xray := &inboundCountingXray{recordingXray: newRecordingXray()}
	agent, err := NewAgent(Config{NodeID: "n1", ServerKey: "n1", ApplyMode: "api", AgentVersion: "test"}, nil, nil, xray, repo, queue)
	if err != nil {
		t.Fatal(err)
	}
	agent.load = newLoadStats(inboundTagsFromEnv(""))
	for i, p := range []struct {
		tag     string
		enabled bool
	}{
		{"vless-reality-in", true},
		{"vless-xhttp-in", true},
		{"vless-reality-in", true},
		{"vless-ws-in", false},
	} {
		email := "u" + string(rune('0'+i)) + "@vpn-platform.local"
		agent.state.Profiles[profileKey(p.tag, email)] = AppliedProfile{InboundTag: p.tag, Email: email, Enabled: p.enabled}
	}

	agent.collectNodeTraffic(context.Background())
	sort.Strings(xray.queried)
	want := []string{"vless-reality-in", "vless-xhttp-in"}
	if len(xray.queried) != len(want) || xray.queried[0] != want[0] || xray.queried[1] != want[1] {
		t.Fatalf("queried inbounds = %v, want %v", xray.queried, want)
	}

	// Явно заданный список по-прежнему имеет приоритет.
	xray.queried = nil
	agent.load = newLoadStats(inboundTagsFromEnv("custom-in"))
	agent.collectNodeTraffic(context.Background())
	if len(xray.queried) != 1 || xray.queried[0] != "custom-in" {
		t.Fatalf("queried inbounds = %v, want [custom-in]", xray.queried)
	}
}

// inboundCountingXray запоминает, по каким инбаундам запрашивался трафик.
type inboundCountingXray struct {
	*recordingXray
	mu3     sync.Mutex
	queried []string
}

func (x *inboundCountingXray) QueryInboundTraffic(_ context.Context, tag string) (UserTraffic, error) {
	x.mu3.Lock()
	defer x.mu3.Unlock()
	x.queried = append(x.queried, tag)
	return UserTraffic{}, nil
}

// Команда, выпущенная до удаления инбаунда с узла (обычный профиль WS), не
// валится и не повторяется: профиль убранного инбаунда пропускается, остальные
// профили команды применяются и попадают в состояние.
func TestSyncUserSkipsProfilesOfRemovedInbound(t *testing.T) {
	dir := t.TempDir()
	repo := NewStateRepository(filepath.Join(dir, "state.json"))
	queue, err := NewPendingQueue(filepath.Join(dir, "pending.json"), nil)
	if err != nil {
		t.Fatal(err)
	}
	xray := &missingInboundXray{recordingXray: newRecordingXray(), missingTag: "vless-ws-in"}
	agent, err := NewAgent(Config{NodeID: "lt-main-1", ServerKey: "lt-main-1", ApplyMode: "api", AgentVersion: "test"}, nil, nil, xray, repo, queue)
	if err != nil {
		t.Fatal(err)
	}
	cmd := &kafkacontracts.NodeSyncUserCommand{
		Type:       kafkacontracts.VPNCommandNodeSyncUser,
		CommandID:  "stale-ws",
		NodeID:     "lt-main-1",
		ServerKey:  "lt-main-1",
		TelegramID: 105757774,
		AccessRev:  3,
		Profiles: []kafkacontracts.VPNNodeUserProfile{
			{ItemKey: "lt-main-1-ws", InboundTag: "vless-ws-in", Email: "tg-105757774-lt-main-1-ws@vpn-platform.local", VLESSUUID: "uuid-1"},
			{ItemKey: "lt-main-1-ws", InboundTag: "vless-reality-in", Email: "tg-105757774-lt-main-1-ws@vpn-platform.local", VLESSUUID: "uuid-1", Flow: "xtls-rprx-vision"},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := agent.syncUser(context.Background(), cmd); err != nil {
		t.Fatalf("syncUser: %v", err)
	}
	want := "vless-reality-in|tg-105757774-lt-main-1-ws@vpn-platform.local"
	if len(xray.added) != 1 || xray.added[0] != want {
		t.Fatalf("applied = %v, want [%s]", xray.added, want)
	}
	reloaded, err := repo.Load("lt-main-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.Profiles) != 1 {
		t.Fatalf("state profiles = %d, want 1 (только Reality)", len(reloaded.Profiles))
	}
	if p, ok := reloaded.Profiles[want]; !ok || !p.Enabled {
		t.Fatalf("Reality-профиль не сохранён включённым: %+v", reloaded.Profiles)
	}
}
