package vpn_node_agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	kafkacontracts "vpn_node_agent/internal/contracts/kafka"
)

// ============================================================================
// ИЗОЛЯЦИЯ ОТЗЫВА CDN.
//
// Инвариант, который здесь доказывается исполняемо:
//
//	исчерпана CDN-квота пользователя на ноде N
//	  → отключается ИСКЛЮЧИТЕЛЬНО CDN-учётка этого пользователя на ноде N.
//
// Проверяется на реальном коде агента: настоящие Agent.revokeUser и
// Agent.syncUser, настоящий StateRepository (файл во временном каталоге),
// подменён только XrayController — чтобы записать ФАКТИЧЕСКИЕ вызовы к Xray
// с точным (inbound_tag, email) у каждого.
// ============================================================================

// recordingXray фиксирует все обращения к Xray. Именно по этим записям
// проверяется область действия операции, а не по намерению кода.
type recordingXray struct {
	mu      sync.Mutex
	removed []string // "inbound|email"
	ensured []string // "inbound|email"
	traffic map[string]UserTraffic
}

func newRecordingXray() *recordingXray {
	return &recordingXray{traffic: map[string]UserTraffic{}}
}

func (x *recordingXray) EnsureVLESSUser(_ context.Context, inboundTag, email, _, _ string, _ uint32) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	// Реальный APIXrayController делает Remove перед Add в пределах ТОГО ЖЕ
	// inbound (xray_controller.go: EnsureVLESSUser). Воспроизводим это, чтобы
	// тест ловил утечку области действия и на пути синхронизации тоже.
	x.removed = append(x.removed, profileKey(inboundTag, email))
	x.ensured = append(x.ensured, profileKey(inboundTag, email))
	return nil
}

func (x *recordingXray) RemoveVLESSUser(_ context.Context, inboundTag, email string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.removed = append(x.removed, profileKey(inboundTag, email))
	return nil
}

func (x *recordingXray) QueryUserTraffic(_ context.Context, email string) (UserTraffic, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.traffic[email], nil
}

func (x *recordingXray) QueryInboundTraffic(_ context.Context, _ string) (UserTraffic, error) {
	return UserTraffic{}, nil
}

func (x *recordingXray) Close() error { return nil }

func (x *recordingXray) snapshot() (removed, ensured []string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	removed = append([]string{}, x.removed...)
	ensured = append([]string{}, x.ensured...)
	return
}

func (x *recordingXray) reset() {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.removed = nil
	x.ensured = nil
}

const (
	tagDirect = "vless-ws-in"
	tagGRPC   = "vless-grpc-cdn-in"
	tagCDN    = "vless-xhttp-cdn-in"

	emailBase = "tg-777-lt-main-1-ws@vpn-platform.local"
	emailCDN  = "tg-777-lt-main-1-ws-cdn@vpn-platform.local"
	userID    = int64(777)
)

func newTestAgent(t *testing.T, nodeID string) (*Agent, *recordingXray, *PendingQueue) {
	t.Helper()
	dir := t.TempDir()
	repo := NewStateRepository(filepath.Join(dir, "state.json"))
	queue, err := NewPendingQueue(filepath.Join(dir, "pending.json"), nil)
	if err != nil {
		t.Fatalf("создание очереди: %v", err)
	}
	xray := newRecordingXray()
	agent, err := NewAgent(
		Config{NodeID: nodeID, ServerKey: nodeID, ApplyMode: "api", AgentVersion: "test"},
		nil, nil, xray, repo, queue,
	)
	if err != nil {
		t.Fatalf("создание агента: %v", err)
	}
	return agent, xray, queue
}

// seedUser заводит пользователю на узле три профиля: обычный, gRPC и CDN.
// CDN получает СОБСТВЕННЫЙ email — так его счётчик в Xray отделён от остальных.
func seedUser(t *testing.T, a *Agent, accessRev int64) {
	t.Helper()
	cmd := &kafkacontracts.NodeSyncUserCommand{
		Type:       kafkacontracts.VPNCommandNodeSyncUser,
		CommandID:  "sync-1",
		NodeID:     a.cfg.NodeID,
		ServerKey:  a.cfg.ServerKey,
		TelegramID: userID,
		AccessRev:  accessRev,
		Profiles: []kafkacontracts.VPNNodeUserProfile{
			{ItemKey: "lt", InboundTag: tagDirect, Email: emailBase, VLESSUUID: "uuid-1"},
			{ItemKey: "lt", InboundTag: tagGRPC, Email: emailBase, VLESSUUID: "uuid-1", Optional: true},
			{ItemKey: "lt", InboundTag: tagCDN, Email: emailCDN, VLESSUUID: "uuid-1", Optional: true},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := a.syncUser(context.Background(), cmd); err != nil {
		t.Fatalf("начальная синхронизация: %v", err)
	}
}

func enabledProfiles(a *Agent) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for key, p := range a.state.Profiles {
		if p.Enabled {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func revokeCDNOnly(t *testing.T, a *Agent, accessRev int64, commandID string) int {
	t.Helper()
	cmd := &kafkacontracts.NodeRevokeUserCommand{
		Type:       kafkacontracts.VPNCommandNodeRevokeUser,
		CommandID:  commandID,
		NodeID:     a.cfg.NodeID,
		ServerKey:  a.cfg.ServerKey,
		TelegramID: userID,
		AccessRev:  accessRev,
		Reason:     "cdn_quota_exhausted",
		Scope:      kafkacontracts.RevokeScopeListedProfiles,
		Profiles: []kafkacontracts.VPNNodeUserProfile{
			{ItemKey: "lt", InboundTag: tagCDN, Email: emailCDN, VLESSUUID: "uuid-1"},
		},
		CreatedAt: time.Now().UTC(),
	}
	n, err := a.revokeUser(context.Background(), cmd)
	if err != nil {
		t.Fatalf("частичный отзыв: %v", err)
	}
	return n
}

// Пункты 1-5 требований: CDN снимается, обычная и gRPC-конфигурации на той же
// ноде остаются активными и остаются зарегистрированными в Xray.
func TestCDNRevokeTouchesOnlyCDNInbound(t *testing.T) {
	agent, xray, _ := newTestAgent(t, "lt-main-1")
	seedUser(t, agent, 5)

	if got := enabledProfiles(agent); len(got) != 3 {
		t.Fatalf("после синхронизации ожидались 3 активных профиля, получено %v", got)
	}
	xray.reset()

	revoked := revokeCDNOnly(t, agent, 5, "revoke-cdn-1")
	if revoked != 1 {
		t.Errorf("снято профилей: %d, ожидался ровно 1", revoked)
	}

	removed, ensured := xray.snapshot()
	if len(removed) != 1 || removed[0] != profileKey(tagCDN, emailCDN) {
		t.Fatalf("обращения к Xray на удаление: %v, ожидалось ровно [%s]", removed, profileKey(tagCDN, emailCDN))
	}
	if len(ensured) != 0 {
		t.Errorf("отзыв не должен ничего добавлять в Xray, получено %v", ensured)
	}

	got := enabledProfiles(agent)
	want := []string{profileKey(tagGRPC, emailBase), profileKey(tagDirect, emailBase)}
	sort.Strings(want)
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("активные профили после отзыва CDN: %v, ожидалось %v", got, want)
	}

	agent.mu.Lock()
	cdn, ok := agent.state.Profiles[profileKey(tagCDN, emailCDN)]
	agent.mu.Unlock()
	if !ok {
		t.Fatal("запись о CDN-профиле пропала из состояния — она должна остаться отключённой")
	}
	if cdn.Enabled {
		t.Error("CDN-профиль остался включённым после отзыва")
	}
}

// Пункт 11: даже если CDN и обычная конфигурация используют ОДИН И ТОТ ЖЕ
// email, операция удаления обязана остаться локальной для CDN-инбаунда.
// Это прямая проверка семантики RemoveUserOperation в Xray: она адресуется
// парой (Tag, Email), а не одному только Email.
func TestCDNRevokeLocalToInboundEvenWithSharedEmail(t *testing.T) {
	agent, xray, _ := newTestAgent(t, "lt-main-1")

	shared := emailBase
	cmd := &kafkacontracts.NodeSyncUserCommand{
		Type: kafkacontracts.VPNCommandNodeSyncUser, CommandID: "sync-shared",
		NodeID: agent.cfg.NodeID, ServerKey: agent.cfg.ServerKey,
		TelegramID: userID, AccessRev: 3,
		Profiles: []kafkacontracts.VPNNodeUserProfile{
			{ItemKey: "lt", InboundTag: tagDirect, Email: shared, VLESSUUID: "uuid-1"},
			{ItemKey: "lt", InboundTag: tagCDN, Email: shared, VLESSUUID: "uuid-1", Optional: true},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := agent.syncUser(context.Background(), cmd); err != nil {
		t.Fatalf("синхронизация с общим email: %v", err)
	}
	xray.reset()

	revoke := &kafkacontracts.NodeRevokeUserCommand{
		Type: kafkacontracts.VPNCommandNodeRevokeUser, CommandID: "revoke-shared",
		NodeID: agent.cfg.NodeID, ServerKey: agent.cfg.ServerKey,
		TelegramID: userID, AccessRev: 3,
		Scope: kafkacontracts.RevokeScopeListedProfiles,
		Profiles: []kafkacontracts.VPNNodeUserProfile{
			{ItemKey: "lt", InboundTag: tagCDN, Email: shared, VLESSUUID: "uuid-1"},
		},
		CreatedAt: time.Now().UTC(),
	}
	if _, err := agent.revokeUser(context.Background(), revoke); err != nil {
		t.Fatalf("отзыв при общем email: %v", err)
	}

	removed, _ := xray.snapshot()
	for _, r := range removed {
		if r == profileKey(tagDirect, shared) {
			t.Fatal("удаление затронуло обычный инбаунд — область действия нарушена")
		}
	}
	if len(removed) != 1 || removed[0] != profileKey(tagCDN, shared) {
		t.Fatalf("обращения к Xray: %v, ожидалось ровно [%s]", removed, profileKey(tagCDN, shared))
	}

	agent.mu.Lock()
	direct := agent.state.Profiles[profileKey(tagDirect, shared)]
	agent.mu.Unlock()
	if !direct.Enabled {
		t.Error("обычный профиль отключился вместе с CDN при общем email")
	}
}

// Пункт 8: повторное событие об исчерпании ничего дополнительно не отключает.
func TestRepeatedCDNRevokeIsIdempotent(t *testing.T) {
	agent, xray, _ := newTestAgent(t, "lt-main-1")
	seedUser(t, agent, 5)
	revokeCDNOnly(t, agent, 5, "revoke-1")
	before := enabledProfiles(agent)
	xray.reset()

	revokeCDNOnly(t, agent, 5, "revoke-2")

	after := enabledProfiles(agent)
	if len(after) != len(before) {
		t.Errorf("повторный отзыв изменил набор активных профилей: было %v, стало %v", before, after)
	}
	removed, _ := xray.snapshot()
	for _, r := range removed {
		if r != profileKey(tagCDN, emailCDN) {
			t.Errorf("повторный отзыв обратился к чужому профилю: %s", r)
		}
	}
}

// Пункт 9: reconcile-подобная повторная синхронизация БЕЗ CDN-профиля
// (желаемое состояние с исчерпанной квотой) не возвращает CDN и не трогает
// обычную конфигурацию.
func TestReconcileWithoutCDNKeepsRevokedAndPreservesDirect(t *testing.T) {
	agent, xray, _ := newTestAgent(t, "lt-main-1")
	seedUser(t, agent, 5)
	revokeCDNOnly(t, agent, 5, "revoke-1")
	xray.reset()

	// Оркестратор исключает CDN-профиль из desired state, пока квота исчерпана
	// (Service.publishSyncCommands: skipCDN).
	resync := &kafkacontracts.NodeSyncUserCommand{
		Type: kafkacontracts.VPNCommandNodeSyncUser, CommandID: "reconcile-1",
		NodeID: agent.cfg.NodeID, ServerKey: agent.cfg.ServerKey,
		TelegramID: userID, AccessRev: 5,
		Profiles: []kafkacontracts.VPNNodeUserProfile{
			{ItemKey: "lt", InboundTag: tagDirect, Email: emailBase, VLESSUUID: "uuid-1"},
			{ItemKey: "lt", InboundTag: tagGRPC, Email: emailBase, VLESSUUID: "uuid-1", Optional: true},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := agent.syncUser(context.Background(), resync); err != nil {
		t.Fatalf("повторная синхронизация: %v", err)
	}

	_, ensured := xray.snapshot()
	for _, e := range ensured {
		if e == profileKey(tagCDN, emailCDN) {
			t.Fatal("reconcile вернул CDN-учётку в Xray до легитимного сброса квоты")
		}
	}

	agent.mu.Lock()
	cdn := agent.state.Profiles[profileKey(tagCDN, emailCDN)]
	direct := agent.state.Profiles[profileKey(tagDirect, emailBase)]
	grpc := agent.state.Profiles[profileKey(tagGRPC, emailBase)]
	agent.mu.Unlock()

	if cdn.Enabled {
		t.Error("CDN-профиль снова активен после reconcile")
	}
	if !direct.Enabled || !grpc.Enabled {
		t.Error("reconcile отключил обычную или gRPC-конфигурацию")
	}
}

// Пункт 10: после сброса квоты CDN возвращается, а обычная конфигурация
// продолжает работать и не пересоздаётся с новым UUID.
func TestAfterResetCDNReturnsAndDirectSurvives(t *testing.T) {
	agent, xray, _ := newTestAgent(t, "lt-main-1")
	seedUser(t, agent, 5)
	revokeCDNOnly(t, agent, 5, "revoke-1")

	agent.mu.Lock()
	uuidBefore := agent.state.Profiles[profileKey(tagDirect, emailBase)].VLESSUUID
	agent.mu.Unlock()
	xray.reset()

	// Сброс квоты → следующий desired state снова содержит CDN. access_rev
	// растёт, потому что доступ пересобирается платёжным событием.
	seedUserRev := &kafkacontracts.NodeSyncUserCommand{
		Type: kafkacontracts.VPNCommandNodeSyncUser, CommandID: "sync-after-reset",
		NodeID: agent.cfg.NodeID, ServerKey: agent.cfg.ServerKey,
		TelegramID: userID, AccessRev: 6,
		Profiles: []kafkacontracts.VPNNodeUserProfile{
			{ItemKey: "lt", InboundTag: tagDirect, Email: emailBase, VLESSUUID: uuidBefore},
			{ItemKey: "lt", InboundTag: tagGRPC, Email: emailBase, VLESSUUID: uuidBefore, Optional: true},
			{ItemKey: "lt", InboundTag: tagCDN, Email: emailCDN, VLESSUUID: uuidBefore, Optional: true},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := agent.syncUser(context.Background(), seedUserRev); err != nil {
		t.Fatalf("синхронизация после сброса: %v", err)
	}

	if got := enabledProfiles(agent); len(got) != 3 {
		t.Errorf("после сброса ожидались 3 активных профиля, получено %v", got)
	}

	agent.mu.Lock()
	uuidAfter := agent.state.Profiles[profileKey(tagDirect, emailBase)].VLESSUUID
	agent.mu.Unlock()
	if uuidAfter != uuidBefore {
		t.Errorf("UUID обычной конфигурации изменился: было %q, стало %q", uuidBefore, uuidAfter)
	}
}

// Пункт 12 (часть на стороне узла): состояние узла N не содержит ничего о
// пользователе на узле M. Квоты ключуются парой (пользователь, узел) в БД,
// а агент вообще не знает о других узлах.
func TestOtherNodeStateUntouched(t *testing.T) {
	agentA, _, _ := newTestAgent(t, "lt-main-1")
	agentB, xrayB, _ := newTestAgent(t, "us-main-1")
	seedUser(t, agentA, 5)
	seedUser(t, agentB, 5)
	xrayB.reset()

	revokeCDNOnly(t, agentA, 5, "revoke-on-a")

	if got := enabledProfiles(agentB); len(got) != 3 {
		t.Errorf("отзыв на узле A изменил состояние узла B: %v", got)
	}
	removed, ensured := xrayB.snapshot()
	if len(removed) != 0 || len(ensured) != 0 {
		t.Errorf("отзыв на узле A вызвал обращения к Xray узла B: removed=%v ensured=%v", removed, ensured)
	}
}

// Полный отзыв (Scope по умолчанию) обязан сохранить прежнее поведение:
// пользователь теряет узел целиком, включая профили, которых нет в команде.
func TestFullRevokeStillTotal(t *testing.T) {
	agent, _, _ := newTestAgent(t, "lt-main-1")
	seedUser(t, agent, 5)

	cmd := &kafkacontracts.NodeRevokeUserCommand{
		Type: kafkacontracts.VPNCommandNodeRevokeUser, CommandID: "revoke-full",
		NodeID: agent.cfg.NodeID, ServerKey: agent.cfg.ServerKey,
		TelegramID: userID, AccessRev: 6,
		Profiles: []kafkacontracts.VPNNodeUserProfile{
			{ItemKey: "lt", InboundTag: tagDirect, Email: emailBase, VLESSUUID: "uuid-1"},
		},
		CreatedAt: time.Now().UTC(),
	}
	if _, err := agent.revokeUser(context.Background(), cmd); err != nil {
		t.Fatalf("полный отзыв: %v", err)
	}
	if got := enabledProfiles(agent); len(got) != 0 {
		t.Errorf("полный отзыв оставил активные профили: %v", got)
	}
}

// Пустой список профилей при частичном отзыве — ошибка, а не «снять всё».
// Без этой проверки баг в центре обернулся бы тотальным отзывом.
func TestListedScopeRejectsEmptyProfiles(t *testing.T) {
	agent, _, _ := newTestAgent(t, "lt-main-1")
	seedUser(t, agent, 5)

	cmd := &kafkacontracts.NodeRevokeUserCommand{
		Type: kafkacontracts.VPNCommandNodeRevokeUser, CommandID: "revoke-empty",
		NodeID: agent.cfg.NodeID, ServerKey: agent.cfg.ServerKey,
		TelegramID: userID, AccessRev: 5,
		Scope:     kafkacontracts.RevokeScopeListedProfiles,
		Profiles:  nil,
		CreatedAt: time.Now().UTC(),
	}
	if _, err := agent.revokeUser(context.Background(), cmd); err == nil {
		t.Fatal("частичный отзыв с пустым списком должен возвращать ошибку")
	}
	if got := enabledProfiles(agent); len(got) != 3 {
		t.Errorf("отвергнутая команда изменила состояние: %v", got)
	}
}

// Anti-rollback не должен блокировать частичный отзыв из-за более свежего
// access_rev у СОСЕДНЕГО профиля: иначе снятие CDN-учётки было бы навсегда
// заблокировано очередной синхронизацией основного доступа.
func TestPartialRevokeNotBlockedByNewerUnrelatedProfile(t *testing.T) {
	agent, _, _ := newTestAgent(t, "lt-main-1")
	seedUser(t, agent, 5)

	// Основной профиль обновился до rev 9, CDN остался на rev 5.
	bump := &kafkacontracts.NodeSyncUserCommand{
		Type: kafkacontracts.VPNCommandNodeSyncUser, CommandID: "sync-bump",
		NodeID: agent.cfg.NodeID, ServerKey: agent.cfg.ServerKey,
		TelegramID: userID, AccessRev: 9,
		Profiles: []kafkacontracts.VPNNodeUserProfile{
			{ItemKey: "lt", InboundTag: tagDirect, Email: emailBase, VLESSUUID: "uuid-1"},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := agent.syncUser(context.Background(), bump); err != nil {
		t.Fatalf("обновление основного профиля: %v", err)
	}

	if n := revokeCDNOnly(t, agent, 5, "revoke-after-bump"); n != 1 {
		t.Fatalf("частичный отзыв заблокирован anti-rollback: снято %d профилей", n)
	}

	agent.mu.Lock()
	direct := agent.state.Profiles[profileKey(tagDirect, emailBase)]
	cdn := agent.state.Profiles[profileKey(tagCDN, emailCDN)]
	agent.mu.Unlock()
	if !direct.Enabled {
		t.Error("основной профиль отключился")
	}
	if cdn.Enabled {
		t.Error("CDN-профиль не отключился")
	}
}

// Устаревшая команда отзыва (access_rev ниже применённого к САМОМУ CDN-профилю)
// обязана игнорироваться — это защита от replay.
func TestStalePartialRevokeIgnored(t *testing.T) {
	agent, _, _ := newTestAgent(t, "lt-main-1")
	seedUser(t, agent, 7)

	if n := revokeCDNOnly(t, agent, 3, "stale-revoke"); n != 0 {
		t.Errorf("устаревшая команда применилась: снято %d профилей", n)
	}
	if got := enabledProfiles(agent); len(got) != 3 {
		t.Errorf("устаревшая команда изменила состояние: %v", got)
	}
}

// ============================================================================
// Классификация трафика: отдельный email у CDN и признак inbound_tag.
// ============================================================================

func readQueue(t *testing.T, q *PendingQueue) []kafkacontracts.VPNNodeTrafficEvent {
	t.Helper()
	raw, err := os.ReadFile(q.path)
	if err != nil {
		t.Fatalf("чтение очереди: %v", err)
	}
	var items []pendingEvent
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatalf("разбор очереди: %v", err)
	}
	var out []kafkacontracts.VPNNodeTrafficEvent
	for _, it := range items {
		if it.EventType != string(kafkacontracts.VPNEventNodeTraffic) {
			continue
		}
		var ev kafkacontracts.VPNNodeTrafficEvent
		if err := json.Unmarshal(it.Payload, &ev); err != nil {
			t.Fatalf("разбор события трафика: %v", err)
		}
		out = append(out, ev)
	}
	return out
}

// CDN-учётка со своим email отправляется отдельным элементом и с ТЕГОМ, по
// которому центр её и классифицирует. Учётка, общая для нескольких инбаундов,
// уходит БЕЗ тега — центр обязан считать её неклассифицированной.
func TestTrafficReportCarriesInboundTag(t *testing.T) {
	agent, xray, queue := newTestAgent(t, "lt-main-1")
	seedUser(t, agent, 5)

	xray.mu.Lock()
	xray.traffic[emailBase] = UserTraffic{Uplink: 1000, Downlink: 2000}
	xray.traffic[emailCDN] = UserTraffic{Uplink: 300, Downlink: 700}
	xray.mu.Unlock()

	if err := agent.publishTrafficStats(context.Background()); err != nil {
		t.Fatalf("публикация трафика: %v", err)
	}

	events := readQueue(t, queue)
	if len(events) != 1 {
		t.Fatalf("ожидалось одно событие трафика, получено %d", len(events))
	}
	byEmail := map[string]kafkacontracts.VPNNodeTrafficItem{}
	for _, it := range events[0].Items {
		byEmail[it.Email] = it
	}
	if len(byEmail) != 2 {
		t.Fatalf("ожидались 2 учётки в отчёте, получено %d: %v", len(byEmail), byEmail)
	}

	cdn, ok := byEmail[emailCDN]
	if !ok {
		t.Fatal("CDN-учётка отсутствует в отчёте")
	}
	if cdn.InboundTag != tagCDN {
		t.Errorf("CDN-учётка пришла с тегом %q, ожидался %q", cdn.InboundTag, tagCDN)
	}
	if cdn.Uplink != 300 || cdn.Downlink != 700 {
		t.Errorf("CDN-трафик: up=%d down=%d, ожидалось 300/700", cdn.Uplink, cdn.Downlink)
	}

	base, ok := byEmail[emailBase]
	if !ok {
		t.Fatal("основная учётка отсутствует в отчёте")
	}
	// Один email в двух инбаундах (ws + grpc) — классификация неоднозначна.
	if base.InboundTag != "" {
		t.Errorf("общая учётка пришла с тегом %q, ожидалась пустая строка", base.InboundTag)
	}
	if base.Uplink != 1000 || base.Downlink != 2000 {
		t.Errorf("обычный трафик: up=%d down=%d, ожидалось 1000/2000", base.Uplink, base.Downlink)
	}
}

// Обратное направление инварианта: обычный трафик не может попасть в CDN-квоту,
// потому что его учётка физически другая и приходит с другим тегом.
func TestDirectTrafficNeverCarriesCDNTag(t *testing.T) {
	agent, xray, queue := newTestAgent(t, "lt-main-1")

	// Только обычный профиль, CDN не выдан.
	cmd := &kafkacontracts.NodeSyncUserCommand{
		Type: kafkacontracts.VPNCommandNodeSyncUser, CommandID: "sync-direct-only",
		NodeID: agent.cfg.NodeID, ServerKey: agent.cfg.ServerKey,
		TelegramID: userID, AccessRev: 1,
		Profiles: []kafkacontracts.VPNNodeUserProfile{
			{ItemKey: "lt", InboundTag: tagDirect, Email: emailBase, VLESSUUID: "uuid-1"},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := agent.syncUser(context.Background(), cmd); err != nil {
		t.Fatalf("синхронизация: %v", err)
	}
	xray.mu.Lock()
	xray.traffic[emailBase] = UserTraffic{Uplink: 5_000_000_000, Downlink: 9_000_000_000}
	xray.mu.Unlock()

	if err := agent.publishTrafficStats(context.Background()); err != nil {
		t.Fatalf("публикация трафика: %v", err)
	}

	events := readQueue(t, queue)
	if len(events) != 1 || len(events[0].Items) != 1 {
		t.Fatalf("ожидался один элемент в одном событии, получено %+v", events)
	}
	if got := events[0].Items[0].InboundTag; got != tagDirect {
		t.Errorf("обычная учётка пришла с тегом %q, ожидался %q", got, tagDirect)
	}
	if got := events[0].Items[0].InboundTag; got == tagCDN {
		t.Fatal("обычный трафик помечен как CDN")
	}
}
