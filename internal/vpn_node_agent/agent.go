package vpn_node_agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	commonkafka "vpn_node_agent/internal/common/kafka"
	kafkacontracts "vpn_node_agent/internal/contracts/kafka"

	kafkago "github.com/segmentio/kafka-go"
)

type Agent struct {
	cfg      Config
	reader   *kafkago.Reader
	producer *commonkafka.Producer
	xray     XrayController
	repo     *StateRepository
	pending  *PendingQueue

	// load — показатели нагрузки ноды: активные подключения и скорость
	// трафика. Заполняется между heartbeat-ами, читается при их отправке.
	load *loadStats

	mu    sync.Mutex
	state *AgentState
}

func NewAgent(cfg Config, reader *kafkago.Reader, producer *commonkafka.Producer, xray XrayController, repo *StateRepository, pending *PendingQueue) (*Agent, error) {
	state, err := repo.Load(cfg.NodeID)
	if err != nil {
		return nil, err
	}
	// Показатели нагрузки: активные подключения и скорость трафика ноды.
	loadTracker := newLoadStats(inboundTagsFromEnv(os.Getenv("NODE_AGENT_INBOUND_TAGS")))

	return &Agent{cfg: cfg, reader: reader, producer: producer, xray: xray, repo: repo,
		pending: pending, state: state, load: loadTracker}, nil
}

func (a *Agent) Run(ctx context.Context) error {
	slog.Info("vpn-node-agent started", "node_id", a.cfg.NodeID, "server_key", a.cfg.ServerKey, "apply_mode", a.cfg.ApplyMode)

	if err := a.reconcileState(ctx); err != nil {
		slog.Warn("vpn-node-agent reconcile state error (continuing anyway)", "err", err)
	}

	heartbeatTicker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	cleanupTicker := time.NewTicker(5 * time.Minute)
	defer cleanupTicker.Stop()

	trafficInterval := a.cfg.TrafficInterval
	if trafficInterval <= 0 {
		trafficInterval = 60 * time.Second
	}
	trafficTicker := time.NewTicker(trafficInterval)
	defer trafficTicker.Stop()

	if a.pending != nil {
		go a.pending.RunPublisher(ctx)
	}

	go func() {
		// Перезапуск-обёртка: если consumeCommands по любой причине завершился
		// (не из-за отмены контекста) — ждём и запускаем заново. Это последняя
		// линия обороны против «мёртвого навсегда» консьюмера после обрыва Kafka.
		for {
			if ctx.Err() != nil {
				return
			}
			err := a.consumeCommands(ctx)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				slog.Error("vpn-node-agent command consumer exited, restarting", "err", err)
			} else {
				slog.Warn("vpn-node-agent command consumer returned nil unexpectedly, restarting")
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}()

	if err := a.publishHeartbeat(ctx); err != nil {
		slog.Warn("vpn-node-agent initial heartbeat enqueue failed", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-heartbeatTicker.C:
			if err := a.publishHeartbeat(ctx); err != nil {
				slog.Warn("vpn-node-agent heartbeat enqueue failed", "err", err)
			}
		case <-cleanupTicker.C:
			a.cleanupSeenCommands()
			a.forgetTrafficCounters()
		case <-trafficTicker.C:
			if err := a.publishTrafficStats(ctx); err != nil {
				slog.Warn("vpn-node-agent traffic stats enqueue failed", "err", err)
			}
		}
	}
}

func (a *Agent) Stop() error {
	if a.reader != nil {
		_ = a.reader.Close()
	}
	if a.producer != nil {
		_ = a.producer.Close()
	}
	if a.xray != nil {
		_ = a.xray.Close()
	}
	return nil
}

// markCommandSeen возвращает true, если команда видна впервые (нужно обработать).
// Если команда уже была обработана (есть в seen, TTL не истёк) — возвращает false.
func (a *Agent) markCommandSeen(commandID string) (bool, error) {
	if commandID == "" {
		return true, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state.SeenCommands == nil {
		a.state.SeenCommands = map[string]time.Time{}
	}
	if _, ok := a.state.SeenCommands[commandID]; ok {
		return false, nil
	}
	a.state.SeenCommands[commandID] = time.Now().UTC()
	return true, a.repo.Save(a.state)
}

// isCommandSeen возвращает true, если commandID уже зарегистрирован как успешно
// обработанный. В отличие от markCommandSeen, эта функция НЕ модифицирует state —
// просто читает. Используется консьюмером, чтобы дешёво отсеять дубликаты до
// фактической обработки, не блокируя retry при временных сбоях.
func (a *Agent) isCommandSeen(commandID string) bool {
	if commandID == "" {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state.SeenCommands == nil {
		return false
	}
	_, ok := a.state.SeenCommands[commandID]
	return ok
}

// saveState сохраняет состояние агента на диск под общим локом.
func (a *Agent) saveState() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.repo.Save(a.state)
}

// accumulateTraffic превращает показание Xray (кумулятив с момента ЕГО старта)
// в монотонно неубывающий счётчик узла и возвращает итоговые значения.
//
// Падение показания относительно предыдущего означает рестарт Xray: прошлое
// значение переносится в базу, и счёт продолжается с нуля. Без этого центр
// получал откат счётчика, а с ним — навсегда замороженную цифру трафика.
func (a *Agent) accumulateTraffic(email string, uplink, downlink int64) (int64, int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.state.Traffic == nil {
		a.state.Traffic = map[string]TrafficCounter{}
	}
	c := a.state.Traffic[email]
	if uplink < c.LastUplink {
		c.BaseUplink += c.LastUplink
	}
	if downlink < c.LastDownlink {
		c.BaseDownlink += c.LastDownlink
	}
	c.LastUplink = uplink
	c.LastDownlink = downlink
	c.UpdatedAt = time.Now().UTC()
	a.state.Traffic[email] = c

	return c.BaseUplink + c.LastUplink, c.BaseDownlink + c.LastDownlink
}

// forgetTrafficCounters убирает счётчики, для которых на узле больше нет ни
// одного профиля: карта не должна расти бесконечно.
func (a *Agent) forgetTrafficCounters() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.state.Traffic) == 0 {
		return
	}
	alive := make(map[string]struct{}, len(a.state.Profiles))
	for _, p := range a.state.Profiles {
		alive[p.Email] = struct{}{}
	}
	changed := false
	for email := range a.state.Traffic {
		if _, ok := alive[email]; !ok {
			delete(a.state.Traffic, email)
			changed = true
		}
	}
	if changed {
		_ = a.repo.Save(a.state)
	}
}

func (a *Agent) cleanupSeenCommands() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state.SeenCommands == nil {
		return
	}
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	changed := false
	for k, t := range a.state.SeenCommands {
		if t.Before(cutoff) {
			delete(a.state.SeenCommands, k)
			changed = true
		}
	}
	if changed {
		_ = a.repo.Save(a.state)
	}
}

func (a *Agent) handleSyncUser(ctx context.Context, cmd *kafkacontracts.NodeSyncUserCommand) error {
	if cmd == nil {
		return nil
	}
	slog.Info("vpn-node-agent sync user", "telegram_id", cmd.TelegramID, "access_rev", cmd.AccessRev, "profiles", len(cmd.Profiles), "command_id", cmd.CommandID)

	err := a.syncUser(ctx, cmd)
	event := kafkacontracts.VPNNodeUserSyncedEvent{
		Type:       kafkacontracts.VPNEventNodeUserSynced,
		CommandID:  cmd.CommandID,
		NodeID:     a.cfg.NodeID,
		ServerKey:  a.cfg.ServerKey,
		TelegramID: cmd.TelegramID,
		AccessRev:  cmd.AccessRev,
		Profiles:   len(cmd.Profiles),
		Success:    err == nil,
		CreatedAt:  time.Now().UTC(),
	}
	if err != nil {
		event.Error = err.Error()
		a.publishNodeError(ctx, cmd.CommandID, err)
	}

	if errors.Is(err, ErrXrayUnavailable) {
		return err
	}

	if pubErr := a.enqueue(a.cfg.NodeID, &event); pubErr != nil {
		slog.Error("vpn-node-agent enqueue sync result failed", "err", pubErr)
		if err == nil {
			return pubErr
		}
	}
	return err
}

func (a *Agent) syncUser(ctx context.Context, cmd *kafkacontracts.NodeSyncUserCommand) error {
	if cmd.NodeID != a.cfg.NodeID {
		return nil
	}
	if cmd.TelegramID == 0 {
		return fmt.Errorf("empty telegram_id")
	}
	if len(cmd.Profiles) == 0 {
		return fmt.Errorf("empty profiles")
	}

	for _, p := range cmd.Profiles {
		if p.InboundTag == "" {
			return fmt.Errorf("empty inbound tag for item_key=%s", p.ItemKey)
		}
		if p.Email == "" {
			return fmt.Errorf("empty email for item_key=%s", p.ItemKey)
		}
		if p.VLESSUUID == "" {
			return fmt.Errorf("empty vless uuid for item_key=%s", p.ItemKey)
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, p := range cmd.Profiles {
		if existing, ok := a.state.Profiles[profileKey(p.InboundTag, p.Email)]; ok {
			if existing.AccessRev > cmd.AccessRev {
				return nil
			}
		}
	}

	for _, p := range cmd.Profiles {
		if err := a.xray.EnsureVLESSUser(ctx, p.InboundTag, p.Email, p.VLESSUUID, p.Flow, p.Level); err != nil {
			// Опциональный профиль (например CDN) с несуществующим на этом узле
			// inbound — не ошибка: пропускаем его, не валя всю команду. Узел без
			// CDN-inbound просто не получает CDN-доступ, но основной профиль
			// применяется штатно.
			if p.Optional && isMissingInboundError(err) {
				slog.Warn("vpn-node-agent optional profile skipped (inbound missing)",
					"inbound", p.InboundTag, "email", p.Email)
				continue
			}
			return err
		}
		a.state.Profiles[profileKey(p.InboundTag, p.Email)] = AppliedProfile{
			TelegramID:  cmd.TelegramID,
			AccessRev:   cmd.AccessRev,
			ItemKey:     p.ItemKey,
			InboundTag:  p.InboundTag,
			Email:       p.Email,
			VLESSUUID:   p.VLESSUUID,
			Flow:        p.Flow,
			Level:       p.Level,
			AccessUntil: p.AccessUntil,
			Enabled:     true,
			UpdatedAt:   time.Now().UTC(),
		}
	}
	return a.repo.Save(a.state)
}

func (a *Agent) handleRevokeUser(ctx context.Context, cmd *kafkacontracts.NodeRevokeUserCommand) error {
	if cmd == nil {
		return nil
	}
	slog.Info("vpn-node-agent revoke user", "telegram_id", cmd.TelegramID, "access_rev", cmd.AccessRev, "profiles", len(cmd.Profiles), "reason", cmd.Reason, "command_id", cmd.CommandID)

	revoked, err := a.revokeUser(ctx, cmd)
	event := kafkacontracts.VPNNodeUserRevokedEvent{
		Type:       kafkacontracts.VPNEventNodeUserRevoked,
		CommandID:  cmd.CommandID,
		NodeID:     a.cfg.NodeID,
		ServerKey:  a.cfg.ServerKey,
		TelegramID: cmd.TelegramID,
		AccessRev:  cmd.AccessRev,
		Profiles:   revoked,
		Success:    err == nil,
		CreatedAt:  time.Now().UTC(),
	}
	if err != nil {
		event.Error = err.Error()
		a.publishNodeError(ctx, cmd.CommandID, err)
	}

	if errors.Is(err, ErrXrayUnavailable) {
		return err
	}

	if pubErr := a.enqueue(a.cfg.NodeID, &event); pubErr != nil {
		slog.Error("vpn-node-agent enqueue revoke result failed", "err", pubErr)
		if err == nil {
			return pubErr
		}
	}
	return err
}

func (a *Agent) revokeUser(ctx context.Context, cmd *kafkacontracts.NodeRevokeUserCommand) (int, error) {
	if cmd.NodeID != a.cfg.NodeID {
		return 0, nil
	}
	if cmd.TelegramID == 0 {
		return 0, fmt.Errorf("empty telegram_id")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, existing := range a.state.Profiles {
		if existing.TelegramID == cmd.TelegramID && existing.AccessRev > cmd.AccessRev {
			return 0, nil
		}
	}

	// Отзыв обязан быть тотальным: пользователь теряет доступ к узлу целиком,
	// а не только к тем inbound'ам, что попали в команду. Список профилей в
	// команде строится по ТЕКУЩЕЙ конфигурации эндпоинтов, и если эндпоинт
	// отвязали от узла (CDN переехал на другую ноду), профиль в команду не
	// попадёт — а регистрация в inbound на узле останется, и отозванный
	// пользователь продолжит через него ходить.
	//
	// Поэтому объединяем присланный список со ВСЕМИ профилями пользователя,
	// которые узел знает про себя, и дедуплицируем по (inbound_tag, email).
	targets := make([]kafkacontracts.VPNNodeUserProfile, 0, len(cmd.Profiles)+4)
	seenTargets := make(map[string]struct{}, len(cmd.Profiles)+4)
	addTarget := func(p kafkacontracts.VPNNodeUserProfile) {
		if p.InboundTag == "" || p.Email == "" {
			return
		}
		key := profileKey(p.InboundTag, p.Email)
		if _, dup := seenTargets[key]; dup {
			return
		}
		seenTargets[key] = struct{}{}
		targets = append(targets, p)
	}
	for _, p := range cmd.Profiles {
		addTarget(p)
	}
	for _, existing := range a.state.Profiles {
		if existing.TelegramID != cmd.TelegramID {
			continue
		}
		addTarget(kafkacontracts.VPNNodeUserProfile{
			ItemKey:    existing.ItemKey,
			InboundTag: existing.InboundTag,
			Email:      existing.Email,
			VLESSUUID:  existing.VLESSUUID,
		})
	}

	revoked := 0
	for _, p := range targets {
		if p.InboundTag == "" || p.Email == "" {
			continue
		}
		if err := a.xray.RemoveVLESSUser(ctx, p.InboundTag, p.Email); err != nil && !isIgnorableRemoveError(err) {
			return revoked, err
		}
		key := profileKey(p.InboundTag, p.Email)
		if existing, ok := a.state.Profiles[key]; ok {
			existing.Enabled = false
			existing.AccessRev = cmd.AccessRev
			existing.UpdatedAt = time.Now().UTC()
			a.state.Profiles[key] = existing
		}
		revoked++
	}
	if err := a.repo.Save(a.state); err != nil {
		return revoked, err
	}
	return revoked, nil
}

func (a *Agent) publishHeartbeat(ctx context.Context) error {
	a.mu.Lock()
	count := a.repo.CountEnabled(a.state)
	a.mu.Unlock()

	// Скорость трафика снимаем прямо здесь: heartbeat идёт чаще, чем сбор
	// пользовательского трафика, и нагрузка на канал должна обновляться
	// с той же частотой.
	a.collectNodeTraffic(ctx)

	onlineUsers, upBps, downBps := int(0), int64(0), int64(0)
	if a.load != nil {
		onlineUsers, upBps, downBps = a.load.snapshot()
	}

	event := kafkacontracts.VPNNodeHeartbeatEvent{
		Type:         kafkacontracts.VPNEventNodeHeartbeat,
		NodeID:       a.cfg.NodeID,
		ServerKey:    a.cfg.ServerKey,
		Online:       true,
		AppliedUsers: count,
		OnlineUsers:  onlineUsers,
		UplinkBps:    upBps,
		DownlinkBps:  downBps,
		XrayAPIAddr:  a.cfg.XrayAPIAddr,
		AgentVersion: a.cfg.AgentVersion,
		CreatedAt:    time.Now().UTC(),
	}
	return a.enqueue(a.cfg.NodeID, &event)
}

// publishTrafficStats собирает кумулятивный трафик по всем включённым профилям
// узла (через Xray Stats API) и публикует пачкой в vpn.events. Дедуп по email:
// у одного пользователя несколько профилей (основной + CDN) с ОДНИМ email, поэтому
// достаточно запросить трафик по email один раз.
func (a *Agent) publishTrafficStats(ctx context.Context) error {
	// снимок нужных (email -> telegram_id) под локом, запросы к Xray — вне лока
	a.mu.Lock()
	type ue struct {
		telegramID int64
		email      string
	}
	seen := make(map[string]struct{}, len(a.state.Profiles))
	targets := make([]ue, 0, len(a.state.Profiles))
	for _, p := range a.state.Profiles {
		if !p.Enabled || p.Email == "" {
			continue
		}
		if _, ok := seen[p.Email]; ok {
			continue
		}
		seen[p.Email] = struct{}{}
		targets = append(targets, ue{telegramID: p.TelegramID, email: p.Email})
	}
	a.mu.Unlock()

	if len(targets) == 0 {
		return nil
	}

	items := make([]kafkacontracts.VPNNodeTrafficItem, 0, len(targets))
	for _, t := range targets {
		qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		tr, err := a.xray.QueryUserTraffic(qctx, t.email)
		cancel()
		if err != nil {
			if errors.Is(err, ErrXrayUnavailable) {
				// Xray временно недоступен — прекращаем проход, попробуем в следующий тик
				return err
			}
			slog.Warn("vpn-node-agent query traffic failed", "email", t.email, "err", err)
			continue
		}
		// В центр уходит НЕ сырое показание Xray, а накопленное узлом значение:
		// оно переживает рестарт Xray и потому монотонно. Нулевой трафик тоже
		// шлём — оркестратор увидит, что пользователь активен.
		totalUp, totalDown := a.accumulateTraffic(t.email, tr.Uplink, tr.Downlink)
		items = append(items, kafkacontracts.VPNNodeTrafficItem{
			TelegramID: t.telegramID,
			Email:      t.email,
			Uplink:     totalUp,
			Downlink:   totalDown,
		})
	}

	// Счётчики изменились — фиксируем их на диске, иначе рестарт агента
	// потерял бы базу и накопленное начало бы считаться заново.
	if err := a.saveState(); err != nil {
		slog.Warn("vpn-node-agent save traffic counters failed", "err", err)
	}

	// Срез суммарных счётчиков: пользователь, у которого сумма выросла с
	// прошлого прохода, реально передавал данные — это и есть подключение.
	if a.load != nil {
		totals := make(map[string]int64, len(items))
		for _, it := range items {
			totals[it.Email] = it.Uplink + it.Downlink
		}
		a.load.observeUsers(totals)
	}

	if len(items) == 0 {
		return nil
	}

	event := kafkacontracts.VPNNodeTrafficEvent{
		Type:      kafkacontracts.VPNEventNodeTraffic,
		NodeID:    a.cfg.NodeID,
		ServerKey: a.cfg.ServerKey,
		Items:     items,
		CreatedAt: time.Now().UTC(),
	}
	return a.enqueue(a.cfg.NodeID, &event)
}

func (a *Agent) reconcileState(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, p := range a.state.Profiles {
		if !p.Enabled {
			continue
		}
		if p.AccessUntil != nil && !p.AccessUntil.After(time.Now().UTC()) {
			_ = a.xray.RemoveVLESSUser(ctx, p.InboundTag, p.Email)
			p.Enabled = false
			p.UpdatedAt = time.Now().UTC()
			a.state.Profiles[profileKey(p.InboundTag, p.Email)] = p
			continue
		}
		if err := a.xray.EnsureVLESSUser(ctx, p.InboundTag, p.Email, p.VLESSUUID, p.Flow, p.Level); err != nil {
			if errors.Is(err, ErrXrayUnavailable) {
				slog.Warn("vpn-node-agent reconcile skipped due to xray unavailable", "inbound", p.InboundTag, "email", p.Email)
				continue
			}
			return err
		}
	}
	return a.repo.Save(a.state)
}

func (a *Agent) enqueue(key string, payload any) error {
	if a.pending == nil {
		return nil
	}
	return a.pending.Enqueue(commonkafka.TopicVPNEvents, key, payload)
}

func (a *Agent) publishNodeError(_ context.Context, commandID string, err error) {
	if err == nil {
		return
	}
	event := kafkacontracts.VPNNodeErrorEvent{
		Type:      kafkacontracts.VPNEventNodeError,
		CommandID: commandID,
		NodeID:    a.cfg.NodeID,
		ServerKey: a.cfg.ServerKey,
		Error:     err.Error(),
		CreatedAt: time.Now().UTC(),
	}
	if enqErr := a.enqueue(a.cfg.NodeID, &event); enqErr != nil {
		slog.Error("vpn-node-agent enqueue node error failed", "err", enqErr)
	}
}
