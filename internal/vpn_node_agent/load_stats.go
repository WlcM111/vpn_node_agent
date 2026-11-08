package vpn_node_agent

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"
)

// ============================================================================
// Сбор показателей нагрузки ноды.
//
// Раньше нагрузка оценивалась числом выданных учётных записей. Это ёмкость, а
// не нагрузка: учётка ничего не стоит, пока по ней не идёт трафик, а один
// активный пользователь может занять канал сильнее сотни спящих.
//
// Здесь считаются две величины:
//
//   OnlineUsers — пользователи, чьи счётчики трафика сдвинулись с прошлого
//   опроса. Это надёжнее, чем счётчик открытых сокетов: показывает тех, кто
//   реально передаёт данные.
//
//   Uplink/DownlinkBps — скорость трафика ноды, разница суммарных счётчиков
//   инбаундов, делённая на прошедшее время.
//
// Оба показателя строятся на GetStats, который уже работает в бою для
// пользовательского трафика — новых зависимостей от непроверенных API нет.
// ============================================================================

// loadStats хранит состояние между опросами, чтобы считать дельты.
type loadStats struct {
	mu sync.Mutex

	// перечень инбаундов, по которым суммируется трафик ноды
	inboundTags []string

	// предыдущий срез счётчиков пользователей: email -> суммарные байты
	prevUserTotals map[string]int64

	// предыдущий суммарный счётчик ноды и момент его снятия
	prevNodeTotalUp   int64
	prevNodeTotalDown int64
	prevNodeAt        time.Time

	// последние вычисленные значения — их читает heartbeat
	onlineUsers int
	uplinkBps   int64
	downlinkBps int64
}

func newLoadStats(inboundTags []string) *loadStats {
	return &loadStats{
		inboundTags:    inboundTags,
		prevUserTotals: make(map[string]int64),
	}
}

// snapshot отдаёт последние посчитанные значения для heartbeat.
func (l *loadStats) snapshot() (onlineUsers int, upBps, downBps int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.onlineUsers, l.uplinkBps, l.downlinkBps
}

// observeUsers обновляет число активных пользователей по свежему срезу
// счётчиков. Активным считается тот, у кого суммарный трафик вырос.
//
// Новый пользователь активным при первом появлении не считается: у него нет
// предыдущего значения, и любой ненулевой счётчик выглядел бы как прирост,
// хотя трафик мог быть набран задолго до этого.
func (l *loadStats) observeUsers(totals map[string]int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	active := 0
	for email, total := range totals {
		prev, seen := l.prevUserTotals[email]
		if seen && total > prev {
			active++
		}
	}
	l.onlineUsers = active
	l.prevUserTotals = totals
}

// observeNodeTraffic пересчитывает скорость по новому суммарному срезу.
func (l *loadStats) observeNodeTraffic(totalUp, totalDown int64, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	defer func() {
		l.prevNodeTotalUp = totalUp
		l.prevNodeTotalDown = totalDown
		l.prevNodeAt = at
	}()

	if l.prevNodeAt.IsZero() {
		return // первый замер — сравнивать не с чем
	}

	elapsed := at.Sub(l.prevNodeAt).Seconds()
	if elapsed <= 0 {
		return
	}

	deltaUp := totalUp - l.prevNodeTotalUp
	deltaDown := totalDown - l.prevNodeTotalDown

	// Отрицательная дельта означает, что Xray перезапустился и счётчики
	// обнулились. Показываем ноль вместо мусорного отрицательного значения
	// и ждём следующего интервала.
	if deltaUp < 0 || deltaDown < 0 {
		l.uplinkBps, l.downlinkBps = 0, 0
		return
	}

	l.uplinkBps = int64(float64(deltaUp) / elapsed)
	l.downlinkBps = int64(float64(deltaDown) / elapsed)
}

// collectNodeTraffic опрашивает счётчики всех инбаундов и обновляет скорость.
//
// Набор инбаундов — NODE_AGENT_INBOUND_TAGS, а если переменная пуста — теги
// включённых профилей агента на момент замера. Профили приходят от центра,
// поэтому список сам следует за конфигурацией ноды: инбаунд без пользователей
// трафика не несёт, а новый транспорт попадает в замер вместе с первым
// пользователем.
func (a *Agent) collectNodeTraffic(ctx context.Context) {
	if a.load == nil {
		return
	}

	tags := a.load.inboundTags
	if len(tags) == 0 {
		tags = a.enabledInboundTags()
	}

	var totalUp, totalDown int64
	for _, tag := range tags {
		qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		tr, err := a.xray.QueryInboundTraffic(qctx, tag)
		cancel()
		if err != nil {
			// Один недоступный инбаунд не должен ронять весь замер:
			// пропускаем его и считаем по остальным.
			continue
		}
		totalUp += tr.Uplink
		totalDown += tr.Downlink
	}

	a.load.observeNodeTraffic(totalUp, totalDown, time.Now())
}

// inboundTagsFromEnv читает явно заданный список инбаундов для подсчёта
// трафика ноды. Пустая переменная даёт пустой список: тогда инбаунды берутся
// из включённых профилей агента (см. enabledInboundTags).
func inboundTagsFromEnv(raw string) []string {
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// enabledInboundTags — различные теги инбаундов включённых профилей агента,
// по алфавиту.
func (a *Agent) enabledInboundTags() []string {
	a.mu.Lock()
	defer a.mu.Unlock()

	seen := make(map[string]struct{}, 4)
	out := make([]string, 0, 4)
	for _, p := range a.state.Profiles {
		if !p.Enabled || p.InboundTag == "" {
			continue
		}
		if _, ok := seen[p.InboundTag]; ok {
			continue
		}
		seen[p.InboundTag] = struct{}{}
		out = append(out, p.InboundTag)
	}
	sort.Strings(out)
	return out
}
