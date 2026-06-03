package vpn_node_agent

import (
	"context"
	"encoding/json"
	"log/slog"

	commonkafka "vpn_node_agent/internal/common/kafka"
	kafkacontracts "vpn_node_agent/internal/contracts/kafka"
)

func (a *Agent) consumeCommands(ctx context.Context) error {
	if a.reader == nil {
		return nil
	}
	slog.Info("vpn-node-agent starting vpn.commands consumer")

	return commonkafka.RunConsumerWithDLT(
		ctx,
		a.reader,
		"vpn-node-agent",
		func(ctx context.Context, msg commonkafka.Message) error {
			var envelope struct {
				Type      string `json:"type"`
				NodeID    string `json:"node_id"`
				CommandID string `json:"command_id"`
			}
			if err := json.Unmarshal(msg.Value, &envelope); err != nil {
				slog.Warn("vpn-node-agent invalid command envelope", "err", err)
				return commonkafka.ErrSkip
			}

			// Игнорируем команды, адресованные не нашей ноде.
			if envelope.NodeID != "" && envelope.NodeID != a.cfg.NodeID {
				return nil
			}

			// Дедуп: только проверяем, не помечаем. Помечать будем после
			// успешной обработки, чтобы при ошибке retry мог повторить.
			if envelope.CommandID != "" && a.isCommandSeen(envelope.CommandID) {
				slog.Debug("vpn-node-agent duplicate command ignored",
					"command_id", envelope.CommandID, "type", envelope.Type)
				return nil
			}

			opCtx, cancel := context.WithTimeout(ctx, a.cfg.ApplyTimeout)
			defer cancel()

			switch envelope.Type {
			case string(kafkacontracts.VPNCommandNodeSyncUser):
				var cmd kafkacontracts.NodeSyncUserCommand
				if err := json.Unmarshal(msg.Value, &cmd); err != nil {
					slog.Warn("vpn-node-agent invalid sync command", "err", err)
					return commonkafka.ErrSkip
				}
				if cmd.NodeID != a.cfg.NodeID {
					return nil
				}
				if err := a.handleSyncUser(opCtx, &cmd); err != nil {
					return err
				}
				// Помечаем как обработанную ТОЛЬКО после успеха.
				// Ошибка записи в state.json не критична — следующий
				// retry просто выполнит handleSyncUser ещё раз (он идемпотентен).
				if _, err := a.markCommandSeen(cmd.CommandID); err != nil {
					slog.Warn("vpn-node-agent mark command seen failed",
						"command_id", cmd.CommandID, "err", err)
				}
				return nil

			case string(kafkacontracts.VPNCommandNodeRevokeUser):
				var cmd kafkacontracts.NodeRevokeUserCommand
				if err := json.Unmarshal(msg.Value, &cmd); err != nil {
					slog.Warn("vpn-node-agent invalid revoke command", "err", err)
					return commonkafka.ErrSkip
				}
				if cmd.NodeID != a.cfg.NodeID {
					return nil
				}
				if err := a.handleRevokeUser(opCtx, &cmd); err != nil {
					return err
				}
				if _, err := a.markCommandSeen(cmd.CommandID); err != nil {
					slog.Warn("vpn-node-agent mark command seen failed",
						"command_id", cmd.CommandID, "err", err)
				}
				return nil

			case string(kafkacontracts.VPNCommandNodePing):
				if err := a.publishHeartbeat(opCtx); err != nil {
					return err
				}
				if _, err := a.markCommandSeen(envelope.CommandID); err != nil {
					slog.Warn("vpn-node-agent mark command seen failed",
						"command_id", envelope.CommandID, "err", err)
				}
				return nil

			default:
				slog.Debug("vpn-node-agent ignored command type", "type", envelope.Type)
				return nil
			}
		},
		a.producer,
		commonkafka.TopicVPNCommandsDLT,
	)
}
