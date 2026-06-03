package vpn_node_agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	proxyman "github.com/xtls/xray-core/app/proxyman/command"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy/vless"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var ErrXrayUnavailable = errors.New("xray unavailable")

type XrayController interface {
	EnsureVLESSUser(ctx context.Context, inboundTag, email, vlessUUID, flow string, level uint32) error
	RemoveVLESSUser(ctx context.Context, inboundTag, email string) error
	Close() error
}

type DryRunXrayController struct{}

func NewDryRunXrayController() *DryRunXrayController { return &DryRunXrayController{} }
func (d *DryRunXrayController) EnsureVLESSUser(_ context.Context, inboundTag, email, vlessUUID, flow string, level uint32) error {
	slog.Info("dry-run ensure user", "inbound", inboundTag, "email", email, "uuid", vlessUUID, "flow", flow, "level", level)
	return nil
}
func (d *DryRunXrayController) RemoveVLESSUser(_ context.Context, inboundTag, email string) error {
	slog.Info("dry-run remove user", "inbound", inboundTag, "email", email)
	return nil
}
func (d *DryRunXrayController) Close() error { return nil }

type APIXrayController struct {
	addr   string
	conn   *grpc.ClientConn
	client proxyman.HandlerServiceClient
}

func NewAPIXrayController(ctx context.Context, addr string) (*APIXrayController, error) {
	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}),
	)
	if err != nil {
		return nil, fmt.Errorf("connect xray api %s: %w", addr, err)
	}
	return &APIXrayController{addr: addr, conn: conn, client: proxyman.NewHandlerServiceClient(conn)}, nil
}

func (c *APIXrayController) EnsureVLESSUser(ctx context.Context, inboundTag, email, vlessUUID, flow string, level uint32) error {
	if strings.TrimSpace(inboundTag) == "" {
		return fmt.Errorf("empty inbound tag")
	}
	if strings.TrimSpace(email) == "" {
		return fmt.Errorf("empty email")
	}
	if strings.TrimSpace(vlessUUID) == "" {
		return fmt.Errorf("empty vless uuid")
	}

	if err := c.RemoveVLESSUser(ctx, inboundTag, email); err != nil && !isIgnorableRemoveError(err) {
		return fmt.Errorf("remove before add: %w", err)
	}

	account := &vless.Account{Id: vlessUUID, Flow: flow}
	user := &protocol.User{Level: level, Email: email, Account: serial.ToTypedMessage(account)}
	op := &proxyman.AddUserOperation{User: user}

	_, err := c.client.AlterInbound(ctx, &proxyman.AlterInboundRequest{Tag: inboundTag, Operation: serial.ToTypedMessage(op)})
	if err != nil {
		return wrapXrayErr(fmt.Errorf("xray add user inbound=%s email=%s: %w", inboundTag, email, err))
	}
	return nil
}

func (c *APIXrayController) RemoveVLESSUser(ctx context.Context, inboundTag, email string) error {
	if strings.TrimSpace(inboundTag) == "" {
		return fmt.Errorf("empty inbound tag")
	}
	if strings.TrimSpace(email) == "" {
		return fmt.Errorf("empty email")
	}
	op := &proxyman.RemoveUserOperation{Email: email}
	_, err := c.client.AlterInbound(ctx, &proxyman.AlterInboundRequest{Tag: inboundTag, Operation: serial.ToTypedMessage(op)})
	if err != nil {
		return wrapXrayErr(fmt.Errorf("xray remove user inbound=%s email=%s: %w", inboundTag, email, err))
	}
	return nil
}

func (c *APIXrayController) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func wrapXrayErr(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
			return fmt.Errorf("%w: %v", ErrXrayUnavailable, err)
		}
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "connection refused") || strings.Contains(msg, "deadline") || strings.Contains(msg, "unavailable") {
		return fmt.Errorf("%w: %v", ErrXrayUnavailable, err)
	}
	return err
}

func isIgnorableRemoveError(err error) bool {
	if err == nil {
		return true
	}
	msg := strings.ToLower(err.Error())
	return errors.Is(err, context.Canceled) ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "not exist") ||
		strings.Contains(msg, "no such user") ||
		strings.Contains(msg, "not a user")
}
