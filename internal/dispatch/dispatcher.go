package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	pb "nexus-gateway/gen"
	"nexus-gateway/internal/pointlist"
)

// Resolver is the subset of pointlist.SyncedResolver needed by the Dispatcher.
type Resolver interface {
	ResolveReverse(pointID string) (pointlist.Entry, bool)
}

// ConnectorReply is the JSON payload returned by a connector write handler over NATS.
type ConnectorReply struct {
	Success  bool   `json:"success"`
	Response string `json:"response"`
}

// WriteRequest is the JSON payload sent to a connector write handler.
type WriteRequest struct {
	ControlID string  `json:"control_id"`
	LocalID   string  `json:"local_id"`
	DeviceRef string  `json:"device_ref"`
	Value     float64 `json:"value"`
	Priority  int32   `json:"priority"`
}

// Dispatcher routes ControlCommands to connectors via NATS core request-reply (ADR-0004).
// NATS subject: cmd.<protocol>.<connectorID>
type Dispatcher struct {
	nc       *nats.Conn
	resolver Resolver
	timeout  time.Duration
	mu       sync.Mutex
	dedup    map[string]*pb.ControlResult
}

func New(nc *nats.Conn, resolver Resolver, timeout time.Duration) *Dispatcher {
	return &Dispatcher{nc: nc, resolver: resolver, timeout: timeout, dedup: make(map[string]*pb.ControlResult)}
}

// Execute dispatches a ControlCommand and returns the result. Duplicate control_ids
// within the lifetime of this dispatcher return the cached result without re-writing.
func (d *Dispatcher) Execute(ctx context.Context, cmd *pb.ControlCommand) *pb.ControlResult {
	d.mu.Lock()
	if r, ok := d.dedup[cmd.ControlId]; ok {
		d.mu.Unlock()
		return r
	}
	d.mu.Unlock()

	result := d.execute(ctx, cmd)

	d.mu.Lock()
	d.dedup[cmd.ControlId] = result
	d.mu.Unlock()

	return result
}

func (d *Dispatcher) execute(ctx context.Context, cmd *pb.ControlCommand) *pb.ControlResult {
	entry, ok := d.resolver.ResolveReverse(cmd.PointId)
	if !ok {
		return &pb.ControlResult{ControlId: cmd.ControlId, Success: false, Response: "no_connector"}
	}
	if !entry.Writable {
		return &pb.ControlResult{ControlId: cmd.ControlId, Success: false, Response: "not_writable"}
	}

	req := WriteRequest{
		ControlID: cmd.ControlId,
		LocalID:   entry.LocalID,
		DeviceRef: entry.DeviceRef,
		Value:     cmd.PresentValue,
		Priority:  cmd.Priority,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return &pb.ControlResult{ControlId: cmd.ControlId, Success: false, Response: fmt.Sprintf("marshal_error: %v", err)}
	}

	subject := fmt.Sprintf("cmd.%s.%s", entry.Protocol, entry.ConnectorID)

	reqCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	msg, err := d.nc.RequestWithContext(reqCtx, subject, data)
	if err != nil {
		return &pb.ControlResult{ControlId: cmd.ControlId, Success: false, Response: "timeout"}
	}

	var reply ConnectorReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return &pb.ControlResult{ControlId: cmd.ControlId, Success: false, Response: fmt.Sprintf("parse_error: %v", err)}
	}

	return &pb.ControlResult{ControlId: cmd.ControlId, Success: reply.Success, Response: reply.Response}
}
