package egress

import (
	"context"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "nexus-gateway/gen"
	"nexus-gateway/internal/dispatch"
)

// Agent connects to the Building OS GatewayEgress service, sends Hello, and
// dispatches incoming ControlCommands via the Dispatcher (ADR-0004).
// It reconnects with a fixed 1s backoff on stream errors.
type Agent struct {
	addr      string
	gatewayID string
	d         *dispatch.Dispatcher
	creds     credentials.TransportCredentials
}

func New(_ *nats.Conn, addr, gatewayID string, d *dispatch.Dispatcher, creds credentials.TransportCredentials) *Agent {
	return &Agent{addr: addr, gatewayID: gatewayID, d: d, creds: creds}
}

// Run connects to BOS and processes commands until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) {
	conn, err := grpc.NewClient(a.addr, grpc.WithTransportCredentials(a.creds))
	if err != nil {
		slog.Error("egress: dial failed", "addr", a.addr, "err", err)
		return
	}
	defer conn.Close()

	client := pb.NewGatewayEgressClient(conn)

	for ctx.Err() == nil {
		if err := a.runStream(ctx, client); err != nil && ctx.Err() == nil {
			slog.Warn("egress stream error, reconnecting", "err", err)
			time.Sleep(time.Second)
		}
	}
}

func (a *Agent) runStream(ctx context.Context, client pb.GatewayEgressClient) error {
	stream, err := client.Connect(ctx)
	if err != nil {
		return err
	}

	if err := stream.Send(&pb.EgressUp{M: &pb.EgressUp_Hello{Hello: &pb.Hello{GatewayId: a.gatewayID}}}); err != nil {
		return err
	}

	for {
		down, err := stream.Recv()
		if err != nil {
			return err
		}
		cmd := down.GetCommand()
		if cmd == nil {
			continue
		}
		result := a.d.Execute(ctx, cmd)
		if err := stream.Send(&pb.EgressUp{M: &pb.EgressUp_Result{Result: result}}); err != nil {
			return err
		}
	}
}
