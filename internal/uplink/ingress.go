package uplink

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "nexus-gateway/gen"
)

// Ingress streams TelemetryFrames to the Building OS GatewayIngress service.
// It reads from frames, sends immediately (no buffering here — S&F is upstream),
// and half-closes the stream every checkpointSize frames or checkpointAge,
// whichever comes first, to collect the StreamAck (ADR-0002, decisions.md).
type Ingress struct {
	addr      string
	gatewayID string
	frames    <-chan *pb.TelemetryFrame
	conn      *grpc.ClientConn
}

const (
	checkpointSize = 1000
	checkpointAge  = 5 * time.Second
)

func NewIngress(ctx context.Context, addr, gatewayID string, frames <-chan *pb.TelemetryFrame) (*Ingress, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	return &Ingress{addr: addr, gatewayID: gatewayID, frames: frames, conn: conn}, nil
}

// Run streams frames until ctx is cancelled. It reconnects on stream errors.
func (u *Ingress) Run(ctx context.Context) {
	defer u.conn.Close()
	client := pb.NewGatewayIngressClient(u.conn)

	for ctx.Err() == nil {
		if err := u.runStream(ctx, client); err != nil && ctx.Err() == nil {
			slog.Warn("ingress stream error, reconnecting", "err", err)
			time.Sleep(time.Second)
		}
	}
}

func (u *Ingress) runStream(ctx context.Context, client pb.GatewayIngressClient) error {
	stream, err := client.StreamTelemetry(ctx)
	if err != nil {
		return err
	}

	sent := 0
	tick := time.NewTicker(checkpointAge)
	defer tick.Stop()

	checkpoint := func() error {
		ack, err := stream.CloseAndRecv()
		if err != nil {
			return fmt.Errorf("checkpoint recv: %w", err)
		}
		if int64(sent) > ack.Accepted {
			slog.Warn("ingress: drift detected", "sent", sent, "accepted", ack.Accepted)
		}
		// Re-open stream for next batch
		newStream, err := client.StreamTelemetry(ctx)
		if err != nil {
			return fmt.Errorf("re-open stream: %w", err)
		}
		stream = newStream
		sent = 0
		return nil
	}

	for {
		select {
		case frame, ok := <-u.frames:
			if !ok {
				_, _ = stream.CloseAndRecv()
				return nil
			}
			if err := stream.Send(frame); err != nil {
				return fmt.Errorf("send: %w", err)
			}
			sent++
			if sent >= checkpointSize {
				tick.Reset(checkpointAge)
				if err := checkpoint(); err != nil {
					return err
				}
			}

		case <-tick.C:
			if sent > 0 {
				if err := checkpoint(); err != nil {
					return err
				}
			}

		case <-ctx.Done():
			if sent > 0 {
				_, _ = stream.CloseAndRecv()
			}
			return nil
		}
	}
}
