package uplink

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "nexus-gateway/gen"
	"nexus-gateway/internal/storeforward"
)

// Config holds tunable checkpoint parameters.
type Config struct {
	CheckpointSize int
	CheckpointAge  time.Duration
}

// DefaultConfig is the production default: send immediately, checkpoint every 5s/1000 frames.
var DefaultConfig = Config{CheckpointSize: 1000, CheckpointAge: 5 * time.Second}

// Ingress streams TelemetryFrames from a storeforward.Buffer to the Building OS
// GatewayIngress service. Frames are sent immediately as they are read; the stream
// is half-closed every CheckpointSize frames or CheckpointAge (whichever comes first)
// to collect the StreamAck and advance the buffer cursor (ADR-0002).
type Ingress struct {
	addr      string
	gatewayID string
	buf       *storeforward.Buffer
	conn      *grpc.ClientConn
	cfg       Config
}

func NewIngress(_ context.Context, addr, gatewayID string, buf *storeforward.Buffer, cfg Config, creds credentials.TransportCredentials) (*Ingress, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	return &Ingress{addr: addr, gatewayID: gatewayID, buf: buf, conn: conn, cfg: cfg}, nil
}

// Run streams frames until ctx is cancelled. Reconnects on stream errors.
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
	// stream is opened lazily on the first frame to avoid holding an idle connection
	// that server-side idle-timeout policies would tear down repeatedly.
	var stream pb.GatewayIngress_StreamTelemetryClient

	cursor := u.buf.Cursor()
	tick := time.NewTicker(u.cfg.CheckpointAge)
	defer tick.Stop()
	pollTick := time.NewTicker(50 * time.Millisecond)
	defer pollTick.Stop()

	// batch tracks frames sent since last checkpoint.
	type sentFrame struct {
		seq     int64
		pointID string
	}
	var batch []sentFrame

	openStream := func() error {
		var err error
		stream, err = client.StreamTelemetry(ctx)
		return err
	}

	checkpoint := func() error {
		if stream == nil || len(batch) == 0 {
			return nil
		}
		ack, err := stream.CloseAndRecv()
		if err != nil {
			return fmt.Errorf("checkpoint recv: %w", err)
		}
		sent := int64(len(batch))
		lost := sent - ack.Accepted
		if lost > 0 {
			// Guard against negative or out-of-range ack.Accepted from a buggy server.
			start := int(ack.Accepted)
			if start < 0 {
				start = 0
			}
			for _, sf := range batch[start:] {
				u.buf.RecordDrift(sf.pointID, 1)
			}
			slog.Warn("ingress: drift", "sent", sent, "accepted", ack.Accepted, "lost", lost)
		}
		// Advance cursor past entire batch regardless (best-effort, ADR-0002).
		cursor = batch[len(batch)-1].seq
		if err := u.buf.Advance(cursor); err != nil {
			return fmt.Errorf("advance cursor: %w", err)
		}
		batch = batch[:0]
		stream = nil // force lazy re-open on next Send

		tick.Reset(u.cfg.CheckpointAge)
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			if stream != nil && len(batch) > 0 {
				_, _ = stream.CloseAndRecv()
			}
			return nil

		case <-tick.C:
			if err := checkpoint(); err != nil {
				return err
			}

		case <-pollTick.C:
			frames, err := u.buf.ReadBatch(cursor, 32)
			if err != nil {
				slog.Warn("ingress: buffer read error", "err", err)
				continue
			}
			for _, sf := range frames {
				if stream == nil {
					if err := openStream(); err != nil {
						return err
					}
				}
				if err := stream.Send(sf.Frame); err != nil {
					return fmt.Errorf("send: %w", err)
				}
				batch = append(batch, sentFrame{seq: sf.Seq, pointID: sf.Frame.PointId})
				cursor = sf.Seq
				if len(batch) >= u.cfg.CheckpointSize {
					if err := checkpoint(); err != nil {
						return err
					}
				}
			}
		}
	}
}
