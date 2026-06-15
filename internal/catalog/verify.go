package catalog

import (
	"context"
	"fmt"
	"os/exec"
)

// Verifier checks that a digest-pinned image reference carries a valid cosign signature.
type Verifier interface {
	Verify(ctx context.Context, imageRef string) error
}

// CosignVerifier calls the cosign CLI to verify the image signature.
// KeyPath is the path to a cosign public key; leave empty for keyless verification.
type CosignVerifier struct {
	KeyPath string
}

func (v CosignVerifier) Verify(ctx context.Context, imageRef string) error {
	args := []string{"verify"}
	if v.KeyPath != "" {
		args = append(args, "--key", v.KeyPath)
	}
	args = append(args, imageRef)
	out, err := exec.CommandContext(ctx, "cosign", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("catalog: cosign verify %q: %w\n%s", imageRef, err, out)
	}
	return nil
}

// NoopVerifier always succeeds — for dev/CI environments where cosign is not available.
type NoopVerifier struct{}

func (NoopVerifier) Verify(_ context.Context, _ string) error { return nil }
