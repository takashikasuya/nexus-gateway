// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

// Package storage implements the DTDPF contract ④ GW Upload Storage client:
// a direct-PUT uploader (Binding B) behind a small, testable Uploader seam.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

// Uploader durably stores an object's bytes under objectName. Implementations
// must be safe to call again with the same objectName/body (idempotent
// overwrite), since FEAT-050's crash-replay semantics may retry an upload.
type Uploader interface {
	Put(ctx context.Context, objectName string, body []byte) error
}

// Hash returns the lowercase 64-character SHA-256 hex digest of body,
// per DTDPF contract ④ [C4-06].
func Hash(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// ObjectName returns the DTDPF contract ④ [C4-02] object name for a telemetry
// attachment: "{telemetryID}.{extension}".
func ObjectName(telemetryID, extension string) string {
	return telemetryID + "." + extension
}
