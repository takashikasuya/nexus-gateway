// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf

import (
	"bytes"
	"encoding/json"
	"fmt"

	"nexus-gateway/internal/telemetry"
)

// AttachmentThreshold is the byte-length threshold for DTDPF contract ④'s
// attachment decision: "values > 1,024 byte compact UTF-8 JSON" ([C4-05]).
const AttachmentThreshold = 1024

// ExceedsAttachmentThreshold reports whether the record's Values object,
// encoded as compact UTF-8 JSON, is strictly larger than AttachmentThreshold
// bytes per [C4-05]. A record with no Values object never exceeds it.
func ExceedsAttachmentThreshold(record *telemetry.Record) (bool, error) {
	if record == nil || record.Values == nil {
		return false, nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, record.Values); err != nil {
		return false, fmt.Errorf("compact telemetry values for attachment threshold: %w", err)
	}
	return buf.Len() > AttachmentThreshold, nil
}
