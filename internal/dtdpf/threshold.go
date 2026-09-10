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

// MaxAttachmentBytes is the largest values payload DTDPF will durably upload
// as a file attachment ([C4-02]: 10 MiB). Larger payloads are sent as a
// summary-only event with no fileUpload properties; they are never chunked.
const MaxAttachmentBytes = 10 * 1024 * 1024

// CompactValues returns the compact (no whitespace) UTF-8 JSON encoding of
// record.Values, or nil when the record has no Values object.
func CompactValues(record *telemetry.Record) ([]byte, error) {
	if record == nil || record.Values == nil {
		return nil, nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, record.Values); err != nil {
		return nil, fmt.Errorf("compact telemetry values for attachment threshold: %w", err)
	}
	return buf.Bytes(), nil
}

// ExceedsAttachmentThreshold reports whether the record's Values object,
// encoded as compact UTF-8 JSON, is strictly larger than AttachmentThreshold
// bytes per [C4-05]. A record with no Values object never exceeds it.
func ExceedsAttachmentThreshold(record *telemetry.Record) (bool, error) {
	compact, err := CompactValues(record)
	if err != nil {
		return false, err
	}
	return len(compact) > AttachmentThreshold, nil
}

