// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/dtdpf"
	"nexus-gateway/internal/telemetry"
)

// valuesJSONOfLength builds a compact JSON object `{"v":"...."}` whose exact
// UTF-8 byte length is n, so boundary tests exercise the real threshold
// comparison rather than an approximation.
func valuesJSONOfLength(n int) []byte {
	const overhead = len(`{"v":""}`)
	padding := n - overhead
	return []byte(`{"v":"` + strings.Repeat("x", padding) + `"}`)
}

func TestExceedsAttachmentThreshold_AtBoundaryDoesNotExceed(t *testing.T) {
	values := valuesJSONOfLength(dtdpf.AttachmentThreshold)
	require.Len(t, values, dtdpf.AttachmentThreshold)

	exceeds, err := dtdpf.ExceedsAttachmentThreshold(&telemetry.Record{Values: values})
	require.NoError(t, err)
	assert.False(t, exceeds, "exactly 1,024 bytes must not exceed the threshold")
}

func TestExceedsAttachmentThreshold_OneByteOverExceeds(t *testing.T) {
	values := valuesJSONOfLength(dtdpf.AttachmentThreshold + 1)
	require.Len(t, values, dtdpf.AttachmentThreshold+1)

	exceeds, err := dtdpf.ExceedsAttachmentThreshold(&telemetry.Record{Values: values})
	require.NoError(t, err)
	assert.True(t, exceeds, "1,025 bytes must exceed the threshold")
}

func TestExceedsAttachmentThreshold_NoValuesNeverExceeds(t *testing.T) {
	exceeds, err := dtdpf.ExceedsAttachmentThreshold(&telemetry.Record{Value: 1.0})
	require.NoError(t, err)
	assert.False(t, exceeds)
}

func TestExceedsAttachmentThreshold_MeasuresCompactEncodingNotRawWhitespace(t *testing.T) {
	// Padded with insignificant whitespace so the raw byte length exceeds the
	// threshold, but the compact encoding does not — the threshold must be
	// computed on the compact form per [C4-05], not the raw payload.
	padding := strings.Repeat(" ", dtdpf.AttachmentThreshold)
	raw := []byte(`{"v": "x"` + padding + `}`)
	require.Greater(t, len(raw), dtdpf.AttachmentThreshold, "raw payload must actually exceed the threshold for this test to be meaningful")

	exceeds, err := dtdpf.ExceedsAttachmentThreshold(&telemetry.Record{Values: raw})
	require.NoError(t, err)
	assert.False(t, exceeds)
}
