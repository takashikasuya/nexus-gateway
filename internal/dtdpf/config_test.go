// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/dtdpf"
)

func TestLoadPointConfigResolvesDTIDAndProtocol(t *testing.T) {
	config, err := dtdpf.LoadPointConfig(strings.NewReader(`{
		"updateDatetime": "2026-09-09T00:00:00Z",
		"correlationId": "config-1",
		"pointDataUpdateDatetime": "2026-09-08T00:00:00Z",
		"pointData": {
			"5": [
				{"dtId":"R90_000001","topic":"takenaka.co.jp/R90/temp","typeId":1,"type":2,"protocol":"bacnet"},
				{"dtId":"R90_000002","topic":"takenaka.co.jp/R90/mqtt"}
			]
		}
	}`))
	require.NoError(t, err)

	metadata, ok := config.Resolve("R90_000001", "bacnet")
	require.True(t, ok)
	assert.Equal(t, int64(5), metadata.RootID)
	assert.Equal(t, "takenaka.co.jp/R90/temp", metadata.Topic)
	require.NotNil(t, metadata.Type)
	assert.Equal(t, 2, *metadata.Type)

	_, ok = config.Resolve("R90_000001", "mqtt")
	assert.False(t, ok, "protocol mismatch must not resolve")
	_, ok = config.Resolve("R90_000002", "mqtt")
	assert.True(t, ok, "omitted protocol defaults to mqtt")
}

func TestLoadPointConfigRejectsInvalidEntries(t *testing.T) {
	tests := map[string]string{
		"missing pointData": `{}`,
		"invalid rootId":    `{"pointData":{"site":[{"dtId":"p1","topic":"t"}]}}`,
		"empty topic":       `{"pointData":{"5":[{"dtId":"p1","topic":""}]}}`,
		"invalid type":      `{"pointData":{"5":[{"dtId":"p1","topic":"t","type":0}]}}`,
		"duplicate dtId":    `{"pointData":{"5":[{"dtId":"p1","topic":"a"}],"6":[{"dtId":"p1","topic":"b"}]}}`,
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := dtdpf.LoadPointConfig(strings.NewReader(document))
			require.Error(t, err)
		})
	}
}
