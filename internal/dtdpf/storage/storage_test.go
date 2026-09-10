// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package storage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"nexus-gateway/internal/dtdpf/storage"
)

func TestHash_ReturnsLowercaseHexSHA256(t *testing.T) {
	// echo -n "hello" | sha256sum
	assert.Equal(t, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", storage.Hash([]byte("hello")))
}

func TestHash_DifferentBytesDifferentHash(t *testing.T) {
	assert.NotEqual(t, storage.Hash([]byte("a")), storage.Hash([]byte("b")))
}

func TestObjectName_FormatsTelemetryIDAndExtension(t *testing.T) {
	assert.Equal(t, "43217568-443d-4b24-96d1-59887fdd1628.json",
		storage.ObjectName("43217568-443d-4b24-96d1-59887fdd1628", "json"))
}
