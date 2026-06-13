//go:build windows

package lifecycle

func diskStatsMB() (usedMB, totalMB float64) { return 0, 0 }
