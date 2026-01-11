package utils

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

func HashSha256(key string) (uint64, error) {
	hasher := sha256.New()

	_, err := hasher.Write([]byte(key))
	if err != nil {
		return 0, fmt.Errorf("hasher.Write: %w", err)
	}

	return binary.BigEndian.Uint64(hasher.Sum(nil)), nil
}
