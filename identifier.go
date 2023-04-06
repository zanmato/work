package work

import (
	"crypto/rand"
	"encoding/hex"
	"io"
)

func makeIdentifier() string {
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}
