package secrets

import (
	"crypto/rand"
	"encoding/hex"
)

// TokenBytes returns a secure random token of the specified number of bytes
func TokenBytes(nbytes uint8) ([]byte, error) {
	token := make([]byte, nbytes)
	_, err := rand.Read(token)
	return token, err
}

// TokenHex returns a secure random token of the specified number of bytes, encoded as hex
func TokenHex(nbytes uint8) (string, error) {
	tokenBytes, err := TokenBytes(nbytes)
	return hex.EncodeToString(tokenBytes), err
}
