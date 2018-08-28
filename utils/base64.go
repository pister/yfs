package utils

import (
	"encoding/base64"
)

func EncodeBase64ToString(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func DecodeBase64fromString(data string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(data)
}