package secret

import "encoding/base64"

func decodeBase64IfNeeded(data []byte) []byte {
	if decoded, err := base64.StdEncoding.DecodeString(string(data)); err == nil {
		return decoded
	}
	return data
}
