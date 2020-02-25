package auth

import "os"

var computeKeys = []string{"GCLOUD_PROJECT", "GOOGLE_CLOUD_PROJECT", "GOOGLE_CLOUD_PROJECT"}

func isInGCE() bool {
	for _, key := range computeKeys {
		if os.Getenv(key) != "" {
			return true
		}
	}
	return false
}
