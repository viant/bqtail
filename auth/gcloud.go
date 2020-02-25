package auth

import (
	"os"
	"path"
)

var gsUtilConfigLocation = path.Join(os.Getenv("HOME"), ".config", "gcloud", "configurations")
