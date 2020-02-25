package auth

import (
	"os"
	"path"
)

//Client represents an oauth client
type Client struct {
	ID       string
	Secret   string
	TokenURL string
}

func (c *Client) Init() {
	if c.TokenURL == "" {
		c.TokenURL = path.Join(os.Getenv("HOME"), ".secret", c.ID+".json")
	}
}
