package auth

import (
	"context"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"testing"
)

func TestService_AuthHTTPClient(t *testing.T) {
	srv := New(BqTailClient, true, "", Scopes...)
	ctx := context.Background()
	client, err := srv.AuthHTTPClient(ctx, Scopes)
	if err != nil {
		log.Fatal(err)
	}
	email, err := client.Get("https://www.googleapis.com/oauth2/v3/userinfo")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer email.Body.Close()
	data, _ := ioutil.ReadAll(email.Body)
	assert.True(t, len(data) > 0)

}
