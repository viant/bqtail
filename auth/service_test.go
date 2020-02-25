package auth

import (
	"context"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

func TestService_AuthHTTPClient(t *testing.T) {
	srv := New(BqTailClient, "", Scopes...)

	ctx := context.Background()
	client, err := srv.AuthHTTPClient(ctx)
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
	log.Println("Email body: ", string(data))
	time.Sleep(time.Minute)

}
