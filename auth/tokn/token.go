package tokn

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"golang.org/x/oauth2"
	"time"
)

type Token oauth2.Token

//OAuth2Token returns oauth2.Token
func (t Token) OAuth2Token() *oauth2.Token {
	token := oauth2.Token(t)
	return &token
}

//Expired returns true if expired
func (t Token) Expired(now time.Time) bool {
	return t.Expiry.Before(now)
}

//Persist stores token
func (t Token) Persist(ctx context.Context, URL string, fs afs.Service) error {
	data, err := json.Marshal(t)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal token: %v, %+v", URL, t)
	}
	err = fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
	if err != nil {
		return errors.Wrapf(err, "failed to store token: %v, %+v", URL, t)
	}
	return err
}

//FromURL creates a token from URL
func FromURL(ctx context.Context, URL string, fs afs.Service) (*Token, error) {
	exists, err := fs.Exists(ctx, URL, option.NewObjectKind(true))
	if err == nil && !exists {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "failed to check token: %v", URL)
	}
	data, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get token: %v", URL)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read token: %v", URL)
	}
	token := &Token{}
	err = json.Unmarshal(data, token)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal token: %v, %s", URL, data)
	}
	return token, nil
}
