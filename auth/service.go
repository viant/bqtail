package auth

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/bqtail/auth/browser"
	"github.com/viant/bqtail/auth/endpoint"
	"github.com/viant/bqtail/auth/gcloud"
	"github.com/viant/bqtail/auth/tokn"
	"github.com/viant/bqtail/shared"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"math/rand"
	"net/http"
	"os"
	"time"
)

//Service auth service
type Service interface {
	//AuthHTTPClient return oauth http client
	AuthHTTPClient(ctx context.Context, scopes []string) (*http.Client, error)

	ProjectID(ctx context.Context, scopes []string) (string, error)
}

type service struct {
	projectID     string
	useGsUtilAuth bool
	fs            afs.Service
	client        *Client
	scopes        []string
	oath2Config   *oauth2.Config
}

func (s *service) auth() (*tokn.Token, error) {
	oConfig, err := s.oauth2Config()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to auth")
	}
	server, err := endpoint.New()
	if err != nil {
		return nil, errors.Wrapf(err, "failed start auth callback server")
	}
	oConfig.RedirectURL = fmt.Sprintf("http://localhost:%v/auth.html", server.Port)

	go server.Start()
	state := randToken()
	URL := oConfig.AuthCodeURL(state)
	cmd := browser.Open(URL)
	var cmdError error
	go func() {
		if cmdError = cmd.Start(); cmdError != nil {
			server.Close()
		}
	}()
	if err = server.Wait(); err != nil {
		return nil, errors.Wrap(err, "failed to handler auth")
	}
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	token, err := oConfig.Exchange(oauth2.NoContext, server.AuthCode())
	if err != nil {
		return nil, errors.Wrap(err, "failed to get auth token")
	}
	result := tokn.Token(*token)
	return &result, nil
}

func (s *service) getToken(ctx context.Context) (*tokn.Token, error) {
	oToken, err := tokn.FromURL(ctx, s.client.TokenURL, s.fs)
	if oToken == nil || oToken.Expired(time.Now()) {
		oToken, err = s.auth()
		if err != nil {
			return nil, err
		}
		if err = oToken.Persist(ctx, s.client.TokenURL, s.fs); err != nil {
			return nil, errors.Wrapf(err, "failed to cache token")
		}
	}
	return oToken, nil
}

func (s *service) oauth2Config() (*oauth2.Config, error) {
	if s.oath2Config != nil {
		return s.oath2Config, nil
	}
	if s.client == nil {
		return nil, errors.New("failed to get oauth2.Config: auth client was nil")
	}
	cfg := &oauth2.Config{
		ClientID:     s.client.ID,
		ClientSecret: s.client.Secret,
		Scopes:       s.scopes,
		Endpoint:     google.Endpoint,
	}
	return cfg, nil
}

func randToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

func (s *service) ProjectID(ctx context.Context, scopes []string) (string, error) {
	if s.projectID != "" {
		return s.projectID, nil
	}

	if os.Getenv(shared.ServiceAccountCredentials) != "" {
		credentials, err := google.FindDefaultCredentials(ctx, scopes...)
		if err != nil {
			return "", err
		}
		s.projectID = credentials.ProjectID
		return credentials.ProjectID, nil
	}

	if !isInGCE() {
		if s.useGsUtilAuth {
			if gsUtilCfg, _ := gcloud.ConfigFromURL(ctx, gsUtilConfigLocation, s.fs); gsUtilCfg != nil {
				s.projectID = gsUtilCfg.Core.Project
				return gsUtilCfg.Core.Project, nil
			}
		}
		client, err := s.AuthHTTPClient(ctx, scopes)
		if err == nil {
			project, err := SelectProjectID(ctx, client)
			if err == nil {
				s.projectID = project
				return s.projectID, nil
			}
		}
	}
	credentials, err := google.FindDefaultCredentials(ctx, scopes...)
	if err != nil {
		return "", err
	}
	return credentials.ProjectID, nil
}

func (s *service) AuthHTTPClient(ctx context.Context, scopes []string) (client *http.Client, err error) {
	if os.Getenv(shared.ServiceAccountCredentials) != "" {
		client, err = google.DefaultClient(ctx, scopes...)
		return client, err
	}
	if !isInGCE() {
		if s.useGsUtilAuth {
			if client, err = s.loadGsUtilAuthHTTPClient(ctx); client != nil {
				return client, err
			}
		}
		if client, err = s.getClientAuthHTTPClient(ctx); client != nil {
			return client, err
		}
	}
	return google.DefaultClient(ctx, scopes...)
}

func (s *service) loadGsUtilAuthHTTPClient(ctx context.Context) (*http.Client, error) {
	gsUtilCfg, err := gcloud.ConfigFromURL(ctx, gsUtilConfigLocation, s.fs)
	if err == nil {
		shared.LogF("%+v\n", gsUtilCfg)
		conf, err := google.NewSDKConfig(gsUtilCfg.Core.Account)
		if err != nil {
			return nil, err
		}
		return conf.Client(oauth2.NoContext), nil
	}
	return nil, nil
}

func (s *service) getClientAuthHTTPClient(ctx context.Context) (*http.Client, error) {
	oToken, err := s.getToken(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get oToken")
	}
	oauthConfig, err := s.oauth2Config()
	if err != nil {
		return nil, err
	}
	client := oauthConfig.Client(oauth2.NoContext, oToken.OAuth2Token())
	return client, err
}

//New creates a new auth service
func New(client *Client, useGsUtilAuth bool, projectID string, scopes ...string) Service {
	client.Init()
	return &service{
		projectID:     projectID,
		useGsUtilAuth: useGsUtilAuth,
		fs:            afs.New(),
		client:        client,
		scopes:        scopes,
	}
}
