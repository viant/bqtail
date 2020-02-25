package auth

import (
	"context"
	"fmt"
	"github.com/viant/bqtail/shared"
	"golang.org/x/oauth2/google"
	goptions "google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
	"net/http"
)

func getDefaultHTTPClient(ctx context.Context, scopes []string) (*http.Client, error) {
	o := []goptions.ClientOption{
		goptions.WithScopes(scopes...),
		goptions.WithUserAgent(shared.UserAgent),
	}
	httpClient, _, err := htransport.NewClient(ctx, o...)
	return httpClient, err
}

func getDefaultProject(ctx context.Context, scopes []string) (string, error) {
	fmt.Printf("FindDefaultCredentials\n")
	credentials, err := google.FindDefaultCredentials(ctx, scopes...)
	if err != nil {
		return "", err
	}
	return credentials.ProjectID, nil
}

//DefaultHTTPClientProvider defaultHTTP client
var DefaultHTTPClientProvider = getDefaultHTTPClient

//DefaultProjectProvider default projectid provider
var DefaultProjectProvider = getDefaultProject
