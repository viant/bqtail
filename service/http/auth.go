package http

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
	"io/ioutil"
	"net/http"
)

const (
	//UserAgent bqtail user agent
	UserAgent              = "Viant/BqTail"
	metadataServerTokenURL = "http://metadata/computeMetadata/v1/instance/service-accounts/default/identity?audience="
)

const (
	//CloudPlatformScope GCP platform scopes
	CloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
	//DevstorageFullControlScope DevstorageFullControl scope
	DevstorageFullControlScope = "https://www.googleapis.com/auth/devstorage.full_control"
	//ComputeCloudPlatformScope GCP scope
	ComputeCloudPlatformScope = "https://www.googleapis.com/auth/compute"
	//BigQueryScope GCP scope
	BigQueryScope = "https://www.googleapis.com/auth/bigquery"
	//BigQueryInsertScope GCP scope
	BigQueryInsertScope = "https://www.googleapis.com/auth/bigquery.insertdata"
)

func authRequest(ctx context.Context, httpRequest *http.Request) error {
	URL := metadataServerTokenURL + httpRequest.URL.String()
	jwtRequest, err := http.NewRequest(http.MethodGet, URL, nil)
	if err != nil {
		return err
	}
	jwtRequest.Header.Set("Metadata-Flavor", "Google")
	jwtRequest = jwtRequest.WithContext(ctx)
	response, err := http.DefaultClient.Do(jwtRequest)
	if err != nil {
		return errors.Wrapf(err, "failed to get token with %v", URL)
	}
	defer response.Body.Close()
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return errors.Wrapf(err, "failed to read token with %v", URL)
	}
	httpRequest.Header.Set("Authorization", fmt.Sprintf("Bearer %s", data))
	return err
}

func scopedHTTPClient(ctx context.Context, scopes ...string) (*http.Client, error) {
	scopes = append(scopes, CloudPlatformScope, DevstorageFullControlScope, ComputeCloudPlatformScope, BigQueryScope, BigQueryInsertScope)
	o := []option.ClientOption{
		option.WithScopes(scopes...),
		option.WithUserAgent(UserAgent),
	}
	httpClient, _, err := htransport.NewClient(ctx, o...)
	return httpClient, err
}
