package http

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"io/ioutil"
	"net/http"
	"strings"
)

func (s *service) Call(ctx context.Context, request *CallRequest) (*CallResponse, error) {
	if request.URL == "" {
		return nil, errors.Errorf("request.URL was empty")
	}
	var err error
	httpClient := http.DefaultClient
	if len(request.Scopes) > 0 {
		request.Auth = false
		if httpClient, err = scopedHTTPClient(ctx, request.Scopes...); err != nil {
			return nil, err
		}
	}
	if request.Method == "" {
		request.Method = http.MethodGet
	}

	httpRequest, err := http.NewRequest(strings.ToUpper(request.Method), request.URL, strings.NewReader(request.Body))
	if err != nil {
		return nil, err
	}
	if request.Auth {
		if err = authRequest(ctx, httpRequest); err != nil {
			return nil, err
		}
	}
	httpResponse, err := httpClient.Do(httpRequest)
	if httpResponse == nil {
		return nil, errors.Wrapf(err, "failed to %v: %v", request.Method, request.URL)
	}
	resp := &CallResponse{
		StatusCode: httpResponse.StatusCode,
		Headers:    httpResponse.Header,
	}

	if httpResponse.Body != nil {
		body, err := ioutil.ReadAll(httpResponse.Body)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read http response	 body: %v", httpResponse.StatusCode)
		}
		resp.Body = string(body)
		if json.Valid(body) {
			_ = json.Unmarshal(body, &resp.Data)
		}
		_ = httpResponse.Body.Close()
	}
	if base.IsLoggingEnabled() {
		base.Log(resp)
	}
	return resp, nil
}

//CallRequest represents an http call request
type CallRequest struct {
	URL     string
	Method  string
	BodyURL string
	Body    string
	//Auth authenticate to call non public cloud function
	Auth bool
	//Scopes for OAuth HTTP client
	Scopes []string
}

//CallResponse represents an http call response
type CallResponse struct {
	StatusCode int
	Headers    http.Header
	Data       interface{} ///JSON inferred response data
	Body       string
}
