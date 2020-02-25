package auth

import (
	"context"
	cloudresourcemanager "google.golang.org/api/cloudresourcemanager/v1beta1"
	goption "google.golang.org/api/option"
	"net/http"
	"strings"
)

//SelectProjectID selector project for auth user
func SelectProjectID(ctx context.Context, client *http.Client, scopes ...string) (string, error) {
	service, err := cloudresourcemanager.NewService(ctx, goption.WithHTTPClient(client), goption.WithScopes(scopes...))
	if err != nil {
		return "", err
	}
	var projects = []string{}
	pageToken := ""
	for {
		call := service.Projects.List()
		call.PageToken(pageToken)
		call.Context(ctx)
		list, err := call.Do()
		if err != nil {
			return "", err
		}
		if len(list.Projects) == 0 {
			break
		}
		for _, prj := range list.Projects {
			projects = append(projects, prj.Name)
		}
		if pageToken = list.NextPageToken; pageToken == "" {
			break
		}
	}

	if project := matchProject(projects, "e2e"); project != "" {
		return project, nil
	}
	if project := matchProject(projects, "test"); project != "" {
		return project, nil
	}
	if project := matchProject(projects, "poc"); project != "" {
		return project, nil
	}
	return projects[0], nil
}

func matchProject(candidates []string, fragmentMatch string) string {
	for _, candidate := range candidates {
		if strings.Contains(candidate, fragmentMatch) {
			return candidate
		}
	}
	return ""
}
