package bqtail

import (
	"bqtail/gs"
	"bqtail/model"
	"fmt"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/url"
	"path"
	"strings"
)

func handleAction(datafile *model.Datafile, action *model.Action) error {
	switch strings.ToLower(action.Name) {
	case "":
		return nil
	case model.ActionDelete:
		return gs.DeleteURL(datafile.URL)
	case model.ActionMove:
		destURL := formatDestURL(datafile.URL, action.DestURL)
		return gs.Move(datafile.URL, destURL)
	default:
		return fmt.Errorf("unknown action: %v", action.Name)
	}
}

func handlePostAction(datafile *model.Datafile, err error) error {
	toolbox.Dump(datafile)
	if err != nil {
		if datafile.OnFailure == nil {
			return err
		}
		if e := handleAction(datafile, datafile.OnFailure); e != nil {
			return fmt.Errorf("%v, %v", e, err)
		}
		return err

	} else if datafile.OnSuccess == nil {
		return nil
	}
	return handleAction(datafile, datafile.OnSuccess)
}

func formatDestURL(assetURL, destRootURL string) string {
	rest, name := toolbox.URLSplit(assetURL)
	_, parent := toolbox.URLSplit(rest)
	return url.NewResource(toolbox.URLPathJoin(destRootURL, path.Join(parent, name))).URL
}
