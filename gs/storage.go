package gs

import (
	"bqtail/model"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/viant/toolbox/storage"
	_ "github.com/viant/toolbox/storage/gs"
	"io"
	"io/ioutil"
	"strings"
)

//OpenURL open url with supplied storage service
func OpenURL(URL string) (io.ReadCloser, error) {
	service, err := storage.NewServiceForURL(URL, "")
	if err != nil {
		return nil, err
	}
	reader, err := service.DownloadWithURL(URL)
	if err != nil {
		return nil, fmt.Errorf("unable to download (%T) '%v', %v", service, URL, err)
	}
	if strings.HasSuffix(URL, ".gz") {
		data, err := ioutil.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return nil, fmt.Errorf("unable to read %v, %v", URL, err)
		}
		gzReader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("unable to open gzip: %v, %v", URL, err)
		}
		return gzReader, nil
	}
	return reader, err
}

//Move removes asset for specified URL
func Move(sourceURL, destURL string) error {
	reader, err := OpenURL(sourceURL)
	if err != nil {
		return err
	}
	return Upload(destURL, reader)
}

//DeleteURL removes asset for specified URL
func DeleteURL(URL string) error {
	service, err := storage.NewServiceForURL(URL, "")
	if err != nil {
		return err
	}
	object, err := service.StorageObject(URL)
	if err != nil {
		return err
	}
	return service.Delete(object)
}

//Upload upload content to the URL
func Upload(URL string, reader io.Reader) error {
	service, err := storage.NewServiceForURL(URL, "")
	if err != nil {
		return err
	}
	return service.Upload(URL, reader)
}

//Get returns object for supplied URL
func Get(URL string) (*model.Datafile, error) {
	service, err := storage.NewServiceForURL(URL, "")
	if err != nil {
		return nil, err
	}
	object, err := service.StorageObject(URL)
	if err != nil {
		return nil, err
	}
	return &model.Datafile{URL: object.URL(), Modified: object.FileInfo().ModTime()}, nil
}

//List lists content object for supplied URL
func List(URL string) ([]*model.Datafile, error) {
	service, err := storage.NewServiceForURL(URL, "")
	if err != nil {
		return nil, err
	}
	objects, err := service.List(URL)
	if err != nil {
		return nil, err
	}

	var result = make([]*model.Datafile, 0)
	for _, object := range objects {
		if object.IsFolder() {
			continue
		}
		result = append(result, &model.Datafile{URL: object.URL(), Modified: object.FileInfo().ModTime()})
	}
	return result, nil
}
