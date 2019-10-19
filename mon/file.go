package mon

import "time"

//File represents workflow file
type File struct {
	URL      string
	Age      string
	Modified time.Time
	Size     int
}
