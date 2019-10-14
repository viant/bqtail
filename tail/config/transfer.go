package config

//Batch transfer config
type Batch struct {
	Window   *Window
	BatchURL string
}

//Init initialises batch mode
func (t *Batch) Init(batchURL string) {
	if t.Window == nil {
		t.Window = &Window{}
	}
	t.Window.Init()
	if t.BatchURL == "" {
		t.BatchURL = batchURL
	}
}

//Validate checks if batch configuration is valid
func (t *Batch) Validate() error {
	return t.Window.Validate()
}
