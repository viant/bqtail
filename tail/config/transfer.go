package config

//Batch transfer config
type Batch struct {
	Window   *Window
	StageURL string
}

//Init initialises batch mode
func (t *Batch) Init() {
	if t.Window == nil {
		t.Window = &Window{}
	}
	t.Window.Init()
}

//Validate checks if batch configuration is valid
func (t *Batch) Validate() error {
	return t.Window.Validate()
}
