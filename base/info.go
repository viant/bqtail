package base

//Info represents meta rule information
type Info struct {
	Workflow     string `json:",omitempty"`
	Description  string `json:",omitempty"`
	ProjectURL   string `json:",omitempty"`
	LeadEngineer string `json:",omitempty"`
	URL          string `json:",omitempty"`
}
