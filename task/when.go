package task

//When conditional action exection
type When struct {
	//Exists if specified action would run only when table exists
	Exists string `json:",omitempty"`
	//GroupDone run only if group is done
	GroupDone bool   `json:",omitempty"`
	GroupURL  string `json:",omitempty"`
}
