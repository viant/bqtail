package task

//When conditional action exection
type When struct {
	//Exists if specified action would run only when table exists
	Exists string `json:",omitempty"`
}
