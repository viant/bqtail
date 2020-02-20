package config

//Schema represents schema
type Schema struct {
	//Template destination table template, when specified destination table will be created if it does not exists
	Template   string
	Autodetect bool
	Split      *Split
}
