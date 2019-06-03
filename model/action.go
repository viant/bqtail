package model

const (
	ActionDelete = "delete"
	ActionMove   = "move"
)

type Action struct {
	Name    string //empty Delete,Move
	DestURL string

	//TODO add support for the following
	/*
		Email        string
		SlackChannel string
		Message      string
	*/
}
