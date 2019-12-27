package http

type JWTToken struct {
	Token string `json:"access_token"`
	Type  string `json:"token_type"`
}
