package http

//JWTToken represents jwt token
type JWTToken struct {
	Token string `json:"access_token"`
	Type  string `json:"token_type"`
}
