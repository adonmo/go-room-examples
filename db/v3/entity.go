package v3

//User Model for User Table
type User struct {
	ID    int
	Name  string `json:"username"`
	Score int
}
