package v3

//User Model for User Table
type User struct {
	ID    uint64
	Name  string `json:"username"`
	Score int
}
