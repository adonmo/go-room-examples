package v2

//User Model for User Table
type User struct {
	ID   uint64
	Name string `json:"username"`
}
