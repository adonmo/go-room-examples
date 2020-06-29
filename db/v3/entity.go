package v3

//User Model for User Table
type User struct {
	ID    int
	Name  string `json:"username"`
	Score int
}

//GetSampleData Namesake
func GetSampleData() []interface{} {
	return []interface{}{}
}
