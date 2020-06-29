package v2

//User Model for User Table
type User struct {
	ID   int
	Name string `json:"username"`
}

//GetSampleData Namesake
func GetSampleData() []interface{} {
	return []interface{}{}
}
