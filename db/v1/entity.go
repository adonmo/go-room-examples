package v1

//User Model for User Table
type User struct {
	ID   int
	Name string
}

//GetSampleData Namesake
func GetSampleData() []interface{} {
	return []interface{}{
		&User{
			Name: "Tom",
		},
		&User{
			Name: "Dick",
		},
		&User{
			Name: "Harry",
		},
	}
}
