package resource

import "strings"

// SplitFullName looks for the first ` `, everything to the left is first name,
// everything to the right is last name.
func SplitFullName(name string) (string, string) {
	names := strings.SplitN(name, " ", 2)
	var firstName, lastName string
	if len(names) > 0 {
		firstName = names[0]
	}
	if len(names) > 1 {
		lastName = names[1]
	}

	return firstName, lastName
}
