package resource

import "strings"

// SplitFullName looks for the first ` `, everything to the left is first name,
// everything to the right is last name.
func SplitFullName(name string) (string, string) {
	names := strings.SplitN(name, " ", 2)
	var firstName, lastName string

	switch len(names) {
	case 1:
		firstName = names[0]
	case 2:
		firstName = names[0]
		lastName = names[1]
	}

	return firstName, lastName
}
