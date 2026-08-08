package expand

import "os"

func fullTestSuite() bool {
	return os.Getenv("BATON_FULL_TESTS") != ""
}
