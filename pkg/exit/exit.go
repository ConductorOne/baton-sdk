package exit

import (
	"context"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Exit exits the program with a code based on the error.
// Exit codes correspond to GRPC status codes.
// If err is nil, exit code is 0.
// Common errors such as context cancelled and deadline exceeded exit with the corresponding GRPC status code.
// Other errors exit with code 2, which is GRPC status code Unknown.
func Exit(err error) {
	os.Exit(exitCode(err))
}

// LogExit logs the error to stderr & calls Exit(), which exits the program with a code based on the error.
func LogExit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}
	Exit(err)
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}

	if grpcErr, ok := status.FromError(err); ok {
		if grpcErr.Code() == codes.OK {
			// An error with code OK should never happen.
			return int(codes.Internal)
		}
		return int(grpcErr.Code())
	}

	if errors.Is(err, context.Canceled) {
		return int(codes.Canceled)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return int(codes.DeadlineExceeded)
	}

	// Otherwise, exit with code 2, which is GRPC status code Unknown.
	return int(codes.Unknown)
}
