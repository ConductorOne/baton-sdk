//go:build windows
// +build windows

package cli

import (
	"golang.org/x/sys/windows/svc"
)

func IsService() bool {
	return svc.IsWindowsService()
}
