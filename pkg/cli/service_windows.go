//go:build windows

package cli

import (
	"golang.org/x/sys/windows/svc"
)

func IsService() bool {
	if ok, _ := svc.IsWindowsService(); ok {
		return true
	}
	return false
}
