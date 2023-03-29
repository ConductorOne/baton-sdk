package ugrpc

import (
	"fmt"
	"net"
)

func HostPort(address string, port string) string {
	return fmt.Sprintf("%s:///%s", "dns", net.JoinHostPort(address, port))
}
