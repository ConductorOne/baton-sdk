package uotel

import (
	"crypto/x509"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCert(t *testing.T) {
	cert := `-----BEGIN CERTIFICATE-----
MIIDLjCCAhagAwIBAgIOfxYG/VG/dh8jgFfwzUQwDQYJKoZIhvcNAQELBQAwJTEj
MCEGA1UEAxMab3RlbC1jb2xsZWN0b3IuYzEuaW50ZXJuYWwwHhcNMjUwMzI1MDIw
MjI1WhcNMzUwMzIzMDIwMjI1WjAlMSMwIQYDVQQDExpvdGVsLWNvbGxlY3Rvci5j
MS5pbnRlcm5hbDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALFvWVtd
JbzW2EM7+YK/4/YjQn9YC2czYaIs0mnJwq9plj1NXcZ1Hy/36ShfklzslNTARYRB
gxdloSo993Ig/xoPXndrc3G2/MfVNdr5So/x5CC6qJGbDflbiaC4iaK243UmowCX
wq+KmajKtPY7IBlg4jE4sJFvbQxNllyX2KNgDyFrdj7rwMq6zcdf7q+fbt0xXqQ+
QD0wF39/nvWqCO1udj0k58QejvRyP4/W0Qy8IIyF4jlGNU3aSENrskXHk83YRR4o
6Bqne8WZurRdNJ1ALt7moK11fjfcAWZNvzD65waLK5YtlOCG08q6kRaPnsXxJB6u
e+MQTDEBbOrUD/8CAwEAAaNcMFowDgYDVR0PAQH/BAQDAgWgMBMGA1UdJQQMMAoG
CCsGAQUFBwMBMAwGA1UdEwEB/wQCMAAwJQYDVR0RBB4wHIIab3RlbC1jb2xsZWN0
b3IuYzEuaW50ZXJuYWwwDQYJKoZIhvcNAQELBQADggEBACpIfWsWO8zd9UyWNQyz
RkH1CAY8p1Vnnl6qQdxLnt27OlksKCnXyFg14vp2JBhSTeq9xms+CWOFgtZSg/2c
zmz/VaGjA7gubV+9paDjhIr8k+geVYiKTYxmt+HjLT7Iz4FJPbXsnEuU7rEbB4U2
dK/JRQ0SBnFFBzkheUsPjzpezA1SD6TBPMIV/GTEH0Qf46fKIEFVKwZ5dvrWPTsk
P3ZnXkZhWMzLtF+hffj8esMizUAHDLE/RScPGKCd/TTnMN2xo7zXpy3QGSXClMCb
+KwmoYwi+OjqnZsyXXb9UR86mWsgCt6JAIDFuTE8LARN3QLvmT+PFoDniQwC8U8R
hFE=
-----END CERTIFICATE-----`

	encodedCert := base64.RawURLEncoding.EncodeToString([]byte(cert))
	t.Logf("Base64 Encoded Certificate:\n%s", encodedCert)

	decodedCert, err := base64.RawURLEncoding.DecodeString(encodedCert)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Decoded Certificate:\n%s", decodedCert)

	require.Equal(t, cert, string(decodedCert))

	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(decodedCert)
	require.True(t, ok)
}
