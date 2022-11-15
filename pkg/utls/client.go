package utls

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"

	v1 "github.com/conductorone/baton-sdk/pb/c1/utls/v1"
)

// ClientConfig takes a credential and returns a TLS configuration that can be used to connect to a server.
func ClientConfig(ctx context.Context, cred *v1.Credential) (*tls.Config, error) {
	caCert, err := x509.ParseCertificate(cred.CaCert)
	if err != nil {
		return nil, err
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	// Validate that we have a valid certificate
	_, err = x509.ParseCertificate(cred.Cert)
	if err != nil {
		return nil, err
	}

	var tlsCert tls.Certificate

	tlsCert.Certificate = append(tlsCert.Certificate, cred.Cert)
	tlsCert.PrivateKey = ed25519.PrivateKey(cred.Key)

	return &tls.Config{
		ServerName:   "127.0.0.1",
		MinVersion:   tls.VersionTLS12,
		RootCAs:      pool,
		Certificates: []tls.Certificate{tlsCert},
	}, nil
}
