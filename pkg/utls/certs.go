package utls

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/utls/v1"
)

func serialNumber() (*big.Int, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, err
	}

	return serialNumber, nil
}

func generateCredential(ctx context.Context, name string, caCert *x509.Certificate, caPrivKey ed25519.PrivateKey) (*v1.Credential, error) {
	serial, err := serialNumber()
	if err != nil {
		return nil, err
	}

	x509cert := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:         name,
			Country:            []string{"US"},
			Province:           []string{"OR"},
			Locality:           []string{"Portland"},
			Organization:       []string{"ConductorOne"},
			OrganizationalUnit: []string{"connector sdk"},
		},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(1, 0, 0),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature,
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}

	signedCert, err := x509.CreateCertificate(rand.Reader, x509cert, caCert, publicKey, caPrivKey)
	if err != nil {
		return nil, err
	}

	return &v1.Credential{
		Key:  privateKey,
		Cert: signedCert,
	}, nil
}

// GenerateClientServerCredentials generates a new CA and two sets of credentials for use in a client/server configuration.
// Returns: client_credentials, server_credentials, error.
func GenerateClientServerCredentials(ctx context.Context) (*v1.Credential, *v1.Credential, error) {
	_, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	caSerial, err := serialNumber()
	if err != nil {
		return nil, nil, err
	}

	ca := &x509.Certificate{
		SerialNumber: caSerial,
		Subject: pkix.Name{
			CommonName:         "c1-connector-ca",
			Country:            []string{"US"},
			Province:           []string{"OR"},
			Locality:           []string{"Portland"},
			Organization:       []string{"ConductorOne"},
			OrganizationalUnit: []string{"connector sdk"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().AddDate(20, 0, 0),
		IsCA:      true,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caCert, err := x509.CreateCertificate(rand.Reader, ca, ca, caKey.Public(), caKey)
	if err != nil {
		return nil, nil, err
	}

	clientCreds, err := generateCredential(ctx, "c1-connector-client", ca, caKey)
	if err != nil {
		return nil, nil, err
	}
	clientCreds.CaCert = caCert

	serverCreds, err := generateCredential(ctx, "c1-connector-server", ca, caKey)
	if err != nil {
		return nil, nil, err
	}
	serverCreds.CaCert = caCert

	return clientCreds, serverCreds, nil
}
