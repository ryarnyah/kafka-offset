package util

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/sirupsen/logrus"
)

// GetTLSConfiguration build TLS configuration for kafka
func GetTLSConfiguration(caFile string, certFile string, keyFile string, insecure bool) (*tls.Config, bool, error) {
	logrus.Debugf("configure tls %s %s %s %t", caFile, certFile, keyFile, insecure)
	if (caFile == "" && (certFile == "" || keyFile == "")) && !insecure {
		return nil, false, nil
	}
	t := &tls.Config{}
	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, false, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		t.RootCAs = caCertPool
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, false, err
		}
		t.Certificates = []tls.Certificate{cert}
	}

	t.InsecureSkipVerify = insecure
	logrus.Debugf("TLS config %+v", t)

	return t, true, nil
}

// GetSASLConfiguration build SASL configuration for kafka
func GetSASLConfiguration(username string, password string) (string, string, bool) {
	if username != "" && password != "" {
		return username, password, true
	}
	return "", "", false
}
