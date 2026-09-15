package x509util

import (
	"crypto/x509"
	"errors"
	"io/fs"
	"os"
)

// readCertFiles reads the PEM contents at caPath. If caPath is a directory, the
// contents of every file within it are returned.
func readCertFiles(caPath string) ([][]byte, error) {
	fi, err := os.Stat(caPath)
	if err != nil {
		return nil, err
	}

	if fi.IsDir() {
		return dirContents(caPath)
	}

	contents, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	return [][]byte{contents}, nil
}

// certPoolFromPEM builds a x509.CertPool out of PEM-encoded certificates.
func certPoolFromPEM(caFiles [][]byte) (*x509.CertPool, error) {
	certPool := x509.NewCertPool()
	for _, caBytes := range caFiles {
		if ok := certPool.AppendCertsFromPEM(caBytes); !ok {
			return nil, errors.New("failed to append certs from CA PEM")
		}
	}

	return certPool, nil
}

func dirContents(dirPath string) ([][]byte, error) {
	var allContents [][]byte
	dirFS := os.DirFS(dirPath)
	if err := fs.WalkDir(dirFS, ".", func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() {
			contents, err := fs.ReadFile(dirFS, d.Name())
			if err != nil {
				return err
			}
			allContents = append(allContents, contents)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return allContents, nil
}
