package grpcutil

import (
	"crypto/x509"
	"errors"
	"io/fs"
	"os"
)

// customCertPool creates a x509.CertPool from a filepath string.
//
// If the path is a directory, it walks the directory and adds all files to the
// pool.
func customCertPool(caPath string) (*x509.CertPool, error) {
	fi, err := os.Stat(caPath)
	if err != nil {
		return nil, err
	}

	var caFiles [][]byte
	if fi.IsDir() {
		caFiles, err = dirContents(caPath)
		if err != nil {
			return nil, err
		}
	} else {
		contents, err := os.ReadFile(caPath)
		if err != nil {
			return nil, err
		}
		caFiles = append(caFiles, contents)
	}

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
