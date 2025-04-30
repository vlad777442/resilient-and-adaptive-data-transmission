package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/quic-go/quic-go"
)

const bufSize = 1024 * 16

func main() {
	outputDir := flag.String("output", ".", "Output directory for received files")
	port := flag.Int("port", 4242, "Port to listen on")
	flag.Parse()

	// Create the output directory if it doesn't exist
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		fmt.Printf("Failed to create output directory: %v\n", err)
		return
	}

	// Generate a self-signed certificate for TLS
	cert, err := generateTLSConfig()
	if err != nil {
		fmt.Printf("Failed to generate TLS certificate: %v\n", err)
		return
	}

	// Configure QUIC transport
	listener, err := quic.ListenAddr(
		fmt.Sprintf("0.0.0.0:%d", *port),
		cert,
		&quic.Config{},
	)
	if err != nil {
		fmt.Printf("Failed to start QUIC server: %v\n", err)
		return
	}

	fmt.Printf("QUIC file server started on :%d\n", *port)
	fmt.Printf("Files will be saved to: %s\n", *outputDir)

	for {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}

		fmt.Printf("Connection established from: %s\n", conn.RemoteAddr())

		// Handle each connection in a goroutine
		go func(conn quic.Connection) {
			for {
				stream, err := conn.AcceptStream(context.Background())
				if err != nil {
					fmt.Printf("Failed to accept stream: %v\n", err)
					return
				}

				go handleStream(stream, *outputDir)
			}
		}(conn)
	}
}

func handleStream(stream quic.Stream, outputDir string) {
	defer stream.Close()

	// First, read the filename
	var filenameLen uint16
	if err := binary.Read(stream, binary.BigEndian, &filenameLen); err != nil {
		fmt.Printf("Failed to read filename length: %v\n", err)
		return
	}

	filenameBytes := make([]byte, filenameLen)
	if _, err := io.ReadFull(stream, filenameBytes); err != nil {
		fmt.Printf("Failed to read filename: %v\n", err)
		return
	}
	filename := string(filenameBytes)

	// Create the destination file
	destPath := filepath.Join(outputDir, filepath.Base(filename))
	fmt.Printf("Receiving file: %s\n", destPath)

	destFile, err := os.Create(destPath)
	if err != nil {
		fmt.Printf("Failed to create destination file: %v\n", err)
		return
	}
	defer destFile.Close()

	// Copy data from the QUIC stream to the file
	n, err := io.Copy(destFile, stream)
	if err != nil {
		fmt.Printf("Error while receiving file: %v\n", err)
		return
	}

	fmt.Printf("Successfully received %s (%d bytes)\n", filename, n)
}

// generateTLSConfig creates a self-signed certificate for the server
func generateTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * 365 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"quic-file-transfer"},
	}, nil
}
