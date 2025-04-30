package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/quic-go/quic-go"
)

const bufSize = 1024 * 16

func main() {
	server := flag.String("server", "localhost:4242", "Server address (host:port)")
	filePath := flag.String("file", "", "Path to file to send")
	flag.Parse()

	if *filePath == "" {
		fmt.Println("Please specify a file to send with -file")
		return
	}

	// Open the file to send
	file, err := os.Open(*filePath)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("Failed to get file info: %v\n", err)
		return
	}

	// Configure TLS
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Skip certificate verification in this example
		NextProtos:         []string{"quic-file-transfer"},
	}

	// Create a context for the connection
	ctx := context.Background()

	// Connect to the server - note the ctx parameter is now the first argument
	conn, err := quic.DialAddr(
		ctx,
		*server,
		tlsConfig,
		&quic.Config{},
	)
	if err != nil {
		fmt.Printf("Failed to connect to server: %v\n", err)
		return
	}
	defer conn.CloseWithError(0, "client closed connection")

	fmt.Printf("Connected to server: %s\n", *server)

	// Create a stream for the file transfer
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		fmt.Printf("Failed to open stream: %v\n", err)
		return
	}
	defer stream.Close()

	// Send the filename first
	filename := filepath.Base(*filePath)
	filenameBytes := []byte(filename)

	// Write filename length as uint16
	if err := binary.Write(stream, binary.BigEndian, uint16(len(filenameBytes))); err != nil {
		fmt.Printf("Failed to send filename length: %v\n", err)
		return
	}

	// Write filename
	if _, err := stream.Write(filenameBytes); err != nil {
		fmt.Printf("Failed to send filename: %v\n", err)
		return
	}

	fmt.Printf("Sending file: %s (%d bytes)\n", filename, fileInfo.Size())

	// Start time for transfer rate calculation
	startTime := time.Now()

	// Send the file data
	n, err := io.Copy(stream, file)
	if err != nil {
		fmt.Printf("Failed to send file: %v\n", err)
		return
	}

	duration := time.Since(startTime)
	rate := float64(n) / 1024 / 1024 / duration.Seconds()

	fmt.Printf("Successfully sent %d bytes in %v (%.2f MB/s)\n", n, duration, rate)
}
