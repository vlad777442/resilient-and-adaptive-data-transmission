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
	inputPath := flag.String("input", "", "Path to file(s) to send (can be a directory or specific file)")
	verbose := flag.Bool("verbose", false, "Show detailed transfer statistics")
	flag.Parse()

	if *inputPath == "" {
		fmt.Println("Please specify a file or directory with -input")
		return
	}

	// Check if the input path exists
	inputInfo, err := os.Stat(*inputPath)
	if err != nil {
		fmt.Printf("Failed to get info for input path: %v\n", err)
		return
	}

	// Build a list of files to transfer
	var filesToTransfer []string
	if inputInfo.IsDir() {
		// If directory, get all files in the directory
		entries, err := os.ReadDir(*inputPath)
		if err != nil {
			fmt.Printf("Failed to read directory: %v\n", err)
			return
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				filesToTransfer = append(filesToTransfer, filepath.Join(*inputPath, entry.Name()))
			}
		}

		if len(filesToTransfer) == 0 {
			fmt.Println("No files found in the specified directory")
			return
		}
	} else {
		// If file, just add it to the list
		filesToTransfer = append(filesToTransfer, *inputPath)
	}

	// Configure TLS
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Skip certificate verification in this example
		NextProtos:         []string{"quic-file-transfer"},
	}

	// Create a context for the connection
	ctx := context.Background()

	// Connect to the server
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

	totalBytes := int64(0)
	totalStartTime := time.Now()
	transferTimes := make([]time.Duration, 0, len(filesToTransfer))

	// Send each file
	for i, filePath := range filesToTransfer {
		fmt.Printf("[%d/%d] Processing file: %s\n", i+1, len(filesToTransfer), filePath)

		fileStartTime := time.Now()
		fileInfo, _ := os.Stat(filePath)
		fileSize := fileInfo.Size()

		if err := sendFile(ctx, conn, filePath, *verbose); err != nil {
			fmt.Printf("Error sending file %s: %v\n", filePath, err)
			continue
		}

		fileDuration := time.Since(fileStartTime)
		transferTimes = append(transferTimes, fileDuration)
		totalBytes += fileSize

		if *verbose {
			fileRate := float64(fileSize) / 1024 / 1024 / fileDuration.Seconds()
			fmt.Printf("  File transfer time: %v (%.2f MB/s)\n", fileDuration, fileRate)
		}
	}

	totalDuration := time.Since(totalStartTime)
	rate := float64(totalBytes) / 1024 / 1024 / totalDuration.Seconds()

	// Calculate min, max, and average transfer durations
	var minTime, maxTime, totalTime time.Duration
	if len(transferTimes) > 0 {
		minTime = transferTimes[0]
		maxTime = transferTimes[0]
		totalTime = 0

		for _, t := range transferTimes {
			totalTime += t
			if t < minTime {
				minTime = t
			}
			if t > maxTime {
				maxTime = t
			}
		}
	}

	// Print summary
	fmt.Printf("\n=== Transfer Summary ===\n")
	fmt.Printf("Files transferred: %d\n", len(transferTimes))
	fmt.Printf("Total data: %.2f MB\n", float64(totalBytes)/(1024*1024))
	fmt.Printf("Total time: %v\n", totalDuration)
	fmt.Printf("Overall throughput: %.2f MB/s\n", rate)

	if len(transferTimes) > 0 {
		avgTime := totalTime / time.Duration(len(transferTimes))
		fmt.Printf("\nFile transfer statistics:\n")
		fmt.Printf("  Min time: %v\n", minTime)
		fmt.Printf("  Max time: %v\n", maxTime)
		fmt.Printf("  Avg time: %v\n", avgTime)
	}
}

func sendFile(ctx context.Context, conn quic.Connection, filePath string, verbose bool) error {
	// Open the file to send
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %v", err)
	}

	// Create a stream for this file transfer
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("failed to open stream: %v", err)
	}
	defer stream.Close()

	// Send the filename first
	filename := filepath.Base(filePath)
	filenameBytes := []byte(filename)

	// Write filename length as uint16
	if err := binary.Write(stream, binary.BigEndian, uint16(len(filenameBytes))); err != nil {
		return fmt.Errorf("failed to send filename length: %v", err)
	}

	// Write filename
	if _, err := stream.Write(filenameBytes); err != nil {
		return fmt.Errorf("failed to send filename: %v", err)
	}

	// Send the file size as int64
	if err := binary.Write(stream, binary.BigEndian, fileInfo.Size()); err != nil {
		return fmt.Errorf("failed to send file size: %v", err)
	}

	fmt.Printf("Sending file: %s (%d bytes)\n", filename, fileInfo.Size())

	// Track detailed timing
	startTime := time.Now()
	setupTime := time.Since(startTime)

	// Time spent in various phases
	dataStartTime := time.Now()

	// Send the file data
	n, err := io.Copy(stream, file)
	if err != nil {
		return fmt.Errorf("failed to send file: %v", err)
	}

	dataTransferTime := time.Since(dataStartTime)
	totalTime := time.Since(startTime)

	rate := float64(n) / 1024 / 1024 / dataTransferTime.Seconds()

	if verbose {
		fmt.Printf("  Setup time: %v\n", setupTime)
		fmt.Printf("  Data transfer time: %v\n", dataTransferTime)
		fmt.Printf("  Total transfer time: %v\n", totalTime)
	}

	fmt.Printf("Successfully sent %d bytes (%.2f MB/s)\n", n, rate)

	return nil
}
