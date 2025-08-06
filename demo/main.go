package main

import (
	"crypto/sha512"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"golang.org/x/sys/unix"
)

func main() {
	if len(os.Args) < 3 || len(os.Args) > 4 {
		fmt.Println("Usage: go run main.go <file_path> <wait_seconds> [--old-logic|--reopen]")
		fmt.Println("  --old-logic: Use old logic (direct cleanup after close)")
		fmt.Println("  --reopen: Use goroutine with reopen file (no dup)")
		fmt.Println("  (no option): Use NEW logic (goroutine with duplicated fd)")
		os.Exit(1)
	}

	filePath := os.Args[1]
	waitSeconds, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal("Invalid wait seconds:", err)
	}

	var logicType string
	if len(os.Args) == 4 {
		logicType = os.Args[3]
	} else {
		logicType = "new"
	}

	switch logicType {
	case "--old-logic":
		fmt.Println("Using OLD logic: direct cleanup after close")
	case "--reopen":
		fmt.Println("Using REOPEN logic: goroutine cleanup with reopened file")
	default:
		fmt.Println("Using NEW logic: goroutine cleanup with duplicated fd")
	}

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Failed to open file:", err)
	}

	// Get original file descriptor
	originalFd := int(file.Fd())
	fmt.Printf("Original file descriptor: %d\n", originalFd)

	switch logicType {
	case "--old-logic":
		// OLD LOGIC: Direct cleanup after close
		// Main process: calculate SHA512
		fmt.Println("Calculating SHA512...")
		hash := sha512.New()

		// Read file in chunks to ensure page cache is populated
		buffer := make([]byte, 64*1024) // 64KB chunks
		for {
			n, err := file.Read(buffer)
			if n > 0 {
				hash.Write(buffer[:n])
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("Error reading file:", err)
			}
		}

		result := hash.Sum(nil)
		fmt.Printf("SHA512: %x\n", result)

		// Close file first
		file.Close()
		fmt.Println("File closed")

		// Wait for specified time
		time.Sleep(time.Duration(waitSeconds) * time.Second)

		// Reopen file for cleanup
		cleanupFile, err := os.Open(filePath)
		if err != nil {
			log.Printf("Failed to reopen file for cleanup: %v", err)
			return
		}
		defer cleanupFile.Close()

		fmt.Printf("Reopened file descriptor: %d\n", int(cleanupFile.Fd()))

		// Clean page cache using reopened fd
		err = unix.Fadvise(int(cleanupFile.Fd()), 0, 0, unix.FADV_DONTNEED)
		if err != nil {
			log.Printf("Failed to clean page cache: %v", err)
		} else {
			fmt.Println("Page cache cleaned successfully (OLD logic)")
		}

	case "--reopen":
		// REOPEN LOGIC: Goroutine cleanup with reopened file
		// Start goroutine to reopen file and clean page cache
		go func() {
			// Wait for specified time
			time.Sleep(time.Duration(waitSeconds) * time.Second)

			// Reopen file for cleanup
			cleanupFile, err := os.Open(filePath)
			if err != nil {
				log.Printf("Failed to reopen file for cleanup: %v", err)
				return
			}
			defer cleanupFile.Close()

			fmt.Printf("Reopened file descriptor: %d\n", int(cleanupFile.Fd()))

			// Clean page cache using reopened fd
			err = unix.Fadvise(int(cleanupFile.Fd()), 0, 0, unix.FADV_DONTNEED)
			if err != nil {
				log.Printf("Failed to clean page cache: %v", err)
			} else {
				fmt.Println("Page cache cleaned successfully (REOPEN logic)")
			}
		}()

		// Main process: calculate SHA512
		fmt.Println("Calculating SHA512...")
		hash := sha512.New()

		// Read file in chunks to ensure page cache is populated
		buffer := make([]byte, 64*1024) // 64KB chunks
		for {
			n, err := file.Read(buffer)
			if n > 0 {
				hash.Write(buffer[:n])
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("Error reading file:", err)
			}
		}

		result := hash.Sum(nil)
		fmt.Printf("SHA512: %x\n", result)

		// Close file
		file.Close()
		fmt.Println("File closed")

		// Wait for goroutine to complete
		time.Sleep(time.Duration(waitSeconds+1) * time.Second)

	default:
		// NEW LOGIC: Goroutine cleanup with duplicated fd
		// Start goroutine to copy fd and clean page cache
		go func() {
			// Duplicate the file descriptor first
			newFd, err := unix.Dup(originalFd)
			if err != nil {
				log.Printf("Failed to duplicate fd: %v", err)
				return
			}
			defer unix.Close(newFd)

			fmt.Printf("Duplicated file descriptor: %d\n", newFd)

			// Wait for specified time
			time.Sleep(time.Duration(waitSeconds) * time.Second)

			// Clean page cache using the duplicated fd
			err = unix.Fadvise(newFd, 0, 0, unix.FADV_DONTNEED)
			if err != nil {
				log.Printf("Failed to clean page cache: %v", err)
			} else {
				fmt.Println("Page cache cleaned successfully (NEW logic)")
			}
		}()

		// Main process: calculate SHA512
		fmt.Println("Calculating SHA512...")
		hash := sha512.New()

		// Read file in chunks to ensure page cache is populated
		buffer := make([]byte, 64*1024) // 64KB chunks
		for {
			n, err := file.Read(buffer)
			if n > 0 {
				hash.Write(buffer[:n])
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("Error reading file:", err)
			}
		}

		result := hash.Sum(nil)
		fmt.Printf("SHA512: %x\n", result)

		// Close file
		file.Close()
		fmt.Println("File closed")

		// Wait for goroutine to complete
		time.Sleep(time.Duration(waitSeconds+1) * time.Second)
	}

	fmt.Println("Program completed")
}
