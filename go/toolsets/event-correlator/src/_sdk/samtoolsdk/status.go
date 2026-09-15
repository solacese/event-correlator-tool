package samtoolsdk

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"
)

// statusWriter writes status messages to the STR's named pipe.
// Messages are newline-delimited JSON matching the Python SDK protocol.
type statusWriter struct {
	pipePath string
	mu       sync.Mutex
	file     *os.File
}

// statusMessage is the JSON format written to the status pipe.
type statusMessage struct {
	Status string `json:"status"`
}

// newStatusWriter creates a status writer for the given pipe path.
// The pipe is opened lazily on the first write.
func newStatusWriter(pipePath string) *statusWriter {
	return &statusWriter{pipePath: pipePath}
}

// Send writes a status message to the pipe.
func (sw *statusWriter) Send(message string) error {
	if sw.pipePath == "" {
		return nil
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Open lazily on first write. Use non-blocking mode with retry to avoid
	// hanging if the STR reader hasn't opened the pipe yet (race condition).
	if sw.file == nil {
		f, err := sw.openPipeWithRetry()
		if err != nil {
			// If we can't open after retries, silently skip status updates
			// rather than blocking the tool execution.
			return nil
		}
		sw.file = f
	}

	data, err := json.Marshal(statusMessage{Status: message})
	if err != nil {
		return fmt.Errorf("marshaling status: %w", err)
	}
	data = append(data, '\n')

	_, err = sw.file.Write(data)
	return err
}

// openPipeWithRetry attempts to open the pipe with non-blocking mode.
// If no reader is available (ENXIO), it retries a few times before giving up.
// This handles the race condition where the subprocess starts before the
// STR reader goroutine has opened the pipe.
func (sw *statusWriter) openPipeWithRetry() (*os.File, error) {
	const maxRetries = 5
	const retryDelay = 50 * time.Millisecond

	for i := range maxRetries {
		fd, err := syscall.Open(sw.pipePath, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			return os.NewFile(uintptr(fd), sw.pipePath), nil
		}

		// ENXIO means no reader has opened the pipe yet. Retry after a delay.
		if err == syscall.ENXIO {
			if i < maxRetries-1 {
				time.Sleep(retryDelay)
				continue
			}
		}

		return nil, fmt.Errorf("opening status pipe: %w", err)
	}

	return nil, fmt.Errorf("status pipe reader not available after %d retries", maxRetries)
}

// Close closes the pipe file handle.
func (sw *statusWriter) Close() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.file != nil {
		err := sw.file.Close()
		sw.file = nil
		return err
	}
	return nil
}
