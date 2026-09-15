package samtoolsdk

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

// ipcClient communicates with the STR's IPC server over a Unix domain socket.
// Used for LLM callbacks from within tool subprocesses.
type ipcClient struct {
	conn   net.Conn
	reader *bufio.Reader
	mu     sync.Mutex // serializes request/response pairs
	nextID atomic.Int64
}

// ipcRequest is a JSON-RPC 2.0 request sent to the STR.
type ipcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

// ipcResponse is a JSON-RPC 2.0 response from the STR.
type ipcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      string          `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *ipcError       `json:"error,omitempty"`
}

type ipcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// LLM call request/response types.

type llmCallParams struct {
	SystemPrompt string  `json:"system_prompt"`
	UserPrompt   string  `json:"user_prompt"`
	Temperature  float64 `json:"temperature"`
}

type llmCallResult struct {
	Text string `json:"text"`
}

// newIPCClient connects to the STR's IPC Unix socket.
func newIPCClient(socketPath string) (*ipcClient, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("connecting to IPC socket %s: %w", socketPath, err)
	}
	return &ipcClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}, nil
}

// CallLLM sends an LLM call request to the STR and returns the response text.
func (c *ipcClient) CallLLM(ctx context.Context, systemPrompt, userPrompt string, temperature float64) (string, error) {
	id := fmt.Sprintf("%d", c.nextID.Add(1))

	req := ipcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  "llm/call",
		Params: llmCallParams{
			SystemPrompt: systemPrompt,
			UserPrompt:   userPrompt,
			Temperature:  temperature,
		},
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Marshal and send request with newline delimiter.
	data, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("marshaling IPC request: %w", err)
	}
	data = append(data, '\n')

	if _, writeErr := c.conn.Write(data); writeErr != nil {
		return "", fmt.Errorf("writing IPC request: %w", writeErr)
	}

	// Read response line.
	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		return "", fmt.Errorf("reading IPC response: %w", err)
	}

	var resp ipcResponse
	if unmarshalErr := json.Unmarshal(line, &resp); unmarshalErr != nil {
		return "", fmt.Errorf("unmarshaling IPC response: %w", unmarshalErr)
	}

	if resp.Error != nil {
		return "", fmt.Errorf("IPC error %d: %s", resp.Error.Code, resp.Error.Message)
	}

	var result llmCallResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return "", fmt.Errorf("unmarshaling LLM result: %w", err)
	}

	return result.Text, nil
}

// Close closes the IPC connection.
func (c *ipcClient) Close() error {
	return c.conn.Close()
}
