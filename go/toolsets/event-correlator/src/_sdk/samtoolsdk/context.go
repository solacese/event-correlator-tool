package samtoolsdk

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
)

// VolumeMount describes a volume mounted into the tool's execution environment.
type VolumeMount struct {
	VolumeID  string `json:"volume_id"`
	MountPath string `json:"mount_path"`
	ReadOnly  bool   `json:"read_only"`
}

// ToolContext provides tools with access to the execution environment.
// It is injected as the third parameter to handler functions.
type ToolContext struct {
	// UserID is the authenticated user for this request.
	UserID string

	// SessionID is the current session.
	SessionID string

	// AppName is the name of the agent executing this tool.
	AppName string

	TaskID string // Agent's task ID for HIL response routing.

	// VolumeMounts lists the volumes mounted for this invocation.
	VolumeMounts []VolumeMount

	config        map[string]any
	status        *statusWriter
	ipc           *ipcClient
	outputDir     string
	oauthToken    string                    // tool-level OAuth access token, empty if not configured
	authHeaders   map[string]string         // framework-injected static auth headers (e.g. "Authorization": "Basic <b64>")
	artifactPaths map[string]string         // pre-loaded artifact file paths, keyed by param or "param[i]"
	artifactMeta  map[string]map[string]any // metadata (filename, mime_type, version) keyed identically

	// In-process bindings. When the tool runs in-process (no subprocess) these
	// direct callbacks replace the pipe/IPC transports above; SendStatus and
	// CallLLM prefer them when set. Populated only by InvokeInProcess.
	statusFn func(string) error
	llmFn    func(ctx context.Context, systemPrompt, userPrompt string, temperature float64) (string, error)
}

// SendStatus sends a status update to the agent/user.
func (tc *ToolContext) SendStatus(message string) error {
	if tc.statusFn != nil {
		return tc.statusFn(message)
	}
	if tc.status == nil {
		return nil
	}
	return tc.status.Send(message)
}

// SetConfigForTest injects tool config for external test packages only.
func (tc *ToolContext) SetConfigForTest(cfg map[string]any) { tc.config = cfg }

// GetConfig returns a tool configuration value by key.
func (tc *ToolContext) GetConfig(key string) (any, bool) {
	if tc.config == nil {
		return nil, false
	}
	v, ok := tc.config[key]
	return v, ok
}

// ConfigKeys returns the names of every top-level tool_config key present
// for this invocation. Lets a tool detect operator-typo'd keys
// (e.g. payload_keey) instead of silently dropping them and applying
// defaults. The returned slice is owned by the caller.
func (tc *ToolContext) ConfigKeys() []string {
	if tc.config == nil {
		return nil
	}
	out := make([]string, 0, len(tc.config))
	for k := range tc.config {
		out = append(out, k)
	}
	return out
}

// GetConfigString returns a tool configuration value as a string.
// Returns defaultVal if the key is not found or is not a string.
func (tc *ToolContext) GetConfigString(key, defaultVal string) string {
	v, ok := tc.config[key]
	if !ok {
		return defaultVal
	}
	s, ok := v.(string)
	if !ok {
		return defaultVal
	}
	return s
}

// CallLLM makes an LLM call proxied through the STR's IPC server.
// Returns the LLM response text. Returns an error if IPC is not available
// (e.g., the STR was not configured with an LLM service).
func (tc *ToolContext) CallLLM(ctx context.Context, systemPrompt, userPrompt string, temperature float64) (string, error) {
	if tc.llmFn != nil {
		return tc.llmFn(ctx, systemPrompt, userPrompt, temperature)
	}
	if tc.ipc == nil {
		return "", fmt.Errorf("LLM calls not available: no IPC connection to STR")
	}
	return tc.ipc.CallLLM(ctx, systemPrompt, userPrompt, temperature)
}

// GetAuthToken returns the OAuth access token for this tool invocation.
// Returns an empty string if the tool has no auth config or no token is available.
// Tools should check for an empty string and return AuthRequired() if they need auth.
func (tc *ToolContext) GetAuthToken() string {
	return tc.oauthToken
}

// AuthorizationHeader returns the ready-to-send Authorization header value for
// this invocation, regardless of the configured auth scheme. It prefers a
// framework-injected header (basic/bearer static auth), then falls back to
// "Bearer <token>" when only an OAuth access token is present, and returns ""
// when neither is available. Tools set the returned value directly as the
// request's Authorization header without knowing which scheme is in play.
func (tc *ToolContext) AuthorizationHeader() string {
	if v := tc.authHeaders["Authorization"]; v != "" {
		return v
	}
	if tc.oauthToken != "" {
		return "Bearer " + tc.oauthToken
	}
	return ""
}

// AuthHeaders returns a copy of the framework-injected static auth headers for
// this invocation (nil if none). Most tools want AuthorizationHeader instead;
// this is the escape hatch for a custom-header scheme (e.g. an X-Api-Key).
func (tc *ToolContext) AuthHeaders() map[string]string {
	if len(tc.authHeaders) == 0 {
		return nil
	}
	out := make(map[string]string, len(tc.authHeaders))
	maps.Copy(out, tc.authHeaders)
	return out
}

// SaveArtifact writes a file to the output directory where the STR will
// collect it and save it to the artifact store.
// The third parameter (MIME type) is accepted for backward compatibility with
// existing tool implementations but is not used — the STR detects MIME type
// from the file content and extension when collecting artifacts.
func (tc *ToolContext) SaveArtifact(filename string, content []byte, _ string) error {
	if tc.outputDir == "" {
		return fmt.Errorf("output directory not configured")
	}
	path := filepath.Join(tc.outputDir, filename)
	return os.WriteFile(path, content, 0o600)
}

// LoadArtifactBytes reads the pre-loaded bytes for an artifact slot. For a
// flat single-artifact param the key is the param name; for a list param
// (flat or structured) the key is formatted as "param[i]". Returns the bytes,
// optional metadata map, and an error if the slot was not pre-loaded.
//
// Intended for tools that declare structured artifact-list params
// (tagged with `artifact:"filename"`) where the per-clip metadata lives in
// the user's struct and only the raw bytes need to be pulled from the sandbox
// input dir at execution time.
func (tc *ToolContext) LoadArtifactBytes(key string) ([]byte, map[string]any, error) {
	path, ok := tc.artifactPaths[key]
	if !ok {
		return nil, nil, fmt.Errorf("no pre-loaded artifact for key %q", key)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("reading artifact %q: %w", key, err)
	}
	var meta map[string]any
	if tc.artifactMeta != nil {
		meta = tc.artifactMeta[key]
	}
	return data, meta, nil
}

// GetVolumeMountPath returns the mount path for the volume with the given
// mount path. This is a convenience for tools that know their declared
// mount path (e.g., "/workspace") and want the resolved path.
// In bwrap mode, the returned path matches the declared mount_path.
// In direct mode, it is the physical host path.
func (tc *ToolContext) GetVolumeMountPath(mountPath string) (string, bool) {
	for _, vm := range tc.VolumeMounts {
		if vm.MountPath == mountPath {
			return vm.MountPath, true
		}
	}
	return "", false
}

// close releases resources held by the ToolContext.
func (tc *ToolContext) close() {
	if tc.status != nil {
		_ = tc.status.Close()
	}
	if tc.ipc != nil {
		_ = tc.ipc.Close()
	}
}
