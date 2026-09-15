package samtoolsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
)

// runnerArgs is the JSON structure read from runner_args.json.
// This extends the Python format with a tool_name field for Go multi-tool dispatch.
type runnerArgs struct {
	ToolName         string                    `json:"tool_name"`
	Args             map[string]any            `json:"args"`
	ToolConfig       map[string]any            `json:"tool_config"`
	ArtifactPaths    map[string]string         `json:"artifact_paths"`
	ArtifactMetadata map[string]map[string]any `json:"artifact_metadata"`
	StatusPipe       string                    `json:"status_pipe"`
	ResultFile       string                    `json:"result_file"`
	OutputDir        string                    `json:"output_dir"`
	IPCSocket        string                    `json:"ipc_socket"`
	UserID           string                    `json:"user_id"`
	SessionID        string                    `json:"session_id"`
	AppName          string                    `json:"app_name"`
	ToolOAuthToken   string                    `json:"tool_oauth_token"`
	ToolAuthHeaders  map[string]string         `json:"tool_auth_headers,omitempty"`
	VolumeMounts     []VolumeMount             `json:"volume_mounts,omitempty"`
	TaskID           string                    `json:"task_id,omitempty"`
}

// resultJSON is the JSON structure written to result.json.
type resultJSON struct {
	Result any    `json:"result"`
	Error  string `json:"error"`
}

// Run is the main entry point for a Go tool binary.
// Call this from main() with one or more tool definitions.
//
// Behavior:
//   - --schema: outputs tool schemas as JSON and exits.
//   - --schema --config <file>: loads config JSON, applies dynamic schema
//     overrides, and outputs the config-aware schemas.
//   - Otherwise, os.Args[1] must be the path to runner_args.json.
//     Reads the args, dispatches to the matching tool handler, and writes result.json.
func Run(tools ...*ToolDef) {
	if len(tools) == 0 {
		fmt.Fprintln(os.Stderr, "samtoolsdk: no tools registered")
		os.Exit(1)
	}

	// Check for --schema flag.
	schemaMode := false
	var configFile string
	args := os.Args[1:]
	for i, arg := range args {
		if arg == "--schema" {
			schemaMode = true
		}
		if arg == "--config" && i+1 < len(args) {
			configFile = args[i+1]
		}
	}

	if schemaMode {
		var config map[string]any
		if configFile != "" {
			data, err := os.ReadFile(configFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "samtoolsdk: reading config %s: %v\n", configFile, err)
				os.Exit(1)
			}
			if err := json.Unmarshal(data, &config); err != nil {
				fmt.Fprintf(os.Stderr, "samtoolsdk: parsing config %s: %v\n", configFile, err)
				os.Exit(1)
			}
		}
		if err := outputSchema(tools, config); err != nil {
			fmt.Fprintf(os.Stderr, "samtoolsdk: schema output failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Normal execution mode: read runner_args.json.
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "samtoolsdk: usage: <binary> <runner_args.json> | --schema [--config <file>]")
		os.Exit(1)
	}

	argsFile := os.Args[1]
	if err := execute(tools, argsFile); err != nil {
		fmt.Fprintf(os.Stderr, "samtoolsdk: %v\n", err)
		os.Exit(1)
	}
}

// execute reads runner_args.json, dispatches to the tool, and writes result.json.
func execute(tools []*ToolDef, argsFile string) error {
	// Read and parse runner_args.json.
	data, err := os.ReadFile(argsFile)
	if err != nil {
		return fmt.Errorf("reading runner_args: %w", err)
	}

	var args runnerArgs
	if unmarshalErr := json.Unmarshal(data, &args); unmarshalErr != nil {
		return fmt.Errorf("parsing runner_args: %w", unmarshalErr)
	}

	// Look up the tool.
	toolDef := findTool(tools, args.ToolName)
	if toolDef == nil {
		return writeError(args.ResultFile, fmt.Sprintf("tool %q not found in binary", args.ToolName))
	}

	// Build ToolContext.
	tc := &ToolContext{
		UserID:        args.UserID,
		SessionID:     args.SessionID,
		AppName:       args.AppName,
		TaskID:        args.TaskID,
		VolumeMounts:  args.VolumeMounts,
		config:        args.ToolConfig,
		outputDir:     args.OutputDir,
		oauthToken:    args.ToolOAuthToken,
		authHeaders:   args.ToolAuthHeaders,
		artifactPaths: args.ArtifactPaths,
		artifactMeta:  args.ArtifactMetadata,
	}

	// Setup status writer.
	if args.StatusPipe != "" {
		tc.status = newStatusWriter(args.StatusPipe)
	}

	// Setup IPC client for LLM callbacks.
	if args.IPCSocket != "" {
		ipc, ipcErr := newIPCClient(args.IPCSocket)
		if ipcErr != nil {
			// Log but don't fail — LLM calls will return errors.
			fmt.Fprintf(os.Stderr, "samtoolsdk: IPC connection failed: %v\n", ipcErr)
		} else {
			tc.ipc = ipc
		}
	}

	defer tc.close()

	// Pre-load artifacts.
	artifactObjects, err := loadArtifacts(toolDef.artifactInfo, args.ArtifactPaths, args.ArtifactMetadata)
	if err != nil {
		return writeError(args.ResultFile, fmt.Sprintf("loading artifacts: %v", err))
	}

	// Execute the tool. Honor SIGINT/SIGTERM so long-running tools observe
	// shutdown via ctx.Done() instead of getting SIGKILLed mid-work. The
	// subprocess doesn't inherit a parent ctx over exec boundary; signals
	// are the only propagation channel from the STR into the tool.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result, execErr := invokeHandler(ctx, toolDef, args.Args, artifactObjects, tc)
	if execErr != nil {
		return writeError(args.ResultFile, execErr.Error())
	}

	// Extract DataObjects with artifact disposition to output dir.
	if result != nil && len(result.DataObjects) > 0 && args.OutputDir != "" {
		extractArtifactOutputs(result, args.OutputDir)
	}

	// Write result.json.
	return writeResult(args.ResultFile, result)
}

// invokeHandler calls the tool's execute closure and recovers from any panic,
// converting it into an error so the process can write a clean result.json and
// exit 0 rather than crashing (exit 2). This matches the Python SDK's behaviour
// of catching handler exceptions and returning an error envelope.
func invokeHandler(
	ctx context.Context,
	toolDef *ToolDef,
	rawArgs map[string]any,
	artifactObjects map[string]any,
	tc *ToolContext,
) (result *Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := compactStack(debug.Stack(), 3)
			err = fmt.Errorf("panic in tool handler: %v | %s", r, stack)
		}
	}()
	return toolDef.execute(ctx, rawArgs, artifactObjects, tc)
}

// sdkFramePrefixes lists the function-name prefixes that belong to the SDK's
// own panic-recovery plumbing. compactStack skips these so the surfaced frames
// point at the user's handler code, not the recovery machinery.
var sdkFramePrefixes = []string{
	"runtime/debug.Stack",
	"runtime.Stack",
	"runtime.gopanic",
	"github.com/SolaceDev/solace-agent-mesh-go/pkg/samtoolsdk.invokeHandler",
}

// compactStack returns the first n user-code frames from a debug.Stack() dump
// as a pipe-separated string. It skips the "goroutine …" header, blank lines,
// and leading SDK/runtime recovery frames so the result points at the panic
// site in tool code, not the recovery plumbing.
func compactStack(stack []byte, n int) string {
	lines := strings.Split(strings.TrimSpace(string(stack)), "\n")
	var frames []string

	// debug.Stack() interleaves function lines and file/line lines: a function
	// name followed by its "\tfile:line". Collect complete pairs as one frame.
	// lines[0] is the "goroutine N [running]:" header, so start at 1.
	i := 1
	for i < len(lines) && len(frames) < n {
		funcLine := strings.TrimSpace(lines[i])
		if funcLine == "" {
			i++
			continue
		}
		fileLine := ""
		if i+1 < len(lines) {
			fileLine = strings.TrimSpace(lines[i+1])
		}

		skip := false
		for _, prefix := range sdkFramePrefixes {
			if strings.HasPrefix(funcLine, prefix) {
				skip = true
				break
			}
		}
		if skip {
			i += 2
			continue
		}

		if fileLine != "" {
			frames = append(frames, funcLine+" | "+fileLine)
			i += 2
		} else {
			frames = append(frames, funcLine)
			i++
		}
	}

	return strings.Join(frames, " | ")
}

// findTool looks up a tool by name.
func findTool(tools []*ToolDef, name string) *ToolDef {
	for _, t := range tools {
		if t.name == name {
			return t
		}
	}
	return nil
}

// loadArtifacts pre-loads artifact files from the input directory and builds
// Artifact objects for injection into the handler's parameter struct.
//
// For structured artifact-list params (FilenameKey != "") the loader does NOT
// inject objects into the parameter struct — instead the pre-loaded bytes are
// exposed to the handler via ToolContext.LoadArtifactBytes keyed by
// "param[i]". The struct itself is hydrated from the regular JSON args.
func loadArtifacts(
	artifactInfo map[string]artifactParamInfo,
	artifactPaths map[string]string,
	artifactMeta map[string]map[string]any,
) (map[string]any, error) {
	objects := make(map[string]any, len(artifactInfo))

	for paramName, info := range artifactInfo {
		if info.FilenameKey != "" {
			// Structured artifact-list. Content is exposed via ToolContext, not
			// the struct. Nothing to inject here.
			continue
		}
		if info.IsList {
			// List artifact: keys are "param[0]", "param[1]", etc.
			var artifacts []*Artifact
			for i := 0; ; i++ {
				key := fmt.Sprintf("%s[%d]", paramName, i)
				path, exists := artifactPaths[key]
				if !exists {
					break
				}
				a, err := loadSingleArtifact(key, path, artifactMeta)
				if err != nil {
					return nil, err
				}
				artifacts = append(artifacts, a)
			}
			objects[paramName] = artifacts
		} else {
			// Single artifact.
			path, exists := artifactPaths[paramName]
			if !exists {
				if info.IsOptional {
					objects[paramName] = (*Artifact)(nil)
					continue
				}
				return nil, fmt.Errorf("required artifact %q not provided", paramName)
			}
			a, err := loadSingleArtifact(paramName, path, artifactMeta)
			if err != nil {
				return nil, err
			}
			objects[paramName] = a
		}
	}

	return objects, nil
}

// loadSingleArtifact loads one artifact from the filesystem.
func loadSingleArtifact(key, path string, meta map[string]map[string]any) (*Artifact, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading artifact %q at %s: %w", key, path, err)
	}

	a := &Artifact{
		Content:  content,
		Metadata: make(map[string]any),
	}

	if m, ok := meta[key]; ok {
		if fn, ok := m["filename"].(string); ok {
			a.Filename = fn
		}
		if mt, ok := m["mime_type"].(string); ok {
			a.MIMEType = mt
		}
		if v, ok := m["version"].(float64); ok {
			a.Version = int(v)
		}
		a.Metadata = m
	}

	return a, nil
}

// extractArtifactOutputs writes DataObjects with artifact dispositions to the output dir.
// These are collected by the STR and saved to the artifact store.
func extractArtifactOutputs(result *Result, outputDir string) {
	cleanOutputDir := filepath.Clean(outputDir)
	var remaining []DataObject
	for _, do := range result.DataObjects {
		if shouldExtractToFile(do.Disposition) && do.Content != nil && do.Name != "" {
			path := filepath.Join(outputDir, do.Name)
			// Validate resolved path stays within the output directory to prevent path traversal.
			if !strings.HasPrefix(filepath.Clean(path), cleanOutputDir+string(filepath.Separator)) && filepath.Clean(path) != cleanOutputDir {
				// Reject path traversal — keep the DataObject as-is.
				remaining = append(remaining, do)
				continue
			}
			if err := os.WriteFile(path, do.Content, 0o600); err != nil {
				// Keep the DataObject if we can't write it.
				remaining = append(remaining, do)
				continue
			}
			// Remove content from the result (STR will load from file).
			do.Content = nil
			remaining = append(remaining, do)
		} else {
			remaining = append(remaining, do)
		}
	}
	result.DataObjects = remaining
}

// shouldExtractToFile returns true if the disposition means the content
// should be written to the output directory as an artifact.
func shouldExtractToFile(d DataDisposition) bool {
	switch d {
	case DispositionArtifact, DispositionArtifactWithPreview, DispositionAuto:
		return true
	default:
		return false
	}
}

// writeResult writes a successful result to result.json.
func writeResult(path string, result *Result) error {
	var wireData any
	if result != nil {
		// Serialize to wire format for compatibility.
		wireBytes, err := marshalWire(result)
		if err != nil {
			return writeError(path, fmt.Sprintf("marshaling result: %v", err))
		}
		// Unmarshal to any for embedding in resultJSON.
		if err := json.Unmarshal(wireBytes, &wireData); err != nil {
			return writeError(path, fmt.Sprintf("re-parsing wire result: %v", err))
		}
	}

	return writeResultJSON(path, resultJSON{Result: wireData})
}

// writeError writes an error to result.json.
func writeError(path string, msg string) error {
	// Sanitize newlines in error messages.
	msg = strings.ReplaceAll(msg, "\n", " | ")
	return writeResultJSON(path, resultJSON{Error: msg})
}

// writeResultJSON marshals and writes a resultJSON to disk.
func writeResultJSON(path string, r resultJSON) error {
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshaling result.json: %w", err)
	}
	return os.WriteFile(path, data, 0o600)
}
