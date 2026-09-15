package samtoolsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
)

// HandlerFunc is the signature for a tool handler function.
// P is the parameter struct type whose fields drive JSON Schema generation.
type HandlerFunc[P any] func(ctx context.Context, params P, tc *ToolContext) (*Result, error)

// ToolDef is a registered tool definition. Created via NewTool.
type ToolDef struct {
	name           string
	description    string
	instructions   string
	schema         *JSONSchema
	artifactInfo   map[string]artifactParamInfo
	paramsType     reflect.Type
	authConfig     *AuthSchemaConfig
	volumeParams   []VolumeParamDecl   // declared volume requirements
	configSchema   []ConfigSchemaField // declared operator config requirements
	dynamicSchema  DynamicSchemaFunc   // optional: config-aware schema overrides
	timeoutSeconds int                 // custom timeout in seconds (0 = use default)

	// execute is the type-erased handler that deserializes args, injects
	// artifacts, and calls the typed handler.
	execute func(ctx context.Context, args map[string]any, artifactObjects map[string]any, tc *ToolContext) (*Result, error)
}

// Name returns the tool's registered name (the name the LLM invokes, before
// any per-deployment dynamic rename).
func (td *ToolDef) Name() string { return td.name }

// AuthSchemaConfig declares auth requirements for a tool.
// This is included in the --schema output so deployers know what credentials
// to configure.
type AuthSchemaConfig struct {
	// Type is the auth type: "oauth2", "basic", "bearer".
	Type string `json:"type"`

	// Scheme describes OAuth2 endpoints (oauth2 only).
	Scheme *AuthSchemaOAuth `json:"scheme,omitempty"`
}

// AuthSchemaOAuth describes OAuth2 endpoints and scopes declared by a tool.
type AuthSchemaOAuth struct {
	AuthorizationURL        string   `json:"authorization_url,omitempty"`
	TokenURL                string   `json:"token_url,omitempty"`
	Scopes                  []string `json:"scopes,omitempty"`
	TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method,omitempty"`
}

// SchemaOverride allows a DynamicSchemaFunc to override parts of the tool
// definition based on per-agent tool_config. Empty/nil fields are ignored
// (the base value is kept).
type SchemaOverride struct {
	Name         string              // override tool name (empty = keep original)
	Description  string              // override description (empty = keep original)
	Instructions string              // override instructions (empty = keep original)
	Parameters   *JSONSchema         // override parameter schema (nil = keep original)
	ConfigSchema []ConfigSchemaField // override config schema (nil = keep original)
}

// ConfigSchemaField declares a configuration parameter the tool needs from
// operators (e.g., database credentials, API keys). Included in --schema
// output so the platform can render a config form and validate completeness.
type ConfigSchemaField struct {
	Key         string `json:"key"`
	Type        string `json:"type"` // "string", "integer", "boolean"
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
	Secret      bool   `json:"secret,omitempty"`
	Default     any    `json:"default,omitempty"`
	Options     []any  `json:"options,omitempty"`
}

// DynamicSchemaFunc is called with the per-agent tool_config and returns
// overrides for the tool's schema. Return nil to use the base schema as-is.
type DynamicSchemaFunc func(config map[string]any) *SchemaOverride

// VolumeParamDecl declares a volume parameter that a tool requires.
// This is included in the --schema output so the STR and agent know what
// volumes to provision and mount.
type VolumeParamDecl struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Mode        string `json:"mode"`       // "readonly" or "readwrite"
	MountPath   string `json:"mount_path"` // path inside sandbox (e.g., "/workspace")
}

// ToolOption configures a ToolDef via functional options.
type ToolOption func(*ToolDef)

// WithInstructions sets optional text injected into the system prompt
// when this tool is available to the LLM.
func WithInstructions(instructions string) ToolOption {
	return func(td *ToolDef) { td.instructions = instructions }
}

// WithDynamicSchema registers a function that can override the tool's name,
// description, instructions, and parameters based on per-agent tool_config.
// When the STR runs --schema --config <file>, this function is called and
// its overrides are applied to the schema output.
func WithDynamicSchema(fn DynamicSchemaFunc) ToolOption {
	return func(td *ToolDef) { td.dynamicSchema = fn }
}

// WithVolumeParams declares volume parameters that this tool requires.
// The declarations are included in the --schema output for the STR manifest
// and agent volume binding configuration.
func WithVolumeParams(params ...VolumeParamDecl) ToolOption {
	return func(td *ToolDef) { td.volumeParams = params }
}

// WithAuth declares auth requirements for a tool. The auth config is included
// in the --schema output so deployers know what credentials to configure.
// At runtime, the framework merges tool-declared auth with deployer-provided
// credentials and manages the OAuth flow transparently.
func WithAuth(cfg AuthSchemaConfig) ToolOption {
	return func(td *ToolDef) { td.authConfig = &cfg }
}

// WithConfigSchema declares operator-provided configuration parameters that
// this tool requires (e.g., database connection strings, API keys). The
// declarations are included in the --schema output so the platform can render
// a configuration form and validate completeness before deployment.
func WithConfigSchema(fields ...ConfigSchemaField) ToolOption {
	return func(td *ToolDef) { td.configSchema = fields }
}

// WithTimeout sets the tool's execution timeout in seconds.
// This is included in the --schema output and overrides the manifest default.
// Use this for tools that are known to complete quickly (e.g., 30s for email)
// or slowly (e.g., 300s for video processing).
func WithTimeout(seconds int) ToolOption {
	return func(td *ToolDef) { td.timeoutSeconds = seconds }
}

// reservedAuthConfigKey is the config-map key the platform reserves for
// deployer-supplied OAuth credentials (auth.credential.client_id and
// allow-listed scheme overrides). Tools cannot declare a ConfigSchemaField
// with this key — the platform routes the deployer auth through the same
// config map under this key, so a tool declaring it would shadow the
// deployer's slot silently.
const reservedAuthConfigKey = "auth"

// NewTool creates a tool definition with a typed handler.
// The generic parameter P must be a struct whose fields define the tool's parameters.
//
// Parameter struct tags:
//   - json:"name"   — JSON property name (required)
//   - desc:"..."    — LLM-visible description
//
// Field types:
//   - string, int*, float*, bool           — standard JSON Schema types
//   - *T (pointer)                          — marks the field as optional
//   - Artifact / *Artifact / []Artifact / *[]Artifact  — artifact parameters (auto-loaded)
//   - []T, map[string]T, struct            — array, object, nested object
func NewTool[P any](name, description string, handler HandlerFunc[P], opts ...ToolOption) *ToolDef {
	var zero P
	t := reflect.TypeOf(zero)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// The description is the highest-salience field the LLM sees when choosing a
	// tool, and strict providers (e.g. Amazon Bedrock's Converse API) reject a
	// tool advertised with an empty description, failing the whole request. Fail
	// fast at registration so the author supplies one during development, not at
	// deploy time against a strict model.
	if strings.TrimSpace(description) == "" {
		panic(fmt.Sprintf("samtoolsdk: tool %q registered with an empty description; every tool must describe what it does", name))
	}

	schema, artifactInfo := schemaFromStruct(t)

	td := &ToolDef{
		name:         name,
		description:  description,
		schema:       schema,
		artifactInfo: artifactInfo,
		paramsType:   t,
	}

	// Build the type-erased execute closure.
	td.execute = func(ctx context.Context, args map[string]any, artifactObjects map[string]any, tc *ToolContext) (*Result, error) {
		params, err := deserializeParams[P](t, args, artifactObjects, artifactInfo)
		if err != nil {
			return nil, fmt.Errorf("deserializing params for tool %q: %w", name, err)
		}
		return handler(ctx, params, tc)
	}

	for _, opt := range opts {
		opt(td)
	}

	// "auth" is a reserved config key — the platform routes the deployer-supplied
	// OAuth credential block through the same config map and reads it from this
	// key. A tool that declares its own config field named "auth" would shadow
	// that path silently. Fail fast at registration so the author sees it during
	// development, not at deploy time.
	for _, f := range td.configSchema {
		if f.Key == reservedAuthConfigKey {
			panic(fmt.Sprintf("samtoolsdk: tool %q declares config field %q, which is reserved for deployer-supplied OAuth credentials", name, reservedAuthConfigKey))
		}
	}

	return td
}

// deserializeParams converts the raw args map into a typed parameter struct,
// injecting pre-loaded artifacts into artifact-typed fields.
//
// Structured artifact-list params (FilenameKey != "") are deserialized via the
// normal JSON path because the Python wire shape is a list of objects — the
// struct already carries the filename and any sibling scalars. Pre-loaded
// bytes for those params are reachable from the handler via
// ToolContext.LoadArtifactBytes using the "param[i]" keys the agent and STR
// use to index them.
func deserializeParams[P any](t reflect.Type, args map[string]any, artifactObjects map[string]any, artifactInfo map[string]artifactParamInfo) (P, error) {
	var zero P

	// Remove flat artifact params from args before JSON unmarshal (they're
	// handled separately). Structured artifact-list params stay in args so the
	// JSON shape (array of objects) deserializes into the user's struct.
	cleanArgs := make(map[string]any, len(args))
	for k, v := range args {
		info, isArtifact := artifactInfo[k]
		if isArtifact && info.FilenameKey == "" {
			continue
		}
		cleanArgs[k] = v
	}

	// LLMs frequently emit a structured parameter (array/object) as a
	// JSON-encoded string rather than a native JSON value — e.g. a `filters`
	// array sent as "[{...}]". A strict unmarshal into the typed field then
	// fails and the whole tool call errors out. Coerce those back to native
	// values before the round-trip so the tool still runs.
	coerceStringifiedComposites(t, cleanArgs)

	// JSON round-trip to deserialize regular fields.
	data, err := json.Marshal(cleanArgs)
	if err != nil {
		return zero, fmt.Errorf("marshaling args: %w", err)
	}

	// Create a new instance of P.
	val := reflect.New(t)
	if err := json.Unmarshal(data, val.Interface()); err != nil {
		return zero, fmt.Errorf("unmarshaling args into %T: %w", zero, err)
	}

	// Inject artifact fields (flat variants only).
	// Skip if P is not a struct (e.g., map[string]any for dynamic params).
	if t.Kind() != reflect.Struct {
		return val.Elem().Interface().(P), nil
	}
	elem := val.Elem()
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		name := jsonFieldName(field)
		info, isArtifact := artifactInfo[name]
		if !isArtifact || info.FilenameKey != "" {
			continue
		}

		artObj, exists := artifactObjects[name]
		if !exists {
			continue
		}

		fieldVal := elem.Field(i)
		if err := setArtifactField(fieldVal, artObj, info); err != nil {
			return zero, fmt.Errorf("setting artifact field %q: %w", name, err)
		}
	}

	return val.Elem().Interface().(P), nil
}

// coerceStringifiedComposites rewrites args where the caller passed a structured
// parameter (array/object) as a JSON-encoded string instead of a native JSON
// value — a common LLM behavior that would otherwise fail the strict unmarshal
// in deserializeParams. For each exported struct field whose type is a
// slice/array/map/struct (or pointer to one), if the matching arg is a string
// that begins with '[' or '{' and parses as JSON, it is replaced with the
// parsed value. Scalar-typed fields (string/number/bool) and `any`/interface
// fields are left untouched, so a value the field legitimately accepts as a
// string is never mangled. A parse failure is left as-is to surface the
// original error downstream.
func coerceStringifiedComposites(t reflect.Type, args map[string]any) {
	if t.Kind() != reflect.Struct {
		return
	}
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() || !isCompositeKind(field.Type) {
			continue
		}
		s, ok := args[jsonFieldName(field)].(string)
		if !ok {
			continue
		}
		trimmed := strings.TrimSpace(s)
		if trimmed == "" || (trimmed[0] != '[' && trimmed[0] != '{') {
			continue
		}
		var parsed any
		if json.Unmarshal([]byte(trimmed), &parsed) == nil {
			args[jsonFieldName(field)] = parsed
		}
	}
}

// isCompositeKind reports whether t (after dereferencing pointers) is a
// slice, array, map, or struct — the kinds that cannot be populated from a
// bare JSON string and so are candidates for stringified-JSON coercion.
func isCompositeKind(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map, reflect.Struct:
		return true
	default:
		return false
	}
}

// setArtifactField sets a struct field to an artifact value.
func setArtifactField(field reflect.Value, artObj any, info artifactParamInfo) error {
	if info.IsList {
		// []Artifact or *[]Artifact (optional list)
		artifacts, ok := artObj.([]*Artifact)
		if !ok {
			return fmt.Errorf("expected []*Artifact, got %T", artObj)
		}
		slice := reflect.MakeSlice(reflect.TypeOf([]Artifact{}), len(artifacts), len(artifacts))
		for i, a := range artifacts {
			if a != nil {
				slice.Index(i).Set(reflect.ValueOf(*a))
			}
		}
		if field.Kind() == reflect.Pointer {
			// *[]Artifact — allocate the slice and point the field at it.
			p := reflect.New(field.Type().Elem())
			p.Elem().Set(slice)
			field.Set(p)
		} else {
			field.Set(slice)
		}
	} else if info.IsOptional {
		// *Artifact
		a, ok := artObj.(*Artifact)
		if !ok {
			return fmt.Errorf("expected *Artifact, got %T", artObj)
		}
		if a != nil {
			field.Set(reflect.ValueOf(a))
		}
	} else {
		// Artifact (required)
		a, ok := artObj.(*Artifact)
		if !ok {
			return fmt.Errorf("expected *Artifact, got %T", artObj)
		}
		if a != nil {
			field.Set(reflect.ValueOf(*a))
		}
	}
	return nil
}

// schemaOutput is the JSON format produced by --schema.
type schemaOutput struct {
	Tools map[string]toolSchemaEntry `json:"tools"`
}

type toolSchemaEntry struct {
	Description    string                       `json:"description"`
	Parameters     *JSONSchema                  `json:"parameters"`
	ArtifactParams map[string]artifactParamInfo `json:"artifact_params,omitempty"`
	Instructions   string                       `json:"instructions,omitempty"`
	Auth           *AuthSchemaConfig            `json:"auth,omitempty"`
	VolumeParams   []VolumeParamDecl            `json:"volume_params,omitempty"`
	ConfigSchema   []ConfigSchemaField          `json:"config_schema,omitempty"`
	TimeoutSeconds int                          `json:"timeout_seconds,omitempty"`
}

// outputSchema writes the schema JSON for all tools to stdout.
// If config is non-nil, dynamic schema functions are called with it.
func outputSchema(tools []*ToolDef, config map[string]any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(buildSchemaOutput(tools, config))
}

// buildSchemaOutput assembles the schema document for all tools, applying
// dynamic schema overrides when config is non-nil. Shared by outputSchema
// (subprocess --schema path) and SchemaJSON (in-process path).
func buildSchemaOutput(tools []*ToolDef, config map[string]any) schemaOutput {
	out := schemaOutput{
		Tools: make(map[string]toolSchemaEntry, len(tools)),
	}
	for _, td := range tools {
		name := td.name
		desc := td.description
		instr := td.instructions
		schema := td.schema
		cfgSchema := td.configSchema

		// Apply dynamic schema overrides when config is provided.
		if config != nil && td.dynamicSchema != nil {
			if override := td.dynamicSchema(config); override != nil {
				if override.Name != "" {
					name = override.Name
				}
				if override.Description != "" {
					desc = override.Description
				}
				if override.Instructions != "" {
					instr = override.Instructions
				}
				if override.Parameters != nil {
					schema = override.Parameters
				}
				if override.ConfigSchema != nil {
					cfgSchema = override.ConfigSchema
				}
			}
		}

		out.Tools[name] = toolSchemaEntry{
			Description:    desc,
			Parameters:     schema,
			ArtifactParams: td.artifactInfo,
			Instructions:   instr,
			Auth:           td.authConfig,
			VolumeParams:   td.volumeParams,
			ConfigSchema:   cfgSchema,
			TimeoutSeconds: td.timeoutSeconds,
		}
	}
	return out
}
