package samtoolsdk

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
)

// JSONSchema is a simplified JSON Schema representation for tool parameters.
// Exported so that DynamicSchemaFunc implementations can build and return
// alternate parameter schemas via SchemaOverride.Parameters.
type JSONSchema struct {
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties,omitempty"`
	Required   []string       `json:"required,omitempty"`
}

// BuildSchema returns the JSON schema that the SDK would auto-generate for
// the parameter struct type T. It is intended for use from DynamicSchemaFunc
// implementations that swap the parameter shape based on per-agent tool_config
// (for example a data-access tool that exposes different fields for MongoDB
// vs DynamoDB backends).
//
// If T is not a struct type the returned schema has Type set to "object" and
// no properties. Artifact-typed fields are rendered in the schema exactly as
// they are by NewTool.
func BuildSchema[T any]() *JSONSchema {
	var zero T
	t := reflect.TypeOf(zero)
	if t == nil {
		return &JSONSchema{Type: "object"}
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	schema, _ := schemaFromStruct(t)
	return schema
}

// artifactParamInfo describes an artifact-typed parameter.
//
// FilenameKey is set when the param is a list of structured objects where the
// artifact filename lives at a named JSON field on each element (e.g. Python's
// concatenate_audio clips_to_join: [{filename, pause_after_ms}]). When unset,
// the param is a flat Artifact / *Artifact / []Artifact as before.
type artifactParamInfo struct {
	IsList      bool   `json:"is_list"`
	IsOptional  bool   `json:"is_optional"`
	FilenameKey string `json:"filename_key,omitempty"`
}

// schemaFromStruct generates a JSON Schema and artifact parameter info from a Go struct type.
//
// Rules:
//   - A field is **required** unless it is a pointer type OR its json tag
//     carries `,omitempty`. Either signal marks the field optional in the
//     emitted JSON Schema, matching the encoding/json convention for
//     "this field is safe to omit at serialization time."
//   - When the two signals disagree (non-pointer field with `,omitempty`),
//     the SDK prints a one-line warning to stderr from the schema generator
//     (see warnSignalMismatch). The author almost always wanted the field
//     to be optional; the warning surfaces that intent so they can switch
//     to a pointer type and avoid the surprise.
//   - json:"name" tag provides the JSON property name.
//   - desc:"..." tag provides the LLM-visible description.
//   - Artifact / *Artifact / []Artifact fields are detected as artifact params
//     and presented as string / string / array-of-strings to the LLM.
//   - []T where T is a struct with exactly one string field tagged
//     `artifact:"filename"` becomes a structured artifact-list param: the
//     schema is array-of-object (Python-compatible) and the SDK treats each
//     element's filename field as the artifact reference.
//   - Go types map: string→"string", int*→"integer", float*→"number",
//     bool→"boolean", []T→"array", map→"object", struct→"object" (recursive).
func schemaFromStruct(t reflect.Type) (*JSONSchema, map[string]artifactParamInfo) {
	// Unwrap pointer types.
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return &JSONSchema{Type: "object"}, nil
	}

	props := make(map[string]any)
	var required []string
	artifactParams := make(map[string]artifactParamInfo)

	for i := range t.NumField() {
		addFieldToSchema(t, t.Field(i), props, &required, artifactParams)
	}

	schema := &JSONSchema{
		Type:       "object",
		Properties: props,
		Required:   required,
	}

	return schema, artifactParams
}

// addFieldToSchema appends one struct field's schema fragment to props /
// required / artifactParams. Extracted from schemaFromStruct so the per-field
// dispatch stays a flat sequence of "is this an artifact? a list of artifacts?
// a structured artifact-list? else a regular field" branches.
func addFieldToSchema(parent reflect.Type, field reflect.StructField,
	props map[string]any, required *[]string, artifactParams map[string]artifactParamInfo,
) {
	if !field.IsExported() {
		return
	}
	name := jsonFieldName(field)
	if name == "-" {
		return
	}

	desc := field.Tag.Get("desc")
	isPointer := field.Type.Kind() == reflect.Ptr
	hasOmitempty := jsonTagHasOmitempty(field)
	// Either pointer or omitempty marks the field optional. A non-pointer
	// field with `,omitempty` is almost always an author-intent / SDK-rule
	// mismatch — warn but honor omitempty, since that matches encoding/json
	// semantics and what authors expect from the json tag.
	isOptional := isPointer || hasOmitempty
	if !isPointer && hasOmitempty && omitemptyAmbiguous(field.Type) {
		warnSignalMismatch(parent, field)
	}

	elemType := field.Type
	if isPointer {
		elemType = field.Type.Elem()
	}

	switch {
	case isArtifactType(elemType):
		addArtifactProp(name, desc, false, isOptional, "", props, artifactParams)
	case elemType.Kind() == reflect.Slice && isArtifactType(elemType.Elem()):
		addArtifactListProp(name, desc, isOptional, props, artifactParams)
	case isStructuredArtifactList(elemType):
		addStructuredArtifactListProp(name, desc, elemType.Elem(), isOptional, props, artifactParams)
	default:
		addRegularProp(name, desc, elemType, props)
	}

	if !isOptional {
		*required = append(*required, name)
	}
}

// addArtifactProp records a single Artifact / *Artifact field. The schema is
// "string" because the LLM only sees the artifact filename; isList must be
// false here (artifact-list variants have their own helpers). filenameKey is
// "" for plain artifacts and is only set by addStructuredArtifactListProp.
func addArtifactProp(name, desc string, isList, isOptional bool, filenameKey string,
	props map[string]any, artifactParams map[string]artifactParamInfo,
) {
	prop := map[string]any{"type": "string"}
	if desc != "" {
		prop["description"] = desc
	}
	props[name] = prop
	artifactParams[name] = artifactParamInfo{
		IsList:      isList,
		IsOptional:  isOptional,
		FilenameKey: filenameKey,
	}
}

// addArtifactListProp records a []Artifact field as a string-array.
func addArtifactListProp(name, desc string, isOptional bool,
	props map[string]any, artifactParams map[string]artifactParamInfo,
) {
	prop := map[string]any{
		"type":  "array",
		"items": map[string]any{"type": "string"},
	}
	if desc != "" {
		prop["description"] = desc
	}
	props[name] = prop
	artifactParams[name] = artifactParamInfo{IsList: true, IsOptional: isOptional}
}

// addStructuredArtifactListProp records a []StructWithFilenameTag field
// (Python-compatible artifact-list with sibling fields like `pause_after_ms`).
// The element struct's own schema is recursed into so nested validation lands
// on the wire.
func addStructuredArtifactListProp(name, desc string, elemStruct reflect.Type, isOptional bool,
	props map[string]any, artifactParams map[string]artifactParamInfo,
) {
	key, _ := filenameTagField(elemStruct)
	nested, _ := schemaFromStruct(elemStruct)
	items := map[string]any{"type": "object"}
	if len(nested.Properties) > 0 {
		items["properties"] = nested.Properties
	}
	if len(nested.Required) > 0 {
		items["required"] = nested.Required
	}
	prop := map[string]any{"type": "array", "items": items}
	if desc != "" {
		prop["description"] = desc
	}
	props[name] = prop
	artifactParams[name] = artifactParamInfo{
		IsList:      true,
		IsOptional:  isOptional,
		FilenameKey: key,
	}
}

// addRegularProp records a non-artifact field via goTypeToSchema.
func addRegularProp(name, desc string, elemType reflect.Type, props map[string]any) {
	prop := goTypeToSchema(elemType)
	if desc != "" {
		prop["description"] = desc
	}
	props[name] = prop
}

// isStructuredArtifactList reports whether t is a []Struct where the struct
// has exactly one string field tagged `artifact:"filename"`. The caller uses
// this as the dispatch gate before recursing into the element shape.
func isStructuredArtifactList(t reflect.Type) bool {
	if t.Kind() != reflect.Slice || t.Elem().Kind() != reflect.Struct {
		return false
	}
	_, ok := filenameTagField(t.Elem())
	return ok
}

// goTypeToSchema maps a Go reflect.Type to a JSON Schema property map.
func goTypeToSchema(t reflect.Type) map[string]any {
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}

	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}

	case reflect.Bool:
		return map[string]any{"type": "boolean"}

	case reflect.Slice:
		items := goTypeToSchema(t.Elem())
		return map[string]any{
			"type":  "array",
			"items": items,
		}

	case reflect.Map:
		return map[string]any{"type": "object"}

	case reflect.Struct:
		// Recursive: generate nested object schema.
		nested, _ := schemaFromStruct(t)
		result := map[string]any{"type": "object"}
		if len(nested.Properties) > 0 {
			result["properties"] = nested.Properties
		}
		if len(nested.Required) > 0 {
			result["required"] = nested.Required
		}
		return result

	case reflect.Ptr:
		return goTypeToSchema(t.Elem())

	default:
		return map[string]any{"type": "string"}
	}
}

// isArtifactType checks if a type is the Artifact struct.
func isArtifactType(t reflect.Type) bool {
	return t == reflect.TypeOf(Artifact{})
}

// filenameTagField returns the JSON name of the string field tagged
// `artifact:"filename"` on struct type t, if exactly one such field exists.
// It is used to detect structured artifact-list parameters like
// clips_to_join: [{filename, pause_after_ms}].
func filenameTagField(t reflect.Type) (string, bool) {
	if t.Kind() != reflect.Struct {
		return "", false
	}
	var match string
	found := 0
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		if f.Type.Kind() != reflect.String {
			continue
		}
		if f.Tag.Get("artifact") != "filename" {
			continue
		}
		match = jsonFieldName(f)
		found++
	}
	if found == 1 {
		return match, true
	}
	return "", false
}

// jsonFieldName extracts the JSON field name from a struct field.
// Returns the field name if no json tag is present.
func jsonFieldName(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if tag == "" {
		return f.Name
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		return f.Name
	}
	return name
}

// jsonTagHasOmitempty reports whether the field's json tag carries the
// `,omitempty` option. Matches encoding/json's own parsing: any
// comma-separated option after the name counts, so `json:",omitempty"`
// and `json:"foo,omitempty"` and `json:"foo,string,omitempty"` all
// resolve to true.
func jsonTagHasOmitempty(f reflect.StructField) bool {
	tag := f.Tag.Get("json")
	if tag == "" {
		return false
	}
	_, opts, _ := strings.Cut(tag, ",")
	for opt := range strings.SplitSeq(opts, ",") {
		if opt == "omitempty" {
			return true
		}
	}
	return false
}

// schemaWarnSink holds the io.Writer warnings are emitted to. Stored via
// atomic.Pointer so test-time substitution and concurrent BuildSchema
// callers (e.g. DynamicSchemaFunc paths invoked from long-running tool
// processes) don't race on the swap. Defaults to os.Stderr so a normal
// `<tool> --schema` invocation surfaces warnings to the operator running
// the build.
var schemaWarnSink atomic.Pointer[io.Writer]

func init() {
	var w io.Writer = os.Stderr
	schemaWarnSink.Store(&w)
}

// setSchemaWarnWriter atomically swaps the warning sink and returns the
// previous value. Used by tests to capture warnings into a buffer; never
// called from production code.
func setSchemaWarnWriter(w io.Writer) io.Writer {
	old := schemaWarnSink.Load()
	schemaWarnSink.Store(&w)
	if old == nil {
		return nil
	}
	return *old
}

// omitemptyAmbiguous reports whether `,omitempty` on a non-pointer field of
// this type is genuinely ambiguous about optionality. For slices and maps a
// nil value is an unambiguous "absent" and omitempty is idiomatic
// encoding/json, so it's not worth a warning. For scalars (and structs, where
// omitempty is a no-op) the zero value can't be distinguished from "absent",
// which is the author-intent mismatch the warning exists to surface.
func omitemptyAmbiguous(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Slice, reflect.Map:
		return false
	default:
		return true
	}
}

// warnSignalMismatch emits a one-line warning when a struct field's type
// (non-pointer) disagrees with its json tag (`,omitempty`) about whether
// the field is optional. The SDK now treats omitempty as enough to make
// the field optional in the emitted JSON Schema, but the warning surfaces
// the author-intent / Go-style mismatch so the author can either switch
// to a pointer type (and drop the omitempty noise) or remove the
// omitempty if they meant the field to be required.
func warnSignalMismatch(parent reflect.Type, f reflect.StructField) {
	w := schemaWarnSink.Load()
	if w == nil || *w == nil {
		return
	}
	// fmt.Fprintf errors aren't actionable here — the warning is best-effort
	// diagnostic output, identical in spirit to log.Printf calls elsewhere
	// in the SDK. Tests assert the warning content via the buffer sink.
	_, _ = fmt.Fprintf(*w,
		"samtoolsdk: %s.%s has json `,omitempty` but is non-pointer (%s); "+
			"treating as optional. Use a pointer type (e.g. *%s) and drop "+
			"`,omitempty` to silence this warning, or remove `,omitempty` "+
			"if the field should be required.\n",
		parent.Name(), f.Name, f.Type.String(), f.Type.String(),
	)
}
