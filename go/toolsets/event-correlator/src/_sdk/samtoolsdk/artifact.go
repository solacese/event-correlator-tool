package samtoolsdk

// Artifact represents a pre-loaded artifact with content and metadata.
// When a tool parameter is typed as Artifact, *Artifact, or []Artifact,
// the SDK framework automatically loads the artifact content from the
// STR's input directory before calling the handler.
type Artifact struct {
	// Content is the raw artifact data.
	Content []byte

	// Filename is the original artifact filename.
	Filename string

	// Version is the artifact version that was loaded.
	Version int

	// MIMEType is the content type (e.g., "text/plain", "image/png").
	MIMEType string

	// Metadata holds additional key-value pairs associated with the artifact.
	Metadata map[string]any
}

// AsText returns the content decoded as a UTF-8 string.
func (a *Artifact) AsText() string {
	if a == nil {
		return ""
	}
	return string(a.Content)
}

// AsBytes returns the raw content bytes.
func (a *Artifact) AsBytes() []byte {
	if a == nil {
		return nil
	}
	return a.Content
}
