// This is reference code that only compiles inside the SAM Go module tree.
// This go.mod prevents `go mod tidy` on the parent module from trying to
// resolve SAM Go internal imports.
module github.com/solacese/event-correlator-go/sam-integration

go 1.22
