// Package correlator implements a multi-source event correlation engine for
// post-trade reconciliation. It correlates trade events by trade ID, detects
// successful matches when all sources report, and raises break alerts when the
// correlation window expires.
//
// # SAM Go Integration
//
// This package is designed to be registered as an AWE instance kind inside
// SAM Go. Because SAM Go's broker/runtime/config packages are internal, the
// integration shim lives inside the SAM Go repo at:
//
//	examples/correlator/internal/instance/instance.go
//
// That shim imports this package for the Engine and wires it to the SAM Go
// broker and AWE lifecycle. Registration:
//
//	exe.RegisterKind("correlator", instance.Factory)
//
// # Standalone Usage
//
// The Engine is broker-agnostic. Use it directly in any Go application:
//
//	engine := correlator.NewEngine([]string{"src_a", "src_b"}, 5*time.Minute)
//	result := engine.Ingest(event, time.Now())
//	breaks := engine.Sweep(time.Now())
package correlator
