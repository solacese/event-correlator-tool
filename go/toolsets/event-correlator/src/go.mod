module github.com/solacese/event-correlator-tool/go/toolsets/event-correlator/src

go 1.26

require (
	github.com/SolaceDev/solace-agent-mesh-go/pkg/samtoolsdk v0.0.0
	github.com/jackc/pgx/v5 v5.7.4
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.52.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.37.0 // indirect
)

replace github.com/SolaceDev/solace-agent-mesh-go/pkg/samtoolsdk => ./_sdk/samtoolsdk
