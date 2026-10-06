# Development

Requires Go 1.24 or later. Run `gofmt -w .`, `go test ./...`, `go test -race ./...`, `go vet ./...`, and `go build ./...` before proposing changes.

Tests should remain deterministic and avoid live infrastructure. Update the README and changelog when a public API, security property, lifecycle rule, or operational assumption changes. Hosted CI, dependency review, artifact integrity, provenance, proxy interoperability, and representative load evidence are release gates; local green checks do not establish production readiness.
