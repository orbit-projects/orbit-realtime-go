# Architecture

This repository is the Go language implementation of Orbit's provider-neutral SSE capability. `Event` owns bounded protocol framing; `NewHandler` adapts caller-owned synchronous producers to Go's `net/http` response writer.

The handler writes and flushes each event before invoking the producer for another event. It does not buffer, retain, replay, or fan out events. `orbit-core` remains the Python application runtime; this package is a separately deployable Go implementation for services that choose that language.

No Core plugin protocol, cross-process event protocol, persistence, broker integration, or event bus is implied by the SSE wire format. Authentication and route ownership remain with the host application.
