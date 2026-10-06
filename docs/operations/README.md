# Operations

Terminate public TLS at a trusted ingress or configure TLS in the host Go service. Verify the ingress flushes `text/event-stream` responses promptly, disables response buffering, and has idle and drain timeouts suited to long-lived requests. Propagate request cancellation to event producers and close their upstream subscriptions when the handler returns.

Size concurrent streams from memory, file descriptor, and upstream subscription budgets. This package has no internal queue, heartbeat scheduler, or retry policy; implement explicit bounded policies in the application where needed. Test disconnect and shutdown behavior through the actual proxy before deployment.
