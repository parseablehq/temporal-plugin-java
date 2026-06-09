# Changelog

All notable changes to `temporal-parseable` (Java SDK) are documented here.

## [0.1.0] — 2026-06-09

### Added
- `ParseablePlugin` — single entry point; wraps config + emitter + interceptor wiring
- `ParseableConfig` — all settings configurable via `PARSEABLE_*` environment variables (feature-parity with TypeScript and Python SDKs)
- `ParseableEmitter` — builds and owns `SdkTracerProvider` + `SdkLoggerProvider`; exports via OTLP/HTTP directly to Parseable
- `ParseableWorkerInterceptor` — top-level `WorkerInterceptor` implementation
- `ParseableWorkflowInboundInterceptor` — emits `started` / `completed` / `failed` workflow events; replay-safe via `Workflow.isReplaying()`
- `ParseableWorkflowOutboundInterceptor` — chained outbound interceptor (extensible for child-workflow / signal tracking)
- `ParseableActivityInboundInterceptor` — emits `started` / `completed` / `failed` activity events
- `SanitizingSpanExporter` — strips/flattens non-primitive span attributes so Parseable's OTLP ingest doesn't reject them
- Example `Worker` and `Client` classes under `examples/`
- Unit tests: `ParseableConfigTest`, `SanitizingSpanExporterTest`
- Integration tests tagged `integration`, skipped in CI by default
- GitHub Actions CI: Java 11/17/21 matrix + Maven Central publish on GitHub release
