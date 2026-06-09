# temporal-parseable (Java SDK)

A Temporal plugin for Java that exports OpenTelemetry traces and logs directly to
[Parseable](https://parseable.com) — no intermediate OTel Collector required.

```
┌──────────────┐   OTLP/HTTP   ┌──────────────┐
│   Temporal   │ ────────────► │  Parseable   │
│   Worker     │ traces / logs │  temporal-*  │
│  + Plugin    │               │   streams    │
└──────────────┘               └──────────────┘
```

## Installation

**Maven:**
```xml
<dependency>
  <groupId>com.parseable</groupId>
  <artifactId>temporal-parseable</artifactId>
  <version>0.1.0</version>
</dependency>
```

**Gradle:**
```groovy
implementation 'com.parseable:temporal-parseable:0.1.0'
```

## Quick start

```java
// 1. Create the plugin (reads PARSEABLE_* env vars)
ParseableConfig config = ParseableConfig.fromEnv();
ParseablePlugin plugin = new ParseablePlugin(config);

// 2. Connect to Temporal
WorkflowServiceStubs stubs = WorkflowServiceStubs.newServiceStubs(
    plugin.configureServiceStubOptions(WorkflowServiceStubsOptions.newBuilder()).build());

WorkflowClient client = WorkflowClient.newInstance(stubs,
    plugin.configureClientOptions(WorkflowClientOptions.newBuilder()).build());

// 3. Create a worker with the interceptor wired in
WorkerFactory factory = WorkerFactory.newInstance(client);
Worker worker = factory.newWorker(
    "my-queue",
    plugin.configureWorkerOptions(WorkerOptions.newBuilder()).build());

worker.registerWorkflowImplementationTypes(MyWorkflow.class);
worker.registerActivitiesImplementations(new MyActivitiesImpl());
factory.start();

// 4. Flush on shutdown
Runtime.getRuntime().addShutdownHook(new Thread(plugin::close));
```

## Configuration

All settings can be set via environment variables or the `ParseableConfig.Builder`:

| Environment variable              | Builder method              | Default                              |
|-----------------------------------|-----------------------------|--------------------------------------|
| `PARSEABLE_ENDPOINT`              | `.endpoint(...)`            | `https://demo.parseable.com:8000`    |
| `PARSEABLE_USERNAME`              | `.username(...)`            | `admin`                              |
| `PARSEABLE_PASSWORD`              | `.password(...)`            | `password`                           |
| `PARSEABLE_LOG_STREAM`            | `.logStream(...)`           | `temporal-logs`                      |
| `PARSEABLE_TRACE_STREAM`          | `.traceStream(...)`         | `temporal-traces`                    |
| `PARSEABLE_TEMPORAL_HOST`         | `.temporalHost(...)`        | `localhost:7233`                     |
| `PARSEABLE_TEMPORAL_NAMESPACE`    | `.temporalNamespace(...)`   | `default`                            |
| `PARSEABLE_SERVICE_NAME`          | `.serviceName(...)`         | `temporal-worker`                    |
| `PARSEABLE_BATCH_EXPORT_TIMEOUT_MS` | `.batchExportTimeoutMs(...)` | `5000`                           |

## Stream pre-creation

Parseable requires streams to exist before ingestion. Create them once:

```bash
# Log stream
curl -X PUT https://<endpoint>/api/v1/logstream \
     -H "X-P-Stream: temporal-logs" \
     -u admin:password

# Trace stream
curl -X PUT https://<endpoint>/api/v1/logstream \
     -H "X-P-Stream: temporal-traces" \
     -u admin:password
```

## Repository layout

```
pom.xml                                         # Maven build; publishes to Maven Central
src/main/java/com/parseable/temporal/
├── ParseablePlugin.java                        # entry point — create one instance per worker
├── ParseableConfig.java                        # settings + PARSEABLE_* env-var wiring
├── ParseableEmitter.java                       # OTel tracer + logger; owns the SDK lifecycle
├── interceptors/
│   ├── ParseableWorkerInterceptor.java         # WorkerInterceptor implementation
│   ├── ParseableWorkflowInboundInterceptor.java  # start / complete / fail events; replay-safe
│   ├── ParseableWorkflowOutboundInterceptor.java # outbound interceptor (extensible)
│   └── ParseableActivityInboundInterceptor.java  # activity start / complete / fail
├── exporters/
│   └── SanitizingSpanExporter.java             # flattens non-primitive span attributes
└── version/
    └── Version.java                            # PLUGIN_VERSION constant

examples/src/main/java/com/parseable/temporal/example/
├── Workflows.java                              # example workflow + activity definitions
├── Worker.java                                 # demo worker
└── Client.java                                 # triggers example workflows

src/test/java/com/parseable/temporal/
├── ParseableConfigTest.java                    # unit tests for config + defaults
├── SanitizingSpanExporterTest.java             # unit tests for attribute sanitization
└── ParseableInterceptorTest.java               # integration tests (tag: integration)
```

## Architecture

```
      ┌───────────────────┐
      │  Temporal Server  │
      │ (localhost:7233)  │
      └─────────┬─────────┘
                │ gRPC
┌───────────────┴───────────────┐
│           Worker              │
│                               │
│  ┌─────────────────────────┐  │
│  │  WorkflowInbound +      │  │
│  │  WorkflowOutbound       │  │
│  │  interceptors           │  │
│  │                         │  │
│  │  Workflow.isReplaying() │  │  ← replay guard
│  └───────────────┬─────────┘  │
│                  ▼            │
│  ┌──────────────────────────┐ │
│  │  ActivityInbound         │ │
│  │  interceptor             │ │
│  └──────────────┬───────────┘ │
│                 │             │
│  ┌──────────────▼───────────┐ │
│  │  ParseableEmitter        │ │
│  │   → OTel Tracer          │ │
│  │   → BatchSpanProcessor   │ │
│  │   → SanitizingSpanExp.   │ │
│  │   → OTLPHttpSpanExporter │ │
│  │                          │ │
│  │   → OTel Logger          │ │
│  │   → BatchLogRecordProc.  │ │
│  │   → OTLPHttpLogExporter  │ │
│  └──────────────┬───────────┘ │
└─────────────────┼─────────────┘
                  │ HTTPS OTLP
        ┌─────────▼──────────┐
        │     Parseable      │
        │  /v1/logs  (logs)  │
        │  /v1/traces(spans) │
        └────────────────────┘
```

### Key design points

**Replay safety.** Workflow events are guarded with `Workflow.isReplaying()`. When Temporal
replays workflow history the guard suppresses emission — no duplicate logs or spans.

**SanitizingSpanExporter.** Parseable's OTLP parser rejects spans containing array or map-typed
attributes. `SanitizingSpanExporter` converts array attributes to comma-joined strings and drops
map-typed attributes before forwarding to the OTLP exporter.

**No sandbox concern.** Unlike the Python SDK, the Java SDK has no workflow sandbox restriction,
so no passthrough configuration is needed.

**Graceful shutdown.** Call `plugin.close()` (or register it as a shutdown hook) before your JVM
exits. This force-flushes both the tracer and logger providers so in-flight batches are sent.

## Tests

| Test class | Type | What it covers |
|---|---|---|
| `ParseableConfigTest` | Unit | Env-var defaults, builder overrides, endpoint derivation |
| `SanitizingSpanExporterTest` | Unit | Primitive pass-through, array flattening, flush/shutdown delegation |
| `ParseableInterceptorTest` | Integration | Workflow + activity event emission, replay-safety assertion |

Run unit tests (default):
```bash
mvn test
```

Run all tests including integration:
```bash
mvn test -Pintegration
```

## Running the demo

```bash
# Terminal 1 — start Temporal dev server
temporal server start-dev

# Terminal 2 — start Parseable (or point at staging.parseable.com)
# Pre-create streams (see above), then:
mvn compile exec:java -Dexec.mainClass=com.parseable.temporal.example.Worker

# Terminal 3
mvn compile exec:java -Dexec.mainClass=com.parseable.temporal.example.Client
```

## Publishing to Maven Central

Tag `v0.1.0` and create a GitHub release — the CI workflow publishes automatically.

```bash
git tag v0.1.0
git push origin v0.1.0
# Create release on GitHub UI
```

Required repository secrets: `CENTRAL_TOKEN_USERNAME`, `CENTRAL_TOKEN_PASSWORD`,
`GPG_PRIVATE_KEY`, `GPG_PASSPHRASE`.

## License

Apache 2.0 — see [LICENSE](LICENSE).
