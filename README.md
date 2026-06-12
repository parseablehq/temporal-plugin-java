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
ParseablePlugin plugin = new ParseablePlugin(ParseableConfig.fromEnv());

// 2. Connect to Temporal — configure the Temporal target normally,
//    then register the plugin once on service stubs
WorkflowServiceStubs stubs = WorkflowServiceStubs.newServiceStubs(
    WorkflowServiceStubsOptions.newBuilder()
        .setTarget("localhost:7233")
        .setPlugins(plugin)
        .build());

WorkflowClient client = WorkflowClient.newInstance(stubs);

// 3. Create a worker factory — the plugin is propagated automatically
WorkerFactory factory = WorkerFactory.newInstance(client);
Worker worker = factory.newWorker("my-queue");

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
| `PARSEABLE_ENDPOINT`              | `.endpoint(...)`            | **required**                         |
| `PARSEABLE_USERNAME`              | `.username(...)`            | **required**                         |
| `PARSEABLE_PASSWORD`              | `.password(...)`            | **required**                         |
| `PARSEABLE_LOG_STREAM`            | `.logStream(...)`           | `temporal-logs`                      |
| `PARSEABLE_TRACE_STREAM`          | `.traceStream(...)`         | `temporal-traces`                    |
| `PARSEABLE_TEMPORAL_HOST`         | `.temporalHost(...)`        | Deprecated; configure Temporal with `WorkflowServiceStubsOptions.setTarget(...)` |
| `PARSEABLE_TEMPORAL_NAMESPACE`    | `.temporalNamespace(...)`   | `default`                            |
| `PARSEABLE_SERVICE_NAME`          | `.serviceName(...)`         | `temporal-worker`                    |
| `PARSEABLE_BATCH_EXPORT_TIMEOUT_MS` | `.batchExportTimeoutMs(...)` | `5000`                           |

## Stream pre-creation

Parseable requires streams to exist before ingestion. Create them once:

```bash
# Log stream
curl -X PUT https://<endpoint>/api/v1/logstream \
     -H "X-P-Stream: temporal-logs" \
     -u "$PARSEABLE_USERNAME:$PARSEABLE_PASSWORD"

# Trace stream
curl -X PUT https://<endpoint>/api/v1/logstream \
     -H "X-P-Stream: temporal-traces" \
     -u "$PARSEABLE_USERNAME:$PARSEABLE_PASSWORD"
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
├── ParseableConfigTest.java                    # unit tests for config validation + defaults
├── SanitizingSpanExporterTest.java             # unit tests for attribute sanitization
├── ParseablePluginTest.java                    # unit tests for plugin configuration
└── ParseableInterceptorTest.java               # in-process Temporal interceptor tests
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

**Restart tolerance.** Each lifecycle event emits its own closed span, keyed with workflow ID and
run ID. In-memory span context is used only for best-effort parent/child correlation, so terminal
events still export after a worker restart.

**Client operations.** The workflow client interceptor records starts, signals, queries, updates,
cancels, and terminations as short `CLIENT` spans/logs. It records operation names and workflow
identifiers, but not arguments or payloads.

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
| `ParseableConfigTest` | Unit | Required fields, builder overrides, endpoint derivation |
| `ParseablePluginTest` | Unit | Service/client/factory configuration, duplicate interceptor guard |
| `SanitizingSpanExporterTest` | Unit | Primitive pass-through, array flattening, flush/shutdown delegation |
| `ParseableInterceptorTest` | Integration | Workflow + activity event emission, replay-safety assertion |

Run tests:
```bash
mvn test
```

Run all tests including integration:
```bash
mvn test -Pintegration
```

Compile examples against the current checkout:
```bash
mvn install -DskipTests -Dgpg.skip=true -Dmaven.javadoc.skip=true
mvn -f examples/pom.xml compile
```

## Running the demo

```bash
# Terminal 1 — start Temporal dev server
temporal server start-dev

# Terminal 2 — start Parseable (or point at staging.parseable.com)
# Pre-create streams (see above), then:
mvn install -DskipTests -Dgpg.skip=true -Dmaven.javadoc.skip=true
mvn -f examples/pom.xml compile exec:java -Dexec.mainClass=com.parseable.temporal.example.Worker

# Terminal 3
mvn -f examples/pom.xml compile exec:java -Dexec.mainClass=com.parseable.temporal.example.Client
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
