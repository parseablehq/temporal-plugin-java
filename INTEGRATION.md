# Integration Guide — temporal-parseable (Java)

This guide walks you through installing the Parseable Temporal plugin for Java, configuring your
Parseable instance, and querying the telemetry data it produces.

---

## Prerequisites

- Java 11 or newer
- A running Parseable instance ([self-hosted](https://www.parseable.com/docs/installation) or
  [Parseable Cloud](https://www.parseable.com/docs/cloud))
- Temporal dev server or Temporal Cloud

---

## 1. Add the dependency

**Maven (`pom.xml`):**

```xml
<dependency>
  <groupId>com.parseable</groupId>
  <artifactId>temporal-parseable</artifactId>
  <version>0.1.0</version>
</dependency>
```

**Gradle (`build.gradle`):**

```groovy
implementation 'com.parseable:temporal-parseable:0.1.0'
```

---

## 2. Pre-create Parseable streams

Parseable requires streams to exist before ingestion. Create them once with:

```bash
export PARSEABLE_ENDPOINT=https://your-parseable:8000
export PARSEABLE_USERNAME=admin
export PARSEABLE_PASSWORD=password

# Log stream
curl -X PUT "$PARSEABLE_ENDPOINT/api/v1/logstream" \
     -H "X-P-Stream: temporal-logs" \
     -u "$PARSEABLE_USERNAME:$PARSEABLE_PASSWORD"

# Trace stream
curl -X PUT "$PARSEABLE_ENDPOINT/api/v1/logstream" \
     -H "X-P-Stream: temporal-traces" \
     -u "$PARSEABLE_USERNAME:$PARSEABLE_PASSWORD"
```

---

## 3. Configure environment variables

| Variable                            | Default                           | Description                            |
| ----------------------------------- | --------------------------------- | -------------------------------------- |
| `PARSEABLE_ENDPOINT`                | `https://demo.parseable.com:8000` | Base URL of your Parseable instance    |
| `PARSEABLE_USERNAME`                | `admin`                           | HTTP Basic auth username               |
| `PARSEABLE_PASSWORD`                | `password`                        | HTTP Basic auth password               |
| `PARSEABLE_LOG_STREAM`              | `temporal-logs`                   | Stream name for log records            |
| `PARSEABLE_TRACE_STREAM`            | `temporal-traces`                 | Stream name for trace spans            |
| `PARSEABLE_TEMPORAL_HOST`           | `localhost:7233`                  | Temporal server gRPC address           |
| `PARSEABLE_TEMPORAL_NAMESPACE`      | `default`                         | Temporal namespace                     |
| `PARSEABLE_SERVICE_NAME`            | `temporal-worker`                 | OTel `service.name` resource attribute |
| `PARSEABLE_BATCH_EXPORT_TIMEOUT_MS` | `5000`                            | Max ms to wait for a batch export      |

---

## 4. Wire up the plugin

```java
import com.parseable.temporal.ParseableConfig;
import com.parseable.temporal.ParseablePlugin;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerOptions;

ParseableConfig config = ParseableConfig.fromEnv();
ParseablePlugin plugin = new ParseablePlugin(config);

// Register a shutdown hook so in-flight telemetry is flushed before exit
Runtime.getRuntime().addShutdownHook(new Thread(plugin::close));

WorkflowServiceStubs stubs = WorkflowServiceStubs.newServiceStubs(
    plugin.configureServiceStubOptions(WorkflowServiceStubsOptions.newBuilder()).build());

WorkflowClient client = WorkflowClient.newInstance(stubs,
    plugin.configureClientOptions(WorkflowClientOptions.newBuilder()).build());

WorkerFactory factory = WorkerFactory.newInstance(client);

Worker worker = factory.newWorker(
    "my-task-queue",
    plugin.configureWorkerOptions(WorkerOptions.newBuilder()).build());

worker.registerWorkflowImplementationTypes(MyWorkflow.class);
worker.registerActivitiesImplementations(new MyActivitiesImpl());
factory.start();
```

---

## 5. Log record schema

Every log record written to `temporal-logs` has the following fields:

| Field                     | Type   | Description                                                            |
| ------------------------- | ------ | ---------------------------------------------------------------------- |
| `body`                    | string | Human-readable event description, e.g. `workflow.MyWorkflow.completed` |
| `severity`                | string | `INFO` (success) or `ERROR` (failure)                                  |
| `temporal.workflow.id`    | string | Workflow execution ID                                                  |
| `temporal.workflow.type`  | string | Workflow class / type name                                             |
| `temporal.activity.type`  | string | Activity type name (activity events only)                              |
| `temporal.task_queue`     | string | Task queue name                                                        |
| `temporal.status`         | string | `started` \| `completed` \| `failed`                                   |
| `error.message`           | string | Error message (only on `failed` events)                                |
| `temporal.plugin.version` | string | Plugin version (e.g. `0.1.0`)                                          |
| `temporal.plugin.sdk`     | string | Always `java`                                                          |
| `service.name`            | string | OTel `service.name` resource attribute                                 |
| `temporal.namespace`      | string | Temporal namespace                                                     |

---

## 6. Trace span schema

Every span written to `temporal-traces` mirrors the log schema but with additional OTel span
metadata:

| Field            | Type   | Description                                     |
| ---------------- | ------ | ----------------------------------------------- |
| `name`           | string | Span name, e.g. `workflow.MyWorkflow.completed` |
| `kind`           | string | Always `INTERNAL`                               |
| `status.code`    | string | `OK` or `ERROR`                                 |
| `status.message` | string | Error message on `ERROR` spans                  |
| `temporal.*`     | string | Same attributes as log schema above             |

---

## 7. Sample queries

Open the Parseable UI at `<endpoint>` and query your streams with SQL.

**All workflow events in the last hour:**

```sql
SELECT p_timestamp, body, "temporal.workflow.type", "temporal.status"
FROM "temporal-logs"
WHERE p_timestamp > NOW() - INTERVAL '1 hour'
ORDER BY p_timestamp DESC
```

**Failed workflows:**

```sql
SELECT p_timestamp, "temporal.workflow.id", "temporal.workflow.type", "error.message"
FROM "temporal-logs"
WHERE "temporal.status" = 'failed'
ORDER BY p_timestamp DESC
```

**Activity failure rate by type:**

```sql
SELECT "temporal.activity.type",
       COUNT(*) AS total,
       SUM(CASE WHEN "temporal.status" = 'failed' THEN 1 ELSE 0 END) AS failed,
       ROUND(100.0 * SUM(CASE WHEN "temporal.status" = 'failed' THEN 1 ELSE 0 END) / COUNT(*), 2)
         AS failure_pct
FROM "temporal-logs"
WHERE "temporal.activity.type" IS NOT NULL
GROUP BY "temporal.activity.type"
ORDER BY failure_pct DESC
```

**All workflow completions for a specific workflow type:**

```sql
SELECT p_timestamp, "temporal.workflow.id", "temporal.status"
FROM "temporal-logs"
WHERE "temporal.workflow.type" = 'MyWorkflow'
  AND "temporal.status" = 'completed'
ORDER BY p_timestamp DESC
LIMIT 50
```

---

## 8. Caveats

- **Streams must be pre-created.** Parseable does not auto-create streams on first ingest. Run
  the `curl -X PUT` commands in step 2 before starting your worker.

- **Batch export.** Records are batched by the OTel `BatchSpanProcessor` and
  `BatchLogRecordProcessor`. Under low traffic you may see up to a 5 s delay before events appear
  in Parseable. Tune `PARSEABLE_BATCH_EXPORT_TIMEOUT_MS` if needed.

- **Replay safety.** The plugin guards emissions with `Workflow.isReplaying()`. Do not bypass this
  guard if you extend the interceptors.

- **Non-primitive attributes.** Parseable's OTLP ingest rejects array and map-typed span
  attributes. `SanitizingSpanExporter` converts arrays to comma-joined strings and drops map types.
  This is transparent — you do not need to configure it.

- **Graceful shutdown.** Always call `plugin.close()` or register it as a shutdown hook. Without
  it, the in-memory batch buffers may not be flushed before JVM exit.
