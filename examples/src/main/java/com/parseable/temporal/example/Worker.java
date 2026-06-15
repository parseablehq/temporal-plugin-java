package com.parseable.temporal.example;

import com.parseable.temporal.ParseableConfig;
import com.parseable.temporal.ParseablePlugin;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.WorkerFactory;

/**
 * Demo worker that registers workflows and activities with the Parseable plugin.
 *
 * <p>Prerequisites:
 * <ol>
 *   <li>Temporal dev server running: {@code temporal server start-dev}</li>
 *   <li>Parseable running: {@code docker run -p 8000:8000 parseable/parseable ...}</li>
 *   <li>Streams pre-created:
 *   <pre>
 *     curl -X PUT "$PARSEABLE_ENDPOINT/api/v1/logstream" \
 *          -H "X-P-Stream: temporal-logs" \
 *          -u "$PARSEABLE_USERNAME:$PARSEABLE_PASSWORD"
 *
 *     curl -X PUT "$PARSEABLE_ENDPOINT/api/v1/logstream" \
 *          -H "X-P-Stream: temporal-traces" \
 *          -u "$PARSEABLE_USERNAME:$PARSEABLE_PASSWORD"
 *   </pre></li>
 * </ol>
 *
 * <p>Required env vars:
 * <ul>
 *   <li>{@code PARSEABLE_ENDPOINT} — e.g. {@code https://parseable.example.com}</li>
 *   <li>{@code PARSEABLE_USERNAME}</li>
 *   <li>{@code PARSEABLE_PASSWORD}</li>
 * </ul>
 *
 * <p>Optional env vars (with defaults):
 * <ul>
 *   <li>{@code PARSEABLE_LOG_STREAM} — default: {@code temporal-logs}</li>
 *   <li>{@code PARSEABLE_TRACE_STREAM} — default: {@code temporal-traces}</li>
 *   <li>{@code TEMPORAL_TARGET} — default: {@code localhost:7233}</li>
 * </ul>
 */
public final class Worker {

  private static final String TASK_QUEUE = "parseable-demo-queue";

  public static void main(String[] args) throws InterruptedException {
    ParseablePlugin plugin = new ParseablePlugin(ParseableConfig.fromEnv());
    String temporalTarget = System.getenv().getOrDefault("TEMPORAL_TARGET", "localhost:7233");
    Runtime.getRuntime().addShutdownHook(new Thread(plugin::close));

    WorkflowServiceStubs stubs = WorkflowServiceStubs.newServiceStubs(
        WorkflowServiceStubsOptions.newBuilder()
            .setTarget(temporalTarget)
            .setPlugins(plugin)
            .build());

    WorkflowClient client = WorkflowClient.newInstance(stubs);

    WorkerFactory factory = WorkerFactory.newInstance(client);

    io.temporal.worker.Worker worker = factory.newWorker(TASK_QUEUE);

    worker.registerWorkflowImplementationTypes(
        Workflows.GreetingWorkflowImpl.class,
        Workflows.FailingWorkflowImpl.class);
    worker.registerActivitiesImplementations(new Workflows.GreetingActivitiesImpl());

    factory.start();
    System.out.println("Worker started on task queue: " + TASK_QUEUE);
    System.out.println("Run Client in a separate terminal to trigger workflows.");

    Thread.currentThread().join();
  }
}
