package com.parseable.temporal.example;

import com.parseable.temporal.ParseableConfig;
import com.parseable.temporal.ParseablePlugin;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;

/**
 * Demo worker that registers workflows and activities with the Parseable plugin.
 *
 * <p>Prerequisites:
 * <ol>
 *   <li>Temporal dev server running: {@code temporal server start-dev}</li>
 *   <li>Parseable running: {@code docker run -p 8000:8000 parseable/parseable ...}</li>
 *   <li>Streams pre-created:
 *   <pre>
 *     curl -X PUT https://demo.parseable.com:8000/api/v1/logstream \
 *          -H "X-P-Stream: temporal-logs" \
 *          -u admin:password
 *
 *     curl -X PUT https://demo.parseable.com:8000/api/v1/logstream \
 *          -H "X-P-Stream: temporal-traces" \
 *          -u admin:password
 *   </pre></li>
 * </ol>
 *
 * <p>Set env vars or use defaults:
 * <ul>
 *   <li>{@code PARSEABLE_ENDPOINT} — default: {@code https://demo.parseable.com:8000}</li>
 *   <li>{@code PARSEABLE_USERNAME} — default: {@code admin}</li>
 *   <li>{@code PARSEABLE_PASSWORD} — default: {@code password}</li>
 *   <li>{@code PARSEABLE_LOG_STREAM} — default: {@code temporal-logs}</li>
 *   <li>{@code PARSEABLE_TRACE_STREAM} — default: {@code temporal-traces}</li>
 *   <li>{@code PARSEABLE_TEMPORAL_HOST} — default: {@code localhost:7233}</li>
 * </ul>
 */
public final class Worker {

  private static final String TASK_QUEUE = "parseable-demo-queue";

  public static void main(String[] args) throws InterruptedException {
    ParseableConfig config = ParseableConfig.fromEnv();
    System.out.println("Starting worker with config: " + config);

    ParseablePlugin plugin = new ParseablePlugin(config);
    Runtime.getRuntime().addShutdownHook(new Thread(plugin::close));

    WorkflowServiceStubs stubs = WorkflowServiceStubs.newServiceStubs(
        plugin.configureServiceStubOptions(
            WorkflowServiceStubsOptions.newBuilder())
            .build());

    WorkflowClient client = WorkflowClient.newInstance(stubs,
        plugin.configureClientOptions(
            io.temporal.client.WorkflowClientOptions.newBuilder())
            .build());

    WorkerFactory factory = WorkerFactory.newInstance(client,
        plugin.configureWorkerFactoryOptions(WorkerFactoryOptions.newBuilder()).build());

    io.temporal.worker.Worker worker = factory.newWorker(TASK_QUEUE);

    worker.registerWorkflowImplementationTypes(
        Workflows.GreetingWorkflowImpl.class,
        Workflows.FailingWorkflowImpl.class);
    worker.registerActivitiesImplementations(new Workflows.GreetingActivitiesImpl());

    factory.start();
    System.out.println("Worker started, listening on task queue: " + TASK_QUEUE);
    System.out.println("Run the Client in a separate terminal to trigger workflows.");

    // Block until interrupted
    Thread.currentThread().join();
  }
}
