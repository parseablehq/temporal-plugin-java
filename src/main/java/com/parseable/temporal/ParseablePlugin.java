package com.parseable.temporal;

import com.parseable.temporal.interceptors.ParseableWorkerInterceptor;

import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.WorkerFactoryOptions;

/**
 * Entry point for the Parseable Temporal plugin (Java SDK).
 *
 * <p>Instantiate once, pass the worker interceptor to every worker, then call {@link #close()}
 * on JVM shutdown to flush telemetry.
 *
 * <h3>Quick start</h3>
 *
 * <pre>{@code
 * // 1. Create the plugin (reads PARSEABLE_* env vars)
 * ParseablePlugin plugin = new ParseablePlugin(ParseableConfig.fromEnv());
 *
 * // 2. Connect to Temporal
 * WorkflowServiceStubs stubs = WorkflowServiceStubs.newServiceStubs(
 *     plugin.configureServiceStubOptions(
 *         WorkflowServiceStubsOptions.newBuilder()
 *             .setTarget(config.getTemporalHost()))
 *         .build());
 *
 * WorkflowClient client = WorkflowClient.newInstance(stubs,
 *     WorkflowClientOptions.newBuilder()
 *         .setNamespace(config.getTemporalNamespace())
 *         .build());
 *
 * // 3. Create a worker factory with the interceptor wired in
 * WorkerFactory factory = WorkerFactory.newInstance(client,
 *     plugin.configureWorkerFactoryOptions(WorkerFactoryOptions.newBuilder()).build());
 * Worker worker = factory.newWorker("my-queue");
 *
 * worker.registerWorkflowImplementationTypes(MyWorkflow.class);
 * worker.registerActivitiesImplementations(new MyActivitiesImpl());
 * factory.start();
 *
 * // 4. Shutdown cleanly
 * Runtime.getRuntime().addShutdownHook(new Thread(plugin::close));
 * }</pre>
 */
public final class ParseablePlugin implements AutoCloseable {

  private final ParseableConfig config;
  private final ParseableEmitter emitter;
  private final ParseableWorkerInterceptor workerInterceptor;

  public ParseablePlugin(ParseableConfig config) {
    this.config = config;
    this.emitter = new ParseableEmitter(config);
    this.workerInterceptor = new ParseableWorkerInterceptor(emitter);
  }

  // ── Configuration helpers ─────────────────────────────────────────────────

  /**
   * Adds the Parseable worker interceptor to a {@link WorkerFactoryOptions.Builder}.
   *
   * <pre>{@code
   * WorkerFactory factory = WorkerFactory.newInstance(client,
   *     plugin.configureWorkerFactoryOptions(WorkerFactoryOptions.newBuilder()).build());
   * }</pre>
   */
  public WorkerFactoryOptions.Builder configureWorkerFactoryOptions(
      WorkerFactoryOptions.Builder builder) {
    return builder.setWorkerInterceptors(workerInterceptor);
  }

  /**
   * Applies the Temporal host from {@link ParseableConfig} to a
   * {@link WorkflowServiceStubsOptions.Builder}.
   */
  public WorkflowServiceStubsOptions.Builder configureServiceStubOptions(
      WorkflowServiceStubsOptions.Builder builder) {
    return builder.setTarget(config.getTemporalHost());
  }

  /**
   * Applies the namespace from {@link ParseableConfig} to a {@link WorkflowClientOptions.Builder}.
   */
  public WorkflowClientOptions.Builder configureClientOptions(
      WorkflowClientOptions.Builder builder) {
    return builder.setNamespace(config.getTemporalNamespace());
  }

  // ── Accessors ─────────────────────────────────────────────────────────────

  public ParseableConfig getConfig() { return config; }
  public ParseableEmitter getEmitter() { return emitter; }
  public ParseableWorkerInterceptor getWorkerInterceptor() { return workerInterceptor; }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  /**
   * Flushes pending telemetry and shuts down the OTel SDK. Call this before your JVM exits.
   */
  @Override
  public void close() {
    emitter.close();
  }
}
