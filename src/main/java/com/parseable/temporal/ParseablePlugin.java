package com.parseable.temporal;

import com.parseable.temporal.interceptors.ParseableWorkerInterceptor;
import com.parseable.temporal.interceptors.ParseableWorkflowClientInterceptor;

import io.temporal.client.WorkflowClientOptions;
import io.temporal.common.SimplePlugin;
import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.common.interceptors.WorkflowClientInterceptor;
import io.temporal.worker.WorkerFactoryOptions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Entry point for the Parseable Temporal plugin (Java SDK).
 *
 * <p>Extends {@link SimplePlugin} so it integrates with the standard Temporal plugin system —
 * register it once and it configures client and worker factory telemetry hooks automatically.
 *
 * <h2>Quick start</h2>
 *
 * <pre>{@code
 * ParseablePlugin plugin = new ParseablePlugin(ParseableConfig.fromEnv());
 *
 * WorkflowServiceStubs stubs = WorkflowServiceStubs.newServiceStubs(
 *     WorkflowServiceStubsOptions.newBuilder().setPlugins(plugin).build());
 *
 * WorkflowClient client = WorkflowClient.newInstance(stubs);
 *
 * WorkerFactory factory = WorkerFactory.newInstance(client);
 * Worker worker = factory.newWorker("my-queue");
 *
 * worker.registerWorkflowImplementationTypes(MyWorkflow.class);
 * worker.registerActivitiesImplementations(new MyActivitiesImpl());
 * factory.start();
 *
 * Runtime.getRuntime().addShutdownHook(new Thread(plugin::close));
 * }</pre>
 */
public final class ParseablePlugin extends SimplePlugin implements AutoCloseable {

  private final ParseableConfig config;
  private final ParseableEmitter emitter;
  private final ParseableWorkerInterceptor workerInterceptor;
  private final ParseableWorkflowClientInterceptor workflowClientInterceptor;

  public ParseablePlugin(ParseableConfig config) {
    this(config, new ParseableEmitter(config));
  }

  ParseablePlugin(ParseableConfig config, ParseableEmitter emitter) {
    super("com.parseable.temporal");
    this.config = config;
    this.emitter = emitter;
    this.workerInterceptor = new ParseableWorkerInterceptor(emitter);
    this.workflowClientInterceptor = new ParseableWorkflowClientInterceptor(emitter);
  }

  // ── SimplePlugin overrides ────────────────────────────────────────────────

  @Override
  public void configureWorkflowClient(@Nonnull WorkflowClientOptions.Builder builder) {
    super.configureWorkflowClient(builder);
    builder.setNamespace(config.getTemporalNamespace());
    WorkflowClientInterceptor[] existing = builder.build().getInterceptors();
    if (existing != null) {
      for (WorkflowClientInterceptor interceptor : existing) {
        if (interceptor == workflowClientInterceptor) {
          return;
        }
      }
    }
    List<WorkflowClientInterceptor> combined = new ArrayList<>(
        existing != null ? Arrays.asList(existing) : new ArrayList<>());
    combined.add(workflowClientInterceptor);
    builder.setInterceptors(combined.toArray(new WorkflowClientInterceptor[0]));
  }

  @Override
  public void configureWorkerFactory(@Nonnull WorkerFactoryOptions.Builder builder) {
    super.configureWorkerFactory(builder);
    WorkerInterceptor[] existing = builder.build().getWorkerInterceptors();
    if (existing != null) {
      for (WorkerInterceptor interceptor : existing) {
        if (interceptor == workerInterceptor) {
          return;
        }
      }
    }
    List<WorkerInterceptor> combined = new ArrayList<>(
        existing != null ? Arrays.asList(existing) : new ArrayList<>());
    combined.add(workerInterceptor);
    builder.setWorkerInterceptors(combined.toArray(new WorkerInterceptor[0]));
  }

  // ── Accessors ─────────────────────────────────────────────────────────────

  public ParseableConfig getConfig() { return config; }
  public ParseableEmitter getEmitter() { return emitter; }
  public ParseableWorkerInterceptor getWorkerInterceptor() { return workerInterceptor; }
  public ParseableWorkflowClientInterceptor getWorkflowClientInterceptor() {
    return workflowClientInterceptor;
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  /** Flushes pending telemetry and shuts down the OTel SDK. Call before JVM exit. */
  @Override
  public void close() {
    emitter.close();
  }
}
