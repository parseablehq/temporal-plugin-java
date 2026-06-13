package com.parseable.temporal;

import io.temporal.client.WorkflowClientOptions;
import io.temporal.common.SimplePlugin;
import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.common.interceptors.WorkflowClientInterceptor;
import io.temporal.opentracing.OpenTracingClientInterceptor;
import io.temporal.opentracing.OpenTracingOptions;
import io.temporal.opentracing.OpenTracingWorkerInterceptor;
import io.temporal.worker.WorkerFactoryOptions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Entry point for the Parseable Temporal plugin (Java SDK).
 *
 * <p>Extends {@link SimplePlugin} so it integrates with the standard Temporal plugin system —
 * register it once and it wires Temporal's official
 * {@link OpenTracingClientInterceptor} and {@link OpenTracingWorkerInterceptor} on top of an
 * OpenTelemetry SDK that exports traces (and exposes a logger provider for logs) to Parseable
 * via OTLP/HTTP.
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
  private final OpenTracingClientInterceptor clientInterceptor;
  private final OpenTracingWorkerInterceptor workerInterceptor;

  public ParseablePlugin(ParseableConfig config) {
    this(config, new ParseableEmitter(config));
  }

  ParseablePlugin(ParseableConfig config, ParseableEmitter emitter) {
    super("com.parseable.temporal");
    this.config = config;
    this.emitter = emitter;
    OpenTracingOptions options = OpenTracingOptions.newBuilder()
        .setTracer(emitter.getOpenTracingTracer())
        .build();
    this.clientInterceptor = new OpenTracingClientInterceptor(options);
    this.workerInterceptor = new OpenTracingWorkerInterceptor(options);
  }

  // ── SimplePlugin overrides ────────────────────────────────────────────────

  @Override
  public void configureWorkflowClient(@Nonnull WorkflowClientOptions.Builder builder) {
    super.configureWorkflowClient(builder);
    String ns = config.getTemporalNamespace();
    if (ns != null && !ns.isEmpty()) {
      builder.setNamespace(ns);
    }
    WorkflowClientInterceptor[] existing = builder.build().getInterceptors();
    if (existing != null) {
      for (WorkflowClientInterceptor interceptor : existing) {
        if (interceptor == clientInterceptor) {
          return;
        }
      }
    }
    List<WorkflowClientInterceptor> combined = new ArrayList<>(
        existing != null ? Arrays.asList(existing) : new ArrayList<>());
    combined.add(clientInterceptor);
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
  public OpenTracingClientInterceptor getClientInterceptor() { return clientInterceptor; }
  public OpenTracingWorkerInterceptor getWorkerInterceptor() { return workerInterceptor; }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  /** Flushes pending telemetry and shuts down the OTel SDK. Call before JVM exit. */
  @Override
  public void close() {
    emitter.close();
  }
}
