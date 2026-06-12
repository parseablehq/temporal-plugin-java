package com.parseable.temporal;

import com.parseable.temporal.exporters.SanitizingSpanExporter;
import com.parseable.temporal.version.Version;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

/**
 * Shared emitter used by all Parseable interceptors.
 *
 * <p>Builds and owns the {@link SdkTracerProvider} and {@link SdkLoggerProvider} that export
 * via OTLP/HTTP directly to Parseable — no collector required.
 *
 * <p>Call {@link #close()} (or let {@link ParseablePlugin#close()} do it) to flush and shut down
 * both providers before your JVM exits.
 */
public class ParseableEmitter implements AutoCloseable {

  static final String INSTRUMENTATION_SCOPE = "temporal-parseable";

  // OTel header names required by Parseable
  private static final String HEADER_STREAM = "X-P-Stream";
  private static final String HEADER_LOG_SOURCE = "X-P-Log-Source";
  private static final String LOG_SOURCE_LOGS = "otel-logs";
  private static final String LOG_SOURCE_TRACES = "otel-traces";

  // Span attribute keys
  private static final AttributeKey<String> WORKFLOW_ID_KEY = AttributeKey.stringKey("temporal.workflow.id");
  private static final AttributeKey<String> WORKFLOW_RUN_ID_KEY =
      AttributeKey.stringKey("temporal.workflow.run_id");
  private static final AttributeKey<String> WORKFLOW_TYPE_KEY = AttributeKey.stringKey("temporal.workflow.type");
  private static final AttributeKey<String> ACTIVITY_ID_KEY = AttributeKey.stringKey("temporal.activity.id");
  private static final AttributeKey<String> ACTIVITY_TYPE_KEY = AttributeKey.stringKey("temporal.activity.type");
  private static final AttributeKey<String> OPERATION_KEY = AttributeKey.stringKey("temporal.operation");
  private static final AttributeKey<String> OPERATION_KIND_KEY =
      AttributeKey.stringKey("temporal.operation.kind");
  private static final AttributeKey<String> SIGNAL_NAME_KEY = AttributeKey.stringKey("temporal.signal.name");
  private static final AttributeKey<String> QUERY_TYPE_KEY = AttributeKey.stringKey("temporal.query.type");
  private static final AttributeKey<String> UPDATE_NAME_KEY = AttributeKey.stringKey("temporal.update.name");
  private static final AttributeKey<String> UPDATE_ID_KEY = AttributeKey.stringKey("temporal.update.id");
  private static final AttributeKey<String> TASK_QUEUE_KEY = AttributeKey.stringKey("temporal.task_queue");
  private static final AttributeKey<String> STATUS_KEY = AttributeKey.stringKey("temporal.status");
  private static final AttributeKey<Long> DURATION_MS_KEY = AttributeKey.longKey("temporal.duration_ms");
  private static final AttributeKey<String> PLUGIN_VERSION_KEY = AttributeKey.stringKey("temporal.plugin.version");
  private static final AttributeKey<String> SDK_KEY = AttributeKey.stringKey("temporal.plugin.sdk");

  // Bounded to cap memory if terminal events are lost (worker crash, dropped interceptor call).
  // 10k entries is enough for correlation while keeping the cache small.
  private static final int MAX_SPAN_CONTEXTS = 10_000;

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ParseableEmitter.class);

  private final ParseableConfig config;
  private final OpenTelemetrySdk sdk;
  private final Tracer tracer;
  private final Logger logger;
  // Maps workflowId:runId to exported span contexts so later events can be parented.
  private final Map<String, SpanContext> workflowSpanContexts = boundedSpanContextMap();
  private final Map<String, SpanContext> activitySpanContexts = boundedSpanContextMap();

  private static Map<String, SpanContext> boundedSpanContextMap() {
    return Collections.synchronizedMap(new LinkedHashMap<String, SpanContext>(256, 0.75f, false) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, SpanContext> eldest) {
        if (size() > MAX_SPAN_CONTEXTS) {
          LOG.warn("span context cache exceeded {} entries; evicting eldest", MAX_SPAN_CONTEXTS);
          return true;
        }
        return false;
      }
    });
  }

  public ParseableEmitter(ParseableConfig config) {
    this(config, buildSdk(config));
  }

  ParseableEmitter(ParseableConfig config, OpenTelemetrySdk sdk) {
    this.config = config;
    this.sdk = sdk;
    this.tracer = sdk.getTracerProvider().get(INSTRUMENTATION_SCOPE, Version.PLUGIN_VERSION);
    this.logger = sdk.getSdkLoggerProvider().get(INSTRUMENTATION_SCOPE);
  }

  // ── Public emit API ───────────────────────────────────────────────────────

  /**
   * Emits a workflow lifecycle event (start / complete / fail) as both a span and a log record.
   *
   * @param workflowId   Temporal workflow ID
   * @param workflowType Workflow class/type name
   * @param taskQueue    Task queue name
   * @param status       "started" | "completed" | "failed" | "canceled"
   * @param errorMessage nullable; only used when status == "failed"
   */
  public void emitWorkflowEvent(
      String workflowId,
      String workflowType,
      String taskQueue,
      String status,
      String errorMessage) {
    emitWorkflowEvent(workflowId, null, workflowType, taskQueue, status, errorMessage);
  }

  /**
   * Emits a workflow lifecycle event (start / complete / fail) as both a span and a log record.
   *
   * @param workflowId   Temporal workflow ID
   * @param runId        Temporal workflow run ID
   * @param workflowType Workflow class/type name
   * @param taskQueue    Task queue name
   * @param status       "started" | "completed" | "failed" | "canceled"
   * @param errorMessage nullable; only used when status == "failed"
   */
  public void emitWorkflowEvent(
      String workflowId,
      String runId,
      String workflowType,
      String taskQueue,
      String status,
      String errorMessage) {

    String spanName = "workflow." + workflowType;
    String logBody = spanName + "." + status;
    AttributesBuilder attrsBuilder = Attributes.builder()
        .put(WORKFLOW_ID_KEY, workflowId)
        .put(WORKFLOW_TYPE_KEY, workflowType)
        .put(TASK_QUEUE_KEY, taskQueue)
        .put(STATUS_KEY, status)
        .put(PLUGIN_VERSION_KEY, Version.PLUGIN_VERSION)
        .put(SDK_KEY, "java");
    if (runId != null) {
      attrsBuilder.put(WORKFLOW_RUN_ID_KEY, runId);
    }
    Attributes attrs = attrsBuilder.build();

    String workflowKey = workflowKey(workflowId, runId);
    SpanContext parentSpanContext = workflowSpanContexts.get(workflowKey);
    Context parent = contextWithSpan(parentSpanContext);
    SpanContext emittedSpanContext = emitSpan(
        spanName, SpanKind.INTERNAL, parent, attrs, status, errorMessage);
    if ("started".equals(status)) {
      workflowSpanContexts.put(workflowKey, emittedSpanContext);
    } else {
      workflowSpanContexts.remove(workflowKey);
    }
    emitLog(logBody, status, attrs, errorMessage);
  }

  /**
   * Emits an activity lifecycle event (start / complete / fail).
   *
   * @param workflowId   Parent workflow ID
   * @param activityType Activity class/type name
   * @param taskQueue    Task queue name
   * @param status       "started" | "completed" | "failed" | "canceled"
   * @param errorMessage nullable; only used when status == "failed"
   */
  public void emitActivityEvent(
      String workflowId,
      String activityType,
      String taskQueue,
      String status,
      String errorMessage) {
    emitActivityEvent(workflowId, null, activityType, activityType, taskQueue, status, errorMessage);
  }

  /**
   * Emits an activity lifecycle event (start / complete / fail).
   *
   * @param workflowId   Parent workflow ID
   * @param activityId   Temporal activity ID
   * @param activityType Activity class/type name
   * @param taskQueue    Task queue name
   * @param status       "started" | "completed" | "failed" | "canceled"
   * @param errorMessage nullable; only used when status == "failed"
   */
  public void emitActivityEvent(
      String workflowId,
      String activityId,
      String activityType,
      String taskQueue,
      String status,
      String errorMessage) {
    emitActivityEvent(workflowId, null, activityId, activityType, taskQueue, status, errorMessage);
  }

  /**
   * Emits an activity lifecycle event (start / complete / fail).
   *
   * @param workflowId   Parent workflow ID
   * @param runId        Parent workflow run ID
   * @param activityId   Temporal activity ID
   * @param activityType Activity class/type name
   * @param taskQueue    Task queue name
   * @param status       "started" | "completed" | "failed" | "canceled"
   * @param errorMessage nullable; only used when status == "failed"
   */
  public void emitActivityEvent(
      String workflowId,
      String runId,
      String activityId,
      String activityType,
      String taskQueue,
      String status,
      String errorMessage) {

    String spanName = "activity." + activityType;
    String logBody = spanName + "." + status;
    AttributesBuilder attrsBuilder = Attributes.builder()
        .put(WORKFLOW_ID_KEY, workflowId)
        .put(ACTIVITY_ID_KEY, activityId)
        .put(ACTIVITY_TYPE_KEY, activityType)
        .put(TASK_QUEUE_KEY, taskQueue)
        .put(STATUS_KEY, status)
        .put(PLUGIN_VERSION_KEY, Version.PLUGIN_VERSION)
        .put(SDK_KEY, "java");
    if (runId != null) {
      attrsBuilder.put(WORKFLOW_RUN_ID_KEY, runId);
    }
    Attributes attrs = attrsBuilder.build();

    String activityKey = activityKey(workflowId, runId, activityId);
    SpanContext parentSpanContext = activitySpanContexts.get(activityKey);
    if (parentSpanContext == null) {
      parentSpanContext = workflowSpanContexts.get(workflowKey(workflowId, runId));
    }
    Context parent = contextWithSpan(parentSpanContext);
    SpanContext emittedSpanContext = emitSpan(
        spanName, SpanKind.INTERNAL, parent, attrs, status, errorMessage);
    if ("started".equals(status)) {
      activitySpanContexts.put(activityKey, emittedSpanContext);
    } else {
      activitySpanContexts.remove(activityKey);
    }
    emitLog(logBody, status, attrs, errorMessage);
  }

  /**
   * Emits a client-side Temporal operation such as start, signal, query, update, cancel, or
   * terminate. Arguments and payloads are intentionally excluded.
   */
  public void emitClientOperation(
      String operation,
      String workflowId,
      String runId,
      String workflowType,
      String taskQueue,
      String signalName,
      String queryType,
      String updateName,
      String updateId,
      String status,
      String errorMessage,
      long startEpochNanos,
      long endEpochNanos) {

    String spanName = "temporal.client." + operation;
    String logBody = spanName + "." + status;
    Attributes attrs = clientOperationAttributes(
        operation, workflowId, runId, workflowType, taskQueue, signalName, queryType, updateName,
        updateId, status, durationMillis(startEpochNanos, endEpochNanos));

    emitSpan(
        spanName, SpanKind.CLIENT, Context.root(), attrs, status, errorMessage, startEpochNanos,
        endEpochNanos);
    emitLog(logBody, status, attrs, errorMessage);
  }

  // ── Internal helpers ──────────────────────────────────────────────────────

  private SpanContext emitSpan(
      String name,
      SpanKind kind,
      Context parentContext,
      Attributes attrs,
      String status,
      String errorMessage) {
    Span span = startSpan(name, kind, parentContext, attrs);
    finishSpan(span, attrs, status, errorMessage);
    return span.getSpanContext();
  }

  private SpanContext emitSpan(
      String name,
      SpanKind kind,
      Context parentContext,
      Attributes attrs,
      String status,
      String errorMessage,
      long startEpochNanos,
      long endEpochNanos) {
    Span span = startSpan(name, kind, parentContext, attrs, startEpochNanos);
    finishSpan(span, attrs, status, errorMessage, endEpochNanos);
    return span.getSpanContext();
  }

  private Context contextWithSpan(SpanContext spanContext) {
    return spanContext != null && spanContext.isValid()
        ? Context.root().with(Span.wrap(spanContext))
        : Context.root();
  }

  private Span startSpan(String name, SpanKind kind, Context parentContext, Attributes attrs) {
    Span span = tracer.spanBuilder(name)
        .setSpanKind(kind)
        .setParent(parentContext)
        .startSpan();
    span.setAllAttributes(attrs);
    return span;
  }

  private Span startSpan(
      String name, SpanKind kind, Context parentContext, Attributes attrs, long startEpochNanos) {
    Span span = tracer.spanBuilder(name)
        .setSpanKind(kind)
        .setParent(parentContext)
        .setStartTimestamp(startEpochNanos, TimeUnit.NANOSECONDS)
        .startSpan();
    span.setAllAttributes(attrs);
    return span;
  }

  private void finishSpan(Span span, Attributes attrs, String status, String errorMessage) {
    try {
      setTerminalAttributes(span, attrs, status, errorMessage);
    } finally {
      span.end();
    }
  }

  private void finishSpan(
      Span span, Attributes attrs, String status, String errorMessage, long endEpochNanos) {
    try {
      setTerminalAttributes(span, attrs, status, errorMessage);
    } finally {
      span.end(endEpochNanos, TimeUnit.NANOSECONDS);
    }
  }

  private void setTerminalAttributes(
      Span span, Attributes attrs, String status, String errorMessage) {
    span.setAllAttributes(attrs);
    if ("failed".equals(status)) {
      if (errorMessage != null) {
        span.setStatus(StatusCode.ERROR, errorMessage);
        span.setAttribute(AttributeKey.stringKey("error.message"), errorMessage);
      } else {
        span.setStatus(StatusCode.ERROR);
      }
    } else {
      span.setStatus(StatusCode.OK);
    }
  }

  private void emitLog(String body, String status, Attributes attrs, String errorMessage) {
    Severity severity = "failed".equals(status) ? Severity.ERROR : Severity.INFO;
    var builder = logger.logRecordBuilder()
        .setSeverity(severity)
        .setBody(body)
        .setAllAttributes(attrs);
    if ("failed".equals(status) && errorMessage != null) {
      builder.setAttribute(AttributeKey.stringKey("error.message"), errorMessage);
    }
    builder.emit();
  }

  private Attributes clientOperationAttributes(
      String operation,
      String workflowId,
      String runId,
      String workflowType,
      String taskQueue,
      String signalName,
      String queryType,
      String updateName,
      String updateId,
      String status,
      long durationMs) {
    AttributesBuilder builder = Attributes.builder()
        .put(OPERATION_KEY, operation)
        .put(OPERATION_KIND_KEY, "client")
        .put(STATUS_KEY, status)
        .put(DURATION_MS_KEY, durationMs)
        .put(PLUGIN_VERSION_KEY, Version.PLUGIN_VERSION)
        .put(SDK_KEY, "java");
    putIfPresent(builder, WORKFLOW_ID_KEY, workflowId);
    putIfPresent(builder, WORKFLOW_RUN_ID_KEY, runId);
    putIfPresent(builder, WORKFLOW_TYPE_KEY, workflowType);
    putIfPresent(builder, TASK_QUEUE_KEY, taskQueue);
    putIfPresent(builder, SIGNAL_NAME_KEY, signalName);
    putIfPresent(builder, QUERY_TYPE_KEY, queryType);
    putIfPresent(builder, UPDATE_NAME_KEY, updateName);
    putIfPresent(builder, UPDATE_ID_KEY, updateId);
    return builder.build();
  }

  private static void putIfPresent(
      AttributesBuilder builder, AttributeKey<String> key, String value) {
    if (value != null && !value.isEmpty()) {
      builder.put(key, value);
    }
  }

  private static long durationMillis(long startEpochNanos, long endEpochNanos) {
    return TimeUnit.NANOSECONDS.toMillis(Math.max(0, endEpochNanos - startEpochNanos));
  }

  // ── SDK construction ──────────────────────────────────────────────────────

  private static OpenTelemetrySdk buildSdk(ParseableConfig config) {
    String authHeader = basicAuthHeader(config.getUsername(), config.getPassword());
    Duration timeout = config.getBatchExportTimeout();

    // ── Span exporter (traces) ──────────────────────────────────────────
    OtlpHttpSpanExporter rawSpanExporter = OtlpHttpSpanExporter.builder()
        .setEndpoint(config.tracesEndpoint())
        .addHeader("Authorization", authHeader)
        .addHeader(HEADER_STREAM, config.getTraceStream())
        .addHeader(HEADER_LOG_SOURCE, LOG_SOURCE_TRACES)
        .setTimeout(timeout)
        .build();

    SanitizingSpanExporter spanExporter = new SanitizingSpanExporter(rawSpanExporter);

    SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(BatchSpanProcessor.builder(spanExporter)
            .setExporterTimeout(timeout)
            .build())
        .setResource(resource(config))
        .build();

    // ── Log exporter ────────────────────────────────────────────────────
    OtlpHttpLogRecordExporter logExporter = OtlpHttpLogRecordExporter.builder()
        .setEndpoint(config.logsEndpoint())
        .addHeader("Authorization", authHeader)
        .addHeader(HEADER_STREAM, config.getLogStream())
        .addHeader(HEADER_LOG_SOURCE, LOG_SOURCE_LOGS)
        .setTimeout(timeout)
        .build();

    SdkLoggerProvider loggerProvider = SdkLoggerProvider.builder()
        .addLogRecordProcessor(BatchLogRecordProcessor.builder(logExporter)
            .setExporterTimeout(timeout)
            .build())
        .setResource(resource(config))
        .build();

    return OpenTelemetrySdk.builder()
        .setTracerProvider(tracerProvider)
        .setLoggerProvider(loggerProvider)
        .build();
  }

  private static Resource resource(ParseableConfig config) {
    return Resource.create(Attributes.builder()
        .put(AttributeKey.stringKey("service.name"), config.getServiceName())
        .put(AttributeKey.stringKey("temporal.namespace"), config.getTemporalNamespace())
        .put(PLUGIN_VERSION_KEY, Version.PLUGIN_VERSION)
        .put(SDK_KEY, "java")
        .build());
  }

  private static String basicAuthHeader(String username, String password) {
    String credentials = username + ":" + password;
    return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
  }

  private static String workflowKey(String workflowId, String runId) {
    return workflowId + ":" + (runId != null ? runId : "");
  }

  private static String activityKey(String workflowId, String runId, String activityId) {
    return workflowKey(workflowId, runId) + ":" + activityId;
  }

  @Override
  public void close() {
    sdk.getSdkTracerProvider().forceFlush().join(10, TimeUnit.SECONDS);
    sdk.getSdkLoggerProvider().forceFlush().join(10, TimeUnit.SECONDS);
    sdk.getSdkTracerProvider().shutdown().join(10, TimeUnit.SECONDS);
    sdk.getSdkLoggerProvider().shutdown().join(10, TimeUnit.SECONDS);
  }
}
