package com.parseable.temporal;

import com.parseable.temporal.exporters.SanitizingSpanExporter;
import com.parseable.temporal.version.Version;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

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
  private static final AttributeKey<String> WORKFLOW_TYPE_KEY = AttributeKey.stringKey("temporal.workflow.type");
  private static final AttributeKey<String> ACTIVITY_TYPE_KEY = AttributeKey.stringKey("temporal.activity.type");
  private static final AttributeKey<String> TASK_QUEUE_KEY = AttributeKey.stringKey("temporal.task_queue");
  private static final AttributeKey<String> STATUS_KEY = AttributeKey.stringKey("temporal.status");
  private static final AttributeKey<String> PLUGIN_VERSION_KEY = AttributeKey.stringKey("temporal.plugin.version");
  private static final AttributeKey<String> SDK_KEY = AttributeKey.stringKey("temporal.plugin.sdk");

  private final ParseableConfig config;
  private final OpenTelemetrySdk sdk;
  private final Tracer tracer;
  private final Logger logger;

  public ParseableEmitter(ParseableConfig config) {
    this.config = config;
    this.sdk = buildSdk(config);
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
   * @param status       "started" | "completed" | "failed"
   * @param errorMessage nullable; only set when status == "failed"
   */
  public void emitWorkflowEvent(
      String workflowId,
      String workflowType,
      String taskQueue,
      String status,
      String errorMessage) {

    String spanName = "workflow." + workflowType + "." + status;
    Attributes attrs = Attributes.builder()
        .put(WORKFLOW_ID_KEY, workflowId)
        .put(WORKFLOW_TYPE_KEY, workflowType)
        .put(TASK_QUEUE_KEY, taskQueue)
        .put(STATUS_KEY, status)
        .put(PLUGIN_VERSION_KEY, Version.PLUGIN_VERSION)
        .put(SDK_KEY, "java")
        .build();

    emitSpan(spanName, SpanKind.INTERNAL, attrs, errorMessage);
    emitLog(spanName, status, attrs, errorMessage);
  }

  /**
   * Emits an activity lifecycle event (start / complete / fail).
   *
   * @param workflowId   Parent workflow ID
   * @param activityType Activity class/type name
   * @param taskQueue    Task queue name
   * @param status       "started" | "completed" | "failed"
   * @param errorMessage nullable; only set when status == "failed"
   */
  public void emitActivityEvent(
      String workflowId,
      String activityType,
      String taskQueue,
      String status,
      String errorMessage) {

    String spanName = "activity." + activityType + "." + status;
    Attributes attrs = Attributes.builder()
        .put(WORKFLOW_ID_KEY, workflowId)
        .put(ACTIVITY_TYPE_KEY, activityType)
        .put(TASK_QUEUE_KEY, taskQueue)
        .put(STATUS_KEY, status)
        .put(PLUGIN_VERSION_KEY, Version.PLUGIN_VERSION)
        .put(SDK_KEY, "java")
        .build();

    emitSpan(spanName, SpanKind.INTERNAL, attrs, errorMessage);
    emitLog(spanName, status, attrs, errorMessage);
  }

  // ── Internal helpers ──────────────────────────────────────────────────────

  private void emitSpan(String name, SpanKind kind, Attributes attrs, String errorMessage) {
    Span span = tracer.spanBuilder(name).setSpanKind(kind).startSpan();
    try {
      span.setAllAttributes(attrs);
      if (errorMessage != null) {
        span.setStatus(StatusCode.ERROR, errorMessage);
        span.setAttribute(AttributeKey.stringKey("error.message"), errorMessage);
      } else {
        span.setStatus(StatusCode.OK);
      }
    } finally {
      span.end();
    }
  }

  private void emitLog(String body, String status, Attributes attrs, String errorMessage) {
    Severity severity = "failed".equals(status) ? Severity.ERROR : Severity.INFO;
    var builder = logger.logRecordBuilder()
        .setSeverity(severity)
        .setBody(body)
        .setAllAttributes(attrs);
    if (errorMessage != null) {
      builder.setAttribute(AttributeKey.stringKey("error.message"), errorMessage);
    }
    builder.emit();
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
    return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());
  }

  @Override
  public void close() {
    sdk.getSdkTracerProvider().forceFlush().join(10, TimeUnit.SECONDS);
    sdk.getSdkLoggerProvider().forceFlush().join(10, TimeUnit.SECONDS);
    sdk.getSdkTracerProvider().shutdown().join(10, TimeUnit.SECONDS);
    sdk.getSdkLoggerProvider().shutdown().join(10, TimeUnit.SECONDS);
  }
}
