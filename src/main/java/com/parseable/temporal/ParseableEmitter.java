package com.parseable.temporal;

import com.parseable.temporal.exporters.SanitizingSpanExporter;
import com.parseable.temporal.version.Version;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.opentracingshim.OpenTracingShim;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * Owns the {@link OpenTelemetrySdk} that exports traces and logs to Parseable via OTLP/HTTP.
 *
 * <p>Tracing instrumentation is delegated to Temporal's official
 * {@code temporal-opentracing} module via the OpenTracing → OpenTelemetry shim
 * exposed by {@link #getOpenTracingTracer()}.
 *
 * <p>Application code may use {@link #getOpenTelemetry()} to obtain the configured SDK
 * (for example, to register an OTel log appender pointed at Parseable's log stream).
 *
 * <p>Call {@link #close()} (or let {@link ParseablePlugin#close()} do it) to flush and shut
 * down both providers before your JVM exits.
 */
public class ParseableEmitter implements AutoCloseable {

  static final String INSTRUMENTATION_SCOPE = "temporal-parseable";

  // OTel header names required by Parseable
  private static final String HEADER_STREAM = "X-P-Stream";
  private static final String HEADER_LOG_SOURCE = "X-P-Log-Source";
  private static final String LOG_SOURCE_LOGS = "otel-logs";
  private static final String LOG_SOURCE_TRACES = "otel-traces";

  private static final AttributeKey<String> PLUGIN_VERSION_KEY =
      AttributeKey.stringKey("temporal.plugin.version");
  private static final AttributeKey<String> SDK_KEY = AttributeKey.stringKey("temporal.plugin.sdk");

  private final OpenTelemetrySdk sdk;
  private final Tracer tracer;
  private final io.opentracing.Tracer openTracingTracer;

  public ParseableEmitter(ParseableConfig config) {
    this(config, buildSdk(config));
  }

  ParseableEmitter(ParseableConfig config, OpenTelemetrySdk sdk) {
    this.sdk = sdk;
    this.tracer = sdk.getTracerProvider().get(INSTRUMENTATION_SCOPE, Version.PLUGIN_VERSION);
    this.openTracingTracer = OpenTracingShim.createTracerShim(sdk);
  }

  /** OpenTelemetry SDK configured to export to Parseable. */
  public OpenTelemetry getOpenTelemetry() {
    return sdk;
  }

  /** OpenTracing tracer bridged to the OTel SDK; passed to Temporal's interceptors. */
  public io.opentracing.Tracer getOpenTracingTracer() {
    return openTracingTracer;
  }

  /** OTel tracer scoped to {@code temporal-parseable}; for plugin-internal use. */
  Tracer getTracer() {
    return tracer;
  }

  // ── SDK construction ──────────────────────────────────────────────────────

  private static OpenTelemetrySdk buildSdk(ParseableConfig config) {
    String authHeader = basicAuthHeader(config.getUsername(), config.getPassword());
    Duration timeout = config.getBatchExportTimeout();

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
    AttributesBuilder b = Attributes.builder()
        .put(AttributeKey.stringKey("service.name"), config.getServiceName())
        .put(PLUGIN_VERSION_KEY, Version.PLUGIN_VERSION)
        .put(SDK_KEY, "java");
    if (config.getTemporalNamespace() != null && !config.getTemporalNamespace().isEmpty()) {
      b.put(AttributeKey.stringKey("temporal.namespace"), config.getTemporalNamespace());
    }
    return Resource.create(b.build());
  }

  private static String basicAuthHeader(String username, String password) {
    String credentials = username + ":" + password;
    return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void close() {
    sdk.getSdkTracerProvider().forceFlush().join(10, TimeUnit.SECONDS);
    sdk.getSdkLoggerProvider().forceFlush().join(10, TimeUnit.SECONDS);
    sdk.getSdkTracerProvider().shutdown().join(10, TimeUnit.SECONDS);
    sdk.getSdkLoggerProvider().shutdown().join(10, TimeUnit.SECONDS);
  }
}
