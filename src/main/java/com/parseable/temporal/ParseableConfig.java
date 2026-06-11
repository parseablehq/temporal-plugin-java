package com.parseable.temporal;

import java.time.Duration;

/**
 * Configuration for the Parseable Temporal plugin.
 *
 * <p>All fields are read from {@code PARSEABLE_*} environment variables when using
 * {@link ParseableConfig#fromEnv()}. You can also construct one programmatically via the
 * {@link Builder}.
 *
 * <pre>{@code
 * // From environment variables:
 * ParseableConfig config = ParseableConfig.fromEnv();
 *
 * // Programmatic:
 * ParseableConfig config = ParseableConfig.builder()
 *     .endpoint("https://parseable.example.com")
 *     .username(System.getenv("PARSEABLE_USERNAME"))
 *     .password(System.getenv("PARSEABLE_PASSWORD"))
 *     .logStream("temporal-logs")
 *     .traceStream("temporal-traces")
 *     .build();
 * }</pre>
 */
public final class ParseableConfig {

  /** {@code PARSEABLE_ENDPOINT} — base URL of your Parseable instance. */
  private final String endpoint;

  /** {@code PARSEABLE_USERNAME} */
  private final String username;

  /** {@code PARSEABLE_PASSWORD} */
  private final String password;

  /** {@code PARSEABLE_LOG_STREAM} — stream name for OTLP log records. */
  private final String logStream;

  /** {@code PARSEABLE_TRACE_STREAM} — stream name for OTLP span records. */
  private final String traceStream;

  /** {@code PARSEABLE_TEMPORAL_HOST} — Temporal server address. */
  private final String temporalHost;

  /** {@code PARSEABLE_TEMPORAL_NAMESPACE} — Temporal namespace. */
  private final String temporalNamespace;

  /** {@code PARSEABLE_SERVICE_NAME} — OTel service.name attribute. */
  private final String serviceName;

  /** {@code PARSEABLE_BATCH_EXPORT_TIMEOUT_MS} — max time to wait for a batch export. */
  private final Duration batchExportTimeout;

  private ParseableConfig(Builder b) {
    this.endpoint = b.endpoint;
    this.username = b.username;
    this.password = b.password;
    this.logStream = b.logStream;
    this.traceStream = b.traceStream;
    this.temporalHost = b.temporalHost;
    this.temporalNamespace = b.temporalNamespace;
    this.serviceName = b.serviceName;
    this.batchExportTimeout = b.batchExportTimeout;
  }

  // ── Factory ───────────────────────────────────────────────────────────────

  /**
   * Reads configuration from {@code PARSEABLE_*} environment variables.
   *
   * <p>{@code PARSEABLE_ENDPOINT}, {@code PARSEABLE_USERNAME}, and {@code PARSEABLE_PASSWORD}
   * are required — an {@link IllegalStateException} is thrown if any are absent or empty.
   */
  public static ParseableConfig fromEnv() {
    return builder()
        .endpoint(envRequired("PARSEABLE_ENDPOINT"))
        .username(envRequired("PARSEABLE_USERNAME"))
        .password(envRequired("PARSEABLE_PASSWORD"))
        .logStream(envOrDefault("PARSEABLE_LOG_STREAM", "temporal-logs"))
        .traceStream(envOrDefault("PARSEABLE_TRACE_STREAM", "temporal-traces"))
        .temporalHost(envOrDefault("PARSEABLE_TEMPORAL_HOST", "localhost:7233"))
        .temporalNamespace(envOrDefault("PARSEABLE_TEMPORAL_NAMESPACE", "default"))
        .serviceName(envOrDefault("PARSEABLE_SERVICE_NAME", "temporal-worker"))
        .batchExportTimeoutMs(
            Long.parseLong(envOrDefault("PARSEABLE_BATCH_EXPORT_TIMEOUT_MS", "5000")))
        .build();
  }

  public static Builder builder() {
    return new Builder();
  }

  // ── Accessors ─────────────────────────────────────────────────────────────

  public String getEndpoint() { return endpoint; }
  public String getUsername() { return username; }
  public String getPassword() { return password; }
  public String getLogStream() { return logStream; }
  public String getTraceStream() { return traceStream; }
  public String getTemporalHost() { return temporalHost; }
  public String getTemporalNamespace() { return temporalNamespace; }
  public String getServiceName() { return serviceName; }
  public Duration getBatchExportTimeout() { return batchExportTimeout; }

  /** Convenience: returns {@code <endpoint>/v1/logs}. */
  public String logsEndpoint() { return endpoint + "/v1/logs"; }

  /** Convenience: returns {@code <endpoint>/v1/traces}. */
  public String tracesEndpoint() { return endpoint + "/v1/traces"; }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private static String envRequired(String key) {
    String val = System.getenv(key);
    if (val == null || val.isEmpty()) {
      throw new IllegalStateException(
          "Required environment variable " + key + " is not set");
    }
    return val;
  }

  private static String envOrDefault(String key, String defaultValue) {
    String val = System.getenv(key);
    return (val != null && !val.isEmpty()) ? val : defaultValue;
  }

  @Override
  public String toString() {
    return "ParseableConfig{"
        + "endpoint='" + endpoint + '\''
        + ", logStream='" + logStream + '\''
        + ", traceStream='" + traceStream + '\''
        + ", temporalHost='" + temporalHost + '\''
        + ", temporalNamespace='" + temporalNamespace + '\''
        + ", serviceName='" + serviceName + '\''
        + '}';
  }

  // ── Builder ───────────────────────────────────────────────────────────────

  public static final class Builder {
    private String endpoint;
    private String username;
    private String password;
    private String logStream = "temporal-logs";
    private String traceStream = "temporal-traces";
    private String temporalHost = "localhost:7233";
    private String temporalNamespace = "default";
    private String serviceName = "temporal-worker";
    private Duration batchExportTimeout = Duration.ofMillis(5000);

    public Builder endpoint(String val) { this.endpoint = val; return this; }
    public Builder username(String val) { this.username = val; return this; }
    public Builder password(String val) { this.password = val; return this; }
    public Builder logStream(String val) { this.logStream = val; return this; }
    public Builder traceStream(String val) { this.traceStream = val; return this; }
    public Builder temporalHost(String val) { this.temporalHost = val; return this; }
    public Builder temporalNamespace(String val) { this.temporalNamespace = val; return this; }
    public Builder serviceName(String val) { this.serviceName = val; return this; }
    public Builder batchExportTimeout(Duration val) { this.batchExportTimeout = val; return this; }
    public Builder batchExportTimeoutMs(long ms) {
      this.batchExportTimeout = Duration.ofMillis(ms);
      return this;
    }

    public ParseableConfig build() {
      if (endpoint == null || endpoint.isEmpty()) {
        throw new IllegalStateException("endpoint is required");
      }
      if (username == null || username.isEmpty()) {
        throw new IllegalStateException("username is required");
      }
      if (password == null || password.isEmpty()) {
        throw new IllegalStateException("password is required");
      }
      return new ParseableConfig(this);
    }
  }
}
