package com.parseable.temporal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParseableConfigTest {

  @Test
  void builderDefaults() {
    ParseableConfig config = ParseableConfig.builder().build();
    assertEquals("https://demo.parseable.com:8000", config.getEndpoint());
    assertEquals("admin", config.getUsername());
    assertEquals("password", config.getPassword());
    assertEquals("temporal-logs", config.getLogStream());
    assertEquals("temporal-traces", config.getTraceStream());
    assertEquals("localhost:7233", config.getTemporalHost());
    assertEquals("default", config.getTemporalNamespace());
    assertEquals("temporal-worker", config.getServiceName());
    assertEquals(5000, config.getBatchExportTimeout().toMillis());
  }

  @Test
  void builderOverrides() {
    ParseableConfig config = ParseableConfig.builder()
        .endpoint("https://my-parseable:8000")
        .username("user")
        .password("secret")
        .logStream("my-logs")
        .traceStream("my-traces")
        .temporalHost("temporal.internal:7233")
        .temporalNamespace("prod")
        .serviceName("order-worker")
        .batchExportTimeoutMs(10_000)
        .build();

    assertEquals("https://my-parseable:8000", config.getEndpoint());
    assertEquals("user", config.getUsername());
    assertEquals("secret", config.getPassword());
    assertEquals("my-logs", config.getLogStream());
    assertEquals("my-traces", config.getTraceStream());
    assertEquals("temporal.internal:7233", config.getTemporalHost());
    assertEquals("prod", config.getTemporalNamespace());
    assertEquals("order-worker", config.getServiceName());
    assertEquals(10_000, config.getBatchExportTimeout().toMillis());
  }

  @Test
  void endpointDerivations() {
    ParseableConfig config = ParseableConfig.builder()
        .endpoint("https://demo.parseable.com:8000")
        .build();
    assertEquals("https://demo.parseable.com:8000/v1/logs", config.logsEndpoint());
    assertEquals("https://demo.parseable.com:8000/v1/traces", config.tracesEndpoint());
  }

  @Test
  void fromEnvUsesDefaults() {
    // When no env vars are set the defaults must match the TypeScript/Python SDKs
    ParseableConfig config = ParseableConfig.fromEnv();
    assertNotNull(config.getEndpoint());
    assertNotNull(config.getLogStream());
    assertNotNull(config.getTraceStream());
  }
}
