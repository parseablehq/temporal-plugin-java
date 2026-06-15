package com.parseable.temporal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParseableConfigTest {

  @Test
  void builderRequiresEndpoint() {
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> ParseableConfig.builder().username("u").password("p").build());
    assertTrue(ex.getMessage().contains("endpoint"));
  }

  @Test
  void builderRequiresUsername() {
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> ParseableConfig.builder().endpoint("http://localhost:8000").password("p").build());
    assertTrue(ex.getMessage().contains("username"));
  }

  @Test
  void builderRequiresPassword() {
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> ParseableConfig.builder().endpoint("http://localhost:8000").username("u").build());
    assertTrue(ex.getMessage().contains("password"));
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
        .username("u")
        .password("p")
        .build();
    assertEquals("https://demo.parseable.com:8000/v1/logs", config.logsEndpoint());
    assertEquals("https://demo.parseable.com:8000/v1/traces", config.tracesEndpoint());
  }
}
