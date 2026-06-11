package com.parseable.temporal;

import io.temporal.client.WorkflowClientOptions;
import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.WorkerFactoryOptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParseablePluginTest {

  @Test
  void configuresServiceStubsAndClientOptions() {
    ParseablePlugin plugin = newPlugin();
    try {
      WorkflowServiceStubsOptions.Builder stubsBuilder = WorkflowServiceStubsOptions.newBuilder();
      plugin.configureServiceStubs(stubsBuilder);
      assertEquals("temporal.internal:7233", stubsBuilder.build().getTarget());

      WorkflowClientOptions.Builder clientBuilder = WorkflowClientOptions.newBuilder();
      plugin.configureWorkflowClient(clientBuilder);
      assertEquals("prod", clientBuilder.build().getNamespace());
    } finally {
      plugin.close();
    }
  }

  @Test
  void configureWorkerFactoryDoesNotDuplicateInterceptor() {
    ParseablePlugin plugin = newPlugin();
    try {
      WorkerFactoryOptions.Builder builder = WorkerFactoryOptions.newBuilder();

      plugin.configureWorkerFactory(builder);
      plugin.configureWorkerFactory(builder);

      WorkerInterceptor[] interceptors = builder.build().getWorkerInterceptors();
      assertNotNull(interceptors);
      assertEquals(1, interceptors.length);
      assertSame(plugin.getWorkerInterceptor(), interceptors[0]);
    } finally {
      plugin.close();
    }
  }

  private static ParseablePlugin newPlugin() {
    return new ParseablePlugin(ParseableConfig.builder()
        .endpoint("http://localhost:9999")
        .username("test")
        .password("test")
        .temporalHost("temporal.internal:7233")
        .temporalNamespace("prod")
        .build());
  }
}
