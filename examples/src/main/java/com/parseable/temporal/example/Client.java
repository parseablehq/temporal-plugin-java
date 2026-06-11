package com.parseable.temporal.example;

import com.parseable.temporal.ParseableConfig;
import com.parseable.temporal.ParseablePlugin;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;

import java.util.UUID;

/**
 * Demo client that triggers all example workflow variants.
 *
 * <p>Start the {@link Worker} first, then run this class.
 */
public final class Client {

  private static final String TASK_QUEUE = "parseable-demo-queue";

  public static void main(String[] args) {
    ParseablePlugin plugin = new ParseablePlugin(ParseableConfig.fromEnv());

    try {
      WorkflowServiceStubs stubs = WorkflowServiceStubs.newServiceStubs(
          WorkflowServiceStubsOptions.newBuilder().setPlugins(plugin).build());

      WorkflowClient client = WorkflowClient.newInstance(stubs);

      // --- Greeting workflow (should succeed) ---
      System.out.println("Starting GreetingWorkflow...");
      Workflows.GreetingWorkflow greeting = client.newWorkflowStub(
          Workflows.GreetingWorkflow.class,
          WorkflowOptions.newBuilder()
              .setWorkflowId("greeting-" + UUID.randomUUID())
              .setTaskQueue(TASK_QUEUE)
              .build());
      String result = greeting.greet("Parseable");
      System.out.println("Result: " + result);

      // --- Failing workflow (should emit error event) ---
      System.out.println("\nStarting FailingWorkflow (expected to fail)...");
      try {
        Workflows.FailingWorkflow failing = client.newWorkflowStub(
            Workflows.FailingWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId("failing-" + UUID.randomUUID())
                .setTaskQueue(TASK_QUEUE)
                .build());
        failing.run();
      } catch (Exception e) {
        System.out.println("Workflow failed as expected: " + e.getMessage());
      }

      ParseableConfig config = plugin.getConfig();
      System.out.println("\nAll workflows triggered. Check Parseable streams:");
      System.out.println("  Logs:   " + config.logsEndpoint());
      System.out.println("  Traces: " + config.tracesEndpoint());

    } finally {
      plugin.close();
    }
  }
}
