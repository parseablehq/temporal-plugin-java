package com.parseable.temporal;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.WorkflowReplayer;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for Parseable interceptors using Temporal's in-process test environment. */
@Tag("integration")
class ParseableInterceptorTest {

  private TestWorkflowEnvironment testEnv;
  private WorkflowClient client;
  private ParseablePlugin plugin;
  private SpyEmitter spy;
  private Worker worker;

  @BeforeEach
  void setUp() {
    spy = new SpyEmitter();
    plugin = new ParseablePlugin(testConfig(), spy);
    testEnv = TestWorkflowEnvironment.newInstance(
        TestEnvironmentOptions.newBuilder()
            .setWorkerFactoryOptions(WorkerFactoryOptions.newBuilder()
                .setPlugins(plugin)
                .build())
            .build());
    client = testEnv.getWorkflowClient();

    worker = testEnv.newWorker("test-queue");
    worker.registerWorkflowImplementationTypes(HelloWorkflowImpl.class);
    worker.registerActivitiesImplementations(new HelloActivitiesImpl());
    testEnv.start();
  }

  @AfterEach
  void tearDown() {
    testEnv.close();
    plugin.close();
  }

  @Test
  void workflowAndActivityEventsEmitted() {
    HelloWorkflow wf = client.newWorkflowStub(
        HelloWorkflow.class,
        WorkflowOptions.newBuilder()
            .setTaskQueue("test-queue")
            .setWorkflowExecutionTimeout(Duration.ofSeconds(10))
            .build());

    String result = wf.greet("World");
    assertEquals("Hello, World!", result);

    // Workflow: started + completed
    assertTrue(spy.events.stream().anyMatch(e -> e.contains("workflow") && e.contains("started")));
    assertTrue(spy.events.stream().anyMatch(e -> e.contains("workflow") && e.contains("completed")));

    // Activity: started + completed
    assertTrue(spy.events.stream().anyMatch(e -> e.contains("activity") && e.contains("started")));
    assertTrue(spy.events.stream().anyMatch(e -> e.contains("activity") && e.contains("completed")));
  }

  @Test
  void replayDoesNotEmitDuplicateEvents() throws Exception {
    // Run the workflow and capture its execution reference for history extraction.
    WorkflowOptions opts = WorkflowOptions.newBuilder()
        .setTaskQueue("test-queue")
        .setWorkflowId("replay-test")
        .setWorkflowExecutionTimeout(Duration.ofSeconds(10))
        .build();
    HelloWorkflow wf = client.newWorkflowStub(HelloWorkflow.class, opts);
    wf.greet("Replay");

    int eventsAfterFirstRun = spy.events.size();
    assertTrue(eventsAfterFirstRun > 0);

    // Fetch completed history and replay it through WorkflowReplayer.
    // During replay Workflow.isReplaying() is true throughout, so the interceptor
    // must not call the emitter at all.
    spy.events.clear();

    WorkflowReplayer.replayWorkflowExecution(
        client.fetchHistory("replay-test"), worker);

    assertEquals(0, spy.events.size(),
        "No events should be emitted during replay; got: " + spy.events);
  }

  // ── Test workflow / activity stubs ────────────────────────────────────────

  @WorkflowInterface
  public interface HelloWorkflow {
    @WorkflowMethod
    String greet(String name);
  }

  @ActivityInterface
  public interface HelloActivities {
    @ActivityMethod
    String buildGreeting(String name);
  }

  public static class HelloWorkflowImpl implements HelloWorkflow {
    private final HelloActivities activities = Workflow.newActivityStub(
        HelloActivities.class,
        ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(5)).build());

    @Override
    public String greet(String name) {
      return activities.buildGreeting(name);
    }
  }

  public static class HelloActivitiesImpl implements HelloActivities {
    @Override
    public String buildGreeting(String name) {
      return "Hello, " + name + "!";
    }
  }

  // ── Spy emitter ───────────────────────────────────────────────────────────

  /**
   * A test double for {@link ParseableEmitter} that records events without doing any
   * OTel/HTTP work.
   */
  static class SpyEmitter extends ParseableEmitter {
    final List<String> events = new ArrayList<>();

    SpyEmitter() {
      super(testConfig());
    }

    @Override
    public void emitWorkflowEvent(
        String workflowId, String runId, String workflowType, String taskQueue,
        String status, String errorMessage) {
      events.add("workflow:" + workflowType + ":" + status);
    }

    @Override
    public void emitActivityEvent(
        String workflowId, String runId, String activityId, String activityType,
        String taskQueue, String status, String errorMessage) {
      events.add("activity:" + activityId + ":" + activityType + ":" + status);
    }
  }

  private static ParseableConfig testConfig() {
    return ParseableConfig.builder()
        .endpoint("http://localhost:9999")
        .username("test")
        .password("test")
        .build();
  }
}
