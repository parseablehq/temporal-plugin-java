package com.parseable.temporal;

import com.parseable.temporal.interceptors.ParseableWorkerInterceptor;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerOptions;
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

/**
 * Integration tests for the Parseable interceptors using Temporal's in-process test environment.
 *
 * <p>Tagged {@code integration} — excluded from CI by default (requires JVM network access for
 * the test server binary). Run locally with:
 * <pre>
 *   mvn test -Pintegration
 * </pre>
 */
@Tag("integration")
class ParseableInterceptorTest {

  private TestWorkflowEnvironment testEnv;
  private WorkflowClient client;
  private SpyEmitter spy;

  @BeforeEach
  void setUp() {
    testEnv = TestWorkflowEnvironment.newInstance();
    client = testEnv.getWorkflowClient();
    spy = new SpyEmitter();

    Worker worker = testEnv.newWorker(
        "test-queue",
        WorkerOptions.newBuilder()
            .setInterceptors(new ParseableWorkerInterceptor(spy))
            .build());
    worker.registerWorkflowImplementationTypes(HelloWorkflowImpl.class);
    worker.registerActivitiesImplementations(new HelloActivitiesImpl());
    testEnv.start();
  }

  @AfterEach
  void tearDown() {
    testEnv.close();
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
  void replaySafetyNoDuplicateEvents() throws Exception {
    // Run the workflow and capture how many events were fired
    HelloWorkflow wf = client.newWorkflowStub(
        HelloWorkflow.class,
        WorkflowOptions.newBuilder()
            .setTaskQueue("test-queue")
            .setWorkflowExecutionTimeout(Duration.ofSeconds(10))
            .build());

    wf.greet("Replay");

    long workflowStarted = spy.events.stream()
        .filter(e -> e.contains("workflow") && e.contains("started"))
        .count();

    // Despite any internal replay, the user should see exactly one "started" event
    assertEquals(1, workflowStarted, "Replay guard should prevent duplicate 'started' events");
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
      super(ParseableConfig.builder()
          .endpoint("http://localhost:9999")   // unreachable — spy never sends
          .build());
    }

    @Override
    public void emitWorkflowEvent(
        String workflowId, String workflowType, String taskQueue,
        String status, String errorMessage) {
      events.add("workflow:" + workflowType + ":" + status);
    }

    @Override
    public void emitActivityEvent(
        String workflowId, String activityType, String taskQueue,
        String status, String errorMessage) {
      events.add("activity:" + activityType + ":" + status);
    }
  }
}
