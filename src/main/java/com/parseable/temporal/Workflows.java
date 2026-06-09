package com.parseable.temporal.example;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;

/** Example workflows and activities for the Parseable Java plugin demo. */
public final class Workflows {

  private Workflows() { }

  // ── Greeting workflow ─────────────────────────────────────────────────────

  @WorkflowInterface
  public interface GreetingWorkflow {
    @WorkflowMethod
    String greet(String name);
  }

  @ActivityInterface
  public interface GreetingActivities {
    @ActivityMethod
    String composeGreeting(String name);
  }

  public static class GreetingWorkflowImpl implements GreetingWorkflow {
    private final GreetingActivities activities = Workflow.newActivityStub(
        GreetingActivities.class,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(10))
            .build());

    @Override
    public String greet(String name) {
      return activities.composeGreeting(name);
    }
  }

  public static class GreetingActivitiesImpl implements GreetingActivities {
    @Override
    public String composeGreeting(String name) {
      return "Hello, " + name + "! (from Parseable Java plugin)";
    }
  }

  // ── Failing workflow (demonstrates error events) ──────────────────────────

  @WorkflowInterface
  public interface FailingWorkflow {
    @WorkflowMethod
    void run();
  }

  public static class FailingWorkflowImpl implements FailingWorkflow {
    @Override
    public void run() {
      throw ApplicationFailure.newNonRetryableFailure(
          "Intentional failure for demo purposes", "DemoFailure");
    }
  }
}
