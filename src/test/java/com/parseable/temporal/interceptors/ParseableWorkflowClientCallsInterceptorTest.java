package com.parseable.temporal.interceptors;

import com.parseable.temporal.ParseableConfig;
import com.parseable.temporal.ParseableEmitter;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowUpdateHandle;
import io.temporal.common.interceptors.Header;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.CancelInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.CancelOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.CountWorkflowOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.CountWorkflowsInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.DescribeWorkflowInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.DescribeWorkflowOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.GetResultAsyncOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.GetResultInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.GetResultOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.ListWorkflowExecutionsInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.ListWorkflowExecutionsOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.PollWorkflowUpdateInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.PollWorkflowUpdateOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.QueryInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.QueryOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.StartUpdateInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.TerminateInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.TerminateOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.WorkflowSignalInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.WorkflowSignalOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.WorkflowSignalWithStartInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.WorkflowSignalWithStartOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.WorkflowStartInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.WorkflowStartOutput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.WorkflowUpdateWithStartInput;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor.WorkflowUpdateWithStartOutput;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ParseableWorkflowClientCallsInterceptorTest {

  @Test
  void startEmitsClientOperationWithOutputRunId() {
    SpyEmitter emitter = new SpyEmitter();
    WorkflowExecution execution = WorkflowExecution.newBuilder()
        .setWorkflowId("wf-1")
        .setRunId("run-1")
        .build();
    StubCallsInterceptor next = new StubCallsInterceptor();
    next.startOutput = new WorkflowStartOutput(execution);

    ParseableWorkflowClientCallsInterceptor interceptor =
        new ParseableWorkflowClientCallsInterceptor(next, emitter);
    interceptor.start(new WorkflowStartInput(
        "wf-1",
        "HelloWorkflow",
        Header.empty(),
        new Object[] {"redacted"},
        WorkflowOptions.newBuilder().setTaskQueue("queue").build()));

    assertTrue(emitter.operations.stream()
        .anyMatch(event -> event.contains("workflow.start:wf-1:run-1:HelloWorkflow:queue:completed")));
  }

  @Test
  void queryFailureEmitsFailedClientOperation() {
    SpyEmitter emitter = new SpyEmitter();
    StubCallsInterceptor next = new StubCallsInterceptor();
    next.queryFailure = new RuntimeException("boom");

    ParseableWorkflowClientCallsInterceptor interceptor =
        new ParseableWorkflowClientCallsInterceptor(next, emitter);
    QueryInput<String> input = new QueryInput<>(
        WorkflowExecution.newBuilder().setWorkflowId("wf-1").setRunId("run-1").build(),
        "currentState",
        Header.empty(),
        new Object[] {"redacted"},
        String.class,
        String.class);

    try {
      interceptor.query(input);
    } catch (RuntimeException ignored) {
      // Expected.
    }

    assertTrue(emitter.operations.stream()
        .anyMatch(event -> event.contains("workflow.query:wf-1:run-1:null:null:failed")));
    assertTrue(emitter.operations.stream().anyMatch(event -> event.contains(":currentState:")));
  }

  static class StubCallsInterceptor implements WorkflowClientCallsInterceptor {
    WorkflowStartOutput startOutput;
    RuntimeException queryFailure;

    @Override
    public WorkflowStartOutput start(WorkflowStartInput input) {
      return startOutput;
    }

    @Override
    public WorkflowSignalOutput signal(WorkflowSignalInput input) {
      return new WorkflowSignalOutput();
    }

    @Override
    public WorkflowSignalWithStartOutput signalWithStart(WorkflowSignalWithStartInput input) {
      return new WorkflowSignalWithStartOutput(startOutput);
    }

    @Override
    public <R> WorkflowUpdateWithStartOutput<R> updateWithStart(
        WorkflowUpdateWithStartInput<R> input) {
      return new WorkflowUpdateWithStartOutput<>(startOutput, null);
    }

    @Override
    public <R> GetResultOutput<R> getResult(GetResultInput<R> input) throws TimeoutException {
      return new GetResultOutput<>(null);
    }

    @Override
    public <R> GetResultAsyncOutput<R> getResultAsync(GetResultInput<R> input) {
      return null;
    }

    @Override
    public <R> QueryOutput<R> query(QueryInput<R> input) {
      if (queryFailure != null) {
        throw queryFailure;
      }
      return new QueryOutput<>(null, null);
    }

    @Override
    public <R> WorkflowUpdateHandle<R> startUpdate(StartUpdateInput<R> input) {
      return null;
    }

    @Override
    public <R> PollWorkflowUpdateOutput<R> pollWorkflowUpdate(PollWorkflowUpdateInput<R> input) {
      return null;
    }

    @Override
    public CancelOutput cancel(CancelInput input) {
      return new CancelOutput();
    }

    @Override
    public TerminateOutput terminate(TerminateInput input) {
      return new TerminateOutput();
    }

    @Override
    public DescribeWorkflowOutput describe(DescribeWorkflowInput input) {
      return null;
    }

    @Override
    public ListWorkflowExecutionsOutput listWorkflowExecutions(ListWorkflowExecutionsInput input) {
      return null;
    }

    @Override
    public CountWorkflowOutput countWorkflows(CountWorkflowsInput input) {
      return null;
    }
  }

  static class SpyEmitter extends ParseableEmitter {
    final List<String> operations = new ArrayList<>();

    SpyEmitter() {
      super(testConfig());
    }

    @Override
    public void emitClientOperation(
        String operation,
        String workflowId,
        String runId,
        String workflowType,
        String taskQueue,
        String signalName,
        String queryType,
        String updateName,
        String updateId,
        String status,
        String errorMessage,
        long startEpochNanos,
        long endEpochNanos) {
      operations.add(operation + ":" + workflowId + ":" + runId + ":" + workflowType + ":"
          + taskQueue + ":" + status + ":" + signalName + ":" + queryType + ":" + updateName
          + ":" + updateId + ":" + errorMessage);
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
