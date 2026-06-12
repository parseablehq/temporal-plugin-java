package com.parseable.temporal.interceptors;

import com.parseable.temporal.ParseableEmitter;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowUpdateHandle;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptor;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptorBase;

import java.util.concurrent.TimeoutException;

/**
 * Emits telemetry for client-side workflow operations such as starts, signals, queries, updates,
 * cancels, and terminations.
 */
final class ParseableWorkflowClientCallsInterceptor extends WorkflowClientCallsInterceptorBase {

  private final ParseableEmitter emitter;

  ParseableWorkflowClientCallsInterceptor(
      WorkflowClientCallsInterceptor next, ParseableEmitter emitter) {
    super(next);
    this.emitter = emitter;
  }

  @Override
  public WorkflowStartOutput start(WorkflowStartInput input) {
    OperationMetadata metadata = OperationMetadata.forStart("workflow.start", input);
    long start = nowNanos();
    try {
      WorkflowStartOutput output = super.start(input);
      metadata = metadata.withExecution(output.getWorkflowExecution());
      emit(metadata, LifecycleStatus.COMPLETED, null, start, nowNanos());
      return output;
    } catch (RuntimeException e) {
      emit(metadata, LifecycleStatus.fromThrowable(e), e.getMessage(), start, nowNanos());
      throw e;
    }
  }

  @Override
  public WorkflowSignalOutput signal(WorkflowSignalInput input) {
    OperationMetadata metadata = OperationMetadata.forSignal(input);
    long start = nowNanos();
    try {
      WorkflowSignalOutput output = super.signal(input);
      emit(metadata, LifecycleStatus.COMPLETED, null, start, nowNanos());
      return output;
    } catch (RuntimeException e) {
      emit(metadata, LifecycleStatus.fromThrowable(e), e.getMessage(), start, nowNanos());
      throw e;
    }
  }

  @Override
  public WorkflowSignalWithStartOutput signalWithStart(WorkflowSignalWithStartInput input) {
    OperationMetadata metadata = OperationMetadata.forSignalWithStart(input);
    long start = nowNanos();
    try {
      WorkflowSignalWithStartOutput output = super.signalWithStart(input);
      metadata = metadata.withExecution(output.getWorkflowStartOutput().getWorkflowExecution());
      emit(metadata, LifecycleStatus.COMPLETED, null, start, nowNanos());
      return output;
    } catch (RuntimeException e) {
      emit(metadata, LifecycleStatus.fromThrowable(e), e.getMessage(), start, nowNanos());
      throw e;
    }
  }

  @Override
  public <R> WorkflowUpdateWithStartOutput<R> updateWithStart(
      WorkflowUpdateWithStartInput<R> input) {
    OperationMetadata metadata = OperationMetadata.forUpdateWithStart(input);
    long start = nowNanos();
    try {
      WorkflowUpdateWithStartOutput<R> output = super.updateWithStart(input);
      metadata = metadata.withExecution(output.getWorkflowStartOutput().getWorkflowExecution());
      emit(metadata, LifecycleStatus.COMPLETED, null, start, nowNanos());
      return output;
    } catch (RuntimeException e) {
      emit(metadata, LifecycleStatus.fromThrowable(e), e.getMessage(), start, nowNanos());
      throw e;
    }
  }

  @Override
  public <R> QueryOutput<R> query(QueryInput<R> input) {
    OperationMetadata metadata = OperationMetadata.forQuery(input);
    long start = nowNanos();
    try {
      QueryOutput<R> output = super.query(input);
      emit(metadata, LifecycleStatus.COMPLETED, null, start, nowNanos());
      return output;
    } catch (RuntimeException e) {
      emit(metadata, LifecycleStatus.fromThrowable(e), e.getMessage(), start, nowNanos());
      throw e;
    }
  }

  @Override
  public <R> GetResultOutput<R> getResult(GetResultInput<R> input) throws TimeoutException {
    OperationMetadata metadata = OperationMetadata.forGetResult(input);
    long start = nowNanos();
    try {
      GetResultOutput<R> output = super.getResult(input);
      emit(metadata, LifecycleStatus.COMPLETED, null, start, nowNanos());
      return output;
    } catch (TimeoutException e) {
      emit(metadata, LifecycleStatus.FAILED, e.getMessage(), start, nowNanos());
      throw e;
    } catch (RuntimeException e) {
      emit(metadata, LifecycleStatus.fromThrowable(e), e.getMessage(), start, nowNanos());
      throw e;
    }
  }

  @Override
  public <R> WorkflowUpdateHandle<R> startUpdate(StartUpdateInput<R> input) {
    OperationMetadata metadata = OperationMetadata.forStartUpdate(input);
    long start = nowNanos();
    try {
      WorkflowUpdateHandle<R> output = super.startUpdate(input);
      emit(metadata, LifecycleStatus.COMPLETED, null, start, nowNanos());
      return output;
    } catch (RuntimeException e) {
      emit(metadata, LifecycleStatus.fromThrowable(e), e.getMessage(), start, nowNanos());
      throw e;
    }
  }

  @Override
  public CancelOutput cancel(CancelInput input) {
    OperationMetadata metadata = OperationMetadata.forExecution("workflow.cancel", input.getWorkflowExecution());
    long start = nowNanos();
    try {
      CancelOutput output = super.cancel(input);
      emit(metadata, LifecycleStatus.CANCELED, null, start, nowNanos());
      return output;
    } catch (RuntimeException e) {
      emit(metadata, LifecycleStatus.fromThrowable(e), e.getMessage(), start, nowNanos());
      throw e;
    }
  }

  @Override
  public TerminateOutput terminate(TerminateInput input) {
    OperationMetadata metadata =
        OperationMetadata.forExecution("workflow.terminate", input.getWorkflowExecution());
    long start = nowNanos();
    try {
      TerminateOutput output = super.terminate(input);
      emit(metadata, LifecycleStatus.COMPLETED, null, start, nowNanos());
      return output;
    } catch (RuntimeException e) {
      emit(metadata, LifecycleStatus.fromThrowable(e), e.getMessage(), start, nowNanos());
      throw e;
    }
  }

  @Override
  public DescribeWorkflowOutput describe(DescribeWorkflowInput input) {
    OperationMetadata metadata =
        OperationMetadata.forExecution("workflow.describe", input.getWorkflowExecution());
    long start = nowNanos();
    try {
      DescribeWorkflowOutput output = super.describe(input);
      emit(metadata, LifecycleStatus.COMPLETED, null, start, nowNanos());
      return output;
    } catch (RuntimeException e) {
      emit(metadata, LifecycleStatus.fromThrowable(e), e.getMessage(), start, nowNanos());
      throw e;
    }
  }

  private void emit(
      OperationMetadata metadata,
      String status,
      String errorMessage,
      long startEpochNanos,
      long endEpochNanos) {
    emitter.emitClientOperation(
        metadata.operation,
        metadata.workflowId,
        metadata.runId,
        metadata.workflowType,
        metadata.taskQueue,
        metadata.signalName,
        metadata.queryType,
        metadata.updateName,
        metadata.updateId,
        status,
        errorMessage,
        startEpochNanos,
        endEpochNanos);
  }

  private static long nowNanos() {
    return System.currentTimeMillis() * 1_000_000L;
  }

  private static final class OperationMetadata {
    private final String operation;
    private final String workflowId;
    private final String runId;
    private final String workflowType;
    private final String taskQueue;
    private final String signalName;
    private final String queryType;
    private final String updateName;
    private final String updateId;

    private OperationMetadata(
        String operation,
        String workflowId,
        String runId,
        String workflowType,
        String taskQueue,
        String signalName,
        String queryType,
        String updateName,
        String updateId) {
      this.operation = operation;
      this.workflowId = workflowId;
      this.runId = runId;
      this.workflowType = workflowType;
      this.taskQueue = taskQueue;
      this.signalName = signalName;
      this.queryType = queryType;
      this.updateName = updateName;
      this.updateId = updateId;
    }

    private static OperationMetadata forStart(String operation, WorkflowStartInput input) {
      return new OperationMetadata(
          operation,
          input.getWorkflowId(),
          null,
          input.getWorkflowType(),
          input.getOptions() != null ? input.getOptions().getTaskQueue() : null,
          null,
          null,
          null,
          null);
    }

    private static OperationMetadata forSignal(WorkflowSignalInput input) {
      return forExecution("workflow.signal", input.getWorkflowExecution())
          .withSignalName(input.getSignalName());
    }

    private static OperationMetadata forSignalWithStart(WorkflowSignalWithStartInput input) {
      return forStart("workflow.signal_with_start", input.getWorkflowStartInput())
          .withSignalName(input.getSignalName());
    }

    private static <R> OperationMetadata forUpdateWithStart(WorkflowUpdateWithStartInput<R> input) {
      return forStart("workflow.update_with_start", input.getWorkflowStartInput())
          .withUpdate(input.getStartUpdateInput().getUpdateName(),
              input.getStartUpdateInput().getUpdateId());
    }

    private static <R> OperationMetadata forQuery(QueryInput<R> input) {
      return forExecution("workflow.query", input.getWorkflowExecution())
          .withQueryType(input.getQueryType());
    }

    private static <R> OperationMetadata forGetResult(GetResultInput<R> input) {
      return forExecution("workflow.result.get", input.getWorkflowExecution())
          .withWorkflowType(input.getWorkflowType().orElse(null));
    }

    private static <R> OperationMetadata forStartUpdate(StartUpdateInput<R> input) {
      return forExecution("workflow.update.start", input.getWorkflowExecution())
          .withWorkflowType(input.getWorkflowType().orElse(null))
          .withUpdate(input.getUpdateName(), input.getUpdateId());
    }

    private static OperationMetadata forExecution(String operation, WorkflowExecution execution) {
      return new OperationMetadata(
          operation,
          execution.getWorkflowId(),
          execution.getRunId(),
          null,
          null,
          null,
          null,
          null,
          null);
    }

    private OperationMetadata withExecution(WorkflowExecution execution) {
      return new OperationMetadata(
          operation,
          execution.getWorkflowId(),
          execution.getRunId(),
          workflowType,
          taskQueue,
          signalName,
          queryType,
          updateName,
          updateId);
    }

    private OperationMetadata withWorkflowType(String value) {
      return new OperationMetadata(
          operation, workflowId, runId, value, taskQueue, signalName, queryType, updateName,
          updateId);
    }

    private OperationMetadata withSignalName(String value) {
      return new OperationMetadata(
          operation, workflowId, runId, workflowType, taskQueue, value, queryType, updateName,
          updateId);
    }

    private OperationMetadata withQueryType(String value) {
      return new OperationMetadata(
          operation, workflowId, runId, workflowType, taskQueue, signalName, value, updateName,
          updateId);
    }

    private OperationMetadata withUpdate(String name, String id) {
      return new OperationMetadata(
          operation, workflowId, runId, workflowType, taskQueue, signalName, queryType, name, id);
    }
  }
}
