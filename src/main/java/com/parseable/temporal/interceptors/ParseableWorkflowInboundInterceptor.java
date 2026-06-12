package com.parseable.temporal.interceptors;

import com.parseable.temporal.ParseableEmitter;

import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor;
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptorBase;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInfo;

/**
 * Intercepts inbound workflow executions.
 *
 * <p>Emits {@code started}, {@code completed}, {@code failed}, and {@code canceled} events to
 * Parseable.
 * All emissions are guarded with {@link Workflow#isReplaying()} to avoid duplicate records
 * when Temporal replays workflow history.
 */
public final class ParseableWorkflowInboundInterceptor
    extends WorkflowInboundCallsInterceptorBase {

  private final ParseableEmitter emitter;

  public ParseableWorkflowInboundInterceptor(
      WorkflowInboundCallsInterceptor next, ParseableEmitter emitter) {
    super(next);
    this.emitter = emitter;
  }

  @Override
  public void init(WorkflowOutboundCallsInterceptor outboundCalls) {
    // Chain our outbound interceptor in
    super.init(new ParseableWorkflowOutboundInterceptor(outboundCalls, emitter));
  }

  @Override
  public WorkflowOutput execute(WorkflowInput input) {
    WorkflowInfo info = Workflow.getInfo();
    String workflowId = info.getWorkflowId();
    String runId = info.getRunId();
    String workflowType = info.getWorkflowType();
    String taskQueue = info.getTaskQueue();

    // Emit "started" only on the first execution, not on replay
    if (!Workflow.isReplaying()) {
      emitter.emitWorkflowEvent(
          workflowId, runId, workflowType, taskQueue, LifecycleStatus.STARTED, null);
    }

    try {
      WorkflowOutput output = super.execute(input);
      if (!Workflow.isReplaying()) {
        emitter.emitWorkflowEvent(
            workflowId, runId, workflowType, taskQueue, LifecycleStatus.COMPLETED, null);
      }
      return output;
    } catch (Exception e) {
      if (!Workflow.isReplaying()) {
        emitter.emitWorkflowEvent(
            workflowId, runId, workflowType, taskQueue, LifecycleStatus.fromThrowable(e),
            e.getMessage());
      }
      throw e;
    }
  }
}
