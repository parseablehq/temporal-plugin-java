package com.parseable.temporal.interceptors;

import com.parseable.temporal.ParseableEmitter;

import io.nexusrpc.handler.OperationContext;
import io.temporal.common.interceptors.ActivityInboundCallsInterceptor;
import io.temporal.common.interceptors.NexusOperationInboundCallsInterceptor;
import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor;

/**
 * Top-level {@link WorkerInterceptor} that wires the Parseable workflow and activity interceptors
 * into a Temporal worker. Registered automatically by {@link com.parseable.temporal.ParseablePlugin}.
 */
public final class ParseableWorkerInterceptor implements WorkerInterceptor {

  private final ParseableEmitter emitter;

  public ParseableWorkerInterceptor(ParseableEmitter emitter) {
    this.emitter = emitter;
  }

  @Override
  public WorkflowInboundCallsInterceptor interceptWorkflow(WorkflowInboundCallsInterceptor next) {
    return new ParseableWorkflowInboundInterceptor(next, emitter);
  }

  @Override
  public ActivityInboundCallsInterceptor interceptActivity(ActivityInboundCallsInterceptor next) {
    return new ParseableActivityInboundInterceptor(next, emitter);
  }

  @Override
  public NexusOperationInboundCallsInterceptor interceptNexusOperation(
      OperationContext context, NexusOperationInboundCallsInterceptor next) {
    return next;
  }
}
