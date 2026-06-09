package com.parseable.temporal.interceptors;

import com.parseable.temporal.ParseableEmitter;

import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptorBase;
import io.temporal.workflow.Workflow;

/**
 * Intercepts workflow outbound calls. Currently a thin pass-through; extend here to capture
 * child workflow launches, signals sent, and timer scheduling.
 *
 * <p>All methods that should emit events <strong>must</strong> guard with
 * {@link Workflow#isReplaying()} to prevent duplicate events during replay.
 */
public final class ParseableWorkflowOutboundInterceptor
    extends WorkflowOutboundCallsInterceptorBase {

  private final ParseableEmitter emitter;

  public ParseableWorkflowOutboundInterceptor(
      WorkflowOutboundCallsInterceptor next, ParseableEmitter emitter) {
    super(next);
    this.emitter = emitter;
  }

  // Override individual outbound methods here as needed.
  // Always guard with:  if (!Workflow.isReplaying()) { emitter.emit(...); }
}
