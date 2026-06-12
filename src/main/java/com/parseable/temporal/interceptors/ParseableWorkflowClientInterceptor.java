package com.parseable.temporal.interceptors;

import com.parseable.temporal.ParseableEmitter;

import io.temporal.common.interceptors.WorkflowClientCallsInterceptor;
import io.temporal.common.interceptors.WorkflowClientInterceptorBase;

/** Intercepts workflow client operations and emits Parseable telemetry. */
public final class ParseableWorkflowClientInterceptor extends WorkflowClientInterceptorBase {

  private final ParseableEmitter emitter;

  public ParseableWorkflowClientInterceptor(ParseableEmitter emitter) {
    this.emitter = emitter;
  }

  @Override
  public WorkflowClientCallsInterceptor workflowClientCallsInterceptor(
      WorkflowClientCallsInterceptor next) {
    return new ParseableWorkflowClientCallsInterceptor(next, emitter);
  }
}
