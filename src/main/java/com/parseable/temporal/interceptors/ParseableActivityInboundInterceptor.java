package com.parseable.temporal.interceptors;

import com.parseable.temporal.ParseableEmitter;

import io.temporal.activity.ActivityExecutionContext;
import io.temporal.activity.ActivityInfo;
import io.temporal.common.interceptors.ActivityInboundCallsInterceptor;
import io.temporal.common.interceptors.ActivityInboundCallsInterceptorBase;

/**
 * Intercepts inbound activity executions and emits start/complete/failed events to Parseable.
 */
public final class ParseableActivityInboundInterceptor
    extends ActivityInboundCallsInterceptorBase {

  private final ParseableEmitter emitter;
  private ActivityExecutionContext context;

  public ParseableActivityInboundInterceptor(
      ActivityInboundCallsInterceptor next, ParseableEmitter emitter) {
    super(next);
    this.emitter = emitter;
  }

  @Override
  public void init(ActivityExecutionContext context) {
    this.context = context;
    super.init(context);
  }

  @Override
  public ActivityOutput execute(ActivityInput input) {
    ActivityInfo info = context.getInfo();
    String workflowId = info.getWorkflowId();
    String activityType = info.getActivityType();
    String taskQueue = info.getActivityTaskQueue();

    emitter.emitActivityEvent(workflowId, activityType, taskQueue, "started", null);
    try {
      ActivityOutput output = super.execute(input);
      emitter.emitActivityEvent(workflowId, activityType, taskQueue, "completed", null);
      return output;
    } catch (Exception e) {
      emitter.emitActivityEvent(workflowId, activityType, taskQueue, "failed", e.getMessage());
      throw e;
    }
  }
}
