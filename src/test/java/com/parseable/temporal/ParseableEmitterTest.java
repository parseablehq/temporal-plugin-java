package com.parseable.temporal;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParseableEmitterTest {

  @Test
  void activitySpanIsChildOfWorkflowSpanAndSpansHaveDuration() throws Exception {
    CapturingExporter exporter = new CapturingExporter();
    ParseableEmitter emitter = new ParseableEmitter(testConfig(), sdkWith(exporter));

    emitter.emitWorkflowEvent("wf-1", "run-1", "HelloWorkflow", "queue", "started", null);
    Thread.sleep(2);
    emitter.emitActivityEvent(
        "wf-1", "run-1", "activity-1", "HelloActivity", "queue", "started", null);
    Thread.sleep(2);
    emitter.emitActivityEvent(
        "wf-1", "run-1", "activity-1", "HelloActivity", "queue", "completed", null);
    emitter.emitWorkflowEvent("wf-1", "run-1", "HelloWorkflow", "queue", "completed", null);
    emitter.close();

    SpanData workflow = spanNamed(exporter.spans, "workflow.HelloWorkflow");
    SpanData activity = spanNamed(exporter.spans, "activity.HelloActivity");

    assertTrue(workflow.getEndEpochNanos() > workflow.getStartEpochNanos());
    assertTrue(activity.getEndEpochNanos() > activity.getStartEpochNanos());
    assertEquals("run-1", workflow.getAttributes().get(workflowRunIdKey()));
    assertEquals(workflow.getSpanContext().getSpanId(),
        activity.getParentSpanContext().getSpanId());
  }

  @Test
  void terminalWorkflowEventExportsWithoutInMemoryStart() {
    CapturingExporter exporter = new CapturingExporter();
    ParseableEmitter emitter = new ParseableEmitter(testConfig(), sdkWith(exporter));

    emitter.emitWorkflowEvent("wf-1", "run-1", "HelloWorkflow", "queue", "completed", null);
    emitter.close();

    SpanData workflow = spanNamed(exporter.spans, "workflow.HelloWorkflow");
    assertEquals("completed", workflow.getAttributes().get(statusKey()));
    assertEquals("run-1", workflow.getAttributes().get(workflowRunIdKey()));
  }

  @Test
  void failedWorkflowSpanIsErrorWhenMessageIsNull() {
    CapturingExporter exporter = new CapturingExporter();
    ParseableEmitter emitter = new ParseableEmitter(testConfig(), sdkWith(exporter));

    emitter.emitWorkflowEvent("wf-1", "run-1", "HelloWorkflow", "queue", "failed", null);
    emitter.close();

    SpanData workflow = spanNamed(exporter.spans, "workflow.HelloWorkflow");
    assertEquals(StatusData.error(), workflow.getStatus());
  }

  @Test
  void canceledWorkflowSpanIsOkEvenWithMessage() {
    CapturingExporter exporter = new CapturingExporter();
    ParseableEmitter emitter = new ParseableEmitter(testConfig(), sdkWith(exporter));

    emitter.emitWorkflowEvent("wf-1", "run-1", "HelloWorkflow", "queue", "canceled", "bye");
    emitter.close();

    SpanData workflow = spanNamed(exporter.spans, "workflow.HelloWorkflow");
    assertEquals(StatusData.ok(), workflow.getStatus());
    assertEquals("canceled", workflow.getAttributes().get(statusKey()));
    assertNull(workflow.getAttributes().get(errorMessageKey()));
  }

  @Test
  void clientOperationSpanIncludesSafeMetadata() {
    CapturingExporter exporter = new CapturingExporter();
    ParseableEmitter emitter = new ParseableEmitter(testConfig(), sdkWith(exporter));

    emitter.emitClientOperation(
        "workflow.query",
        "wf-1",
        "run-1",
        "HelloWorkflow",
        "queue",
        null,
        "currentState",
        null,
        null,
        "completed",
        null,
        1_000_000_000L,
        1_125_000_000L);
    emitter.close();

    SpanData operation = spanNamed(exporter.spans, "temporal.client.workflow.query");
    assertEquals(SpanKind.CLIENT, operation.getKind());
    assertEquals(StatusData.ok(), operation.getStatus());
    assertEquals("workflow.query", operation.getAttributes().get(operationKey()));
    assertEquals("client", operation.getAttributes().get(operationKindKey()));
    assertEquals("currentState", operation.getAttributes().get(queryTypeKey()));
    assertEquals(125L, operation.getAttributes().get(durationMsKey()));
  }

  private static OpenTelemetrySdk sdkWith(CapturingExporter exporter) {
    SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
        .build();
    return OpenTelemetrySdk.builder()
        .setTracerProvider(tracerProvider)
        .setLoggerProvider(SdkLoggerProvider.builder().build())
        .build();
  }

  private static SpanData spanNamed(List<SpanData> spans, String name) {
    return spans.stream()
        .filter(span -> name.equals(span.getName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing span: " + name));
  }

  private static AttributeKey<String> workflowRunIdKey() {
    return AttributeKey.stringKey("temporal.workflow.run_id");
  }

  private static AttributeKey<String> statusKey() {
    return AttributeKey.stringKey("temporal.status");
  }

  private static AttributeKey<String> errorMessageKey() {
    return AttributeKey.stringKey("error.message");
  }

  private static AttributeKey<String> operationKey() {
    return AttributeKey.stringKey("temporal.operation");
  }

  private static AttributeKey<String> operationKindKey() {
    return AttributeKey.stringKey("temporal.operation.kind");
  }

  private static AttributeKey<String> queryTypeKey() {
    return AttributeKey.stringKey("temporal.query.type");
  }

  private static AttributeKey<Long> durationMsKey() {
    return AttributeKey.longKey("temporal.duration_ms");
  }

  private static ParseableConfig testConfig() {
    return ParseableConfig.builder()
        .endpoint("http://localhost:9999")
        .username("test")
        .password("test")
        .build();
  }

  static class CapturingExporter implements SpanExporter {
    final List<SpanData> spans = new ArrayList<>();

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
      this.spans.addAll(spans);
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }
  }
}
