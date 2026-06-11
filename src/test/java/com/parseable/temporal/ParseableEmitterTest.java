package com.parseable.temporal;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
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
    SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
        .build();
    OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
        .setTracerProvider(tracerProvider)
        .setLoggerProvider(SdkLoggerProvider.builder().build())
        .build();
    ParseableEmitter emitter = new ParseableEmitter(testConfig(), sdk);

    emitter.emitWorkflowEvent("wf-1", "HelloWorkflow", "queue", "started", null);
    Thread.sleep(2);
    emitter.emitActivityEvent("wf-1", "activity-1", "HelloActivity", "queue", "started", null);
    Thread.sleep(2);
    emitter.emitActivityEvent("wf-1", "activity-1", "HelloActivity", "queue", "completed", null);
    emitter.emitWorkflowEvent("wf-1", "HelloWorkflow", "queue", "completed", null);
    emitter.close();

    SpanData workflow = spanNamed(exporter.spans, "workflow.HelloWorkflow");
    SpanData activity = spanNamed(exporter.spans, "activity.HelloActivity");

    assertTrue(workflow.getEndEpochNanos() > workflow.getStartEpochNanos());
    assertTrue(activity.getEndEpochNanos() > activity.getStartEpochNanos());
    assertEquals(workflow.getSpanContext().getSpanId(),
        activity.getParentSpanContext().getSpanId());
  }

  private static SpanData spanNamed(List<SpanData> spans, String name) {
    return spans.stream()
        .filter(span -> name.equals(span.getName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing span: " + name));
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
