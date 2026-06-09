package com.parseable.temporal;

import com.parseable.temporal.exporters.SanitizingSpanExporter;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SanitizingSpanExporterTest {

  @Test
  void primitiveAttributesPassedThrough() {
    CapturingExporter delegate = new CapturingExporter();
    SanitizingSpanExporter exporter = new SanitizingSpanExporter(delegate);

    Attributes input = Attributes.builder()
        .put(AttributeKey.stringKey("service"), "order-worker")
        .put(AttributeKey.longKey("retry.count"), 3L)
        .put(AttributeKey.booleanKey("success"), true)
        .put(AttributeKey.doubleKey("duration.ms"), 12.5)
        .build();

    exporter.export(List.of(spanWith(input)));

    assertEquals(1, delegate.captured.size());
    Attributes out = delegate.captured.get(0).getAttributes();
    assertEquals("order-worker", out.get(AttributeKey.stringKey("service")));
    assertEquals(3L, out.get(AttributeKey.longKey("retry.count")));
    assertTrue(out.get(AttributeKey.booleanKey("success")));
    assertEquals(12.5, out.get(AttributeKey.doubleKey("duration.ms")));
  }

  @Test
  void stringArrayFlattenedToString() {
    CapturingExporter delegate = new CapturingExporter();
    SanitizingSpanExporter exporter = new SanitizingSpanExporter(delegate);

    Attributes input = Attributes.of(
        AttributeKey.stringArrayKey("tags"), List.of("a", "b", "c"));

    exporter.export(List.of(spanWith(input)));

    Attributes out = delegate.captured.get(0).getAttributes();
    String flat = out.get(AttributeKey.stringKey("tags"));
    assertEquals("a, b, c", flat);
  }

  @Test
  void longArrayFlattenedToString() {
    CapturingExporter delegate = new CapturingExporter();
    SanitizingSpanExporter exporter = new SanitizingSpanExporter(delegate);

    Attributes input = Attributes.of(
        AttributeKey.longArrayKey("codes"), List.of(1L, 2L, 3L));

    exporter.export(List.of(spanWith(input)));

    Attributes out = delegate.captured.get(0).getAttributes();
    String flat = out.get(AttributeKey.stringKey("codes"));
    assertEquals("1, 2, 3", flat);
  }

  @Test
  void flushAndShutdownDelegated() {
    CapturingExporter delegate = new CapturingExporter();
    SanitizingSpanExporter exporter = new SanitizingSpanExporter(delegate);

    exporter.flush();
    exporter.shutdown();

    assertTrue(delegate.flushed);
    assertTrue(delegate.shutdown);
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private static SpanData spanWith(Attributes attrs) {
    return new StubSpanData(attrs);
  }

  static class CapturingExporter implements SpanExporter {
    final List<SpanData> captured = new ArrayList<>();
    boolean flushed = false;
    boolean shutdown = false;

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
      captured.addAll(spans);
      return CompletableResultCode.ofSuccess();
    }
    @Override public CompletableResultCode flush() { flushed = true; return CompletableResultCode.ofSuccess(); }
    @Override public CompletableResultCode shutdown() { shutdown = true; return CompletableResultCode.ofSuccess(); }
  }

  /** Minimal SpanData stub for testing. */
  static class StubSpanData implements SpanData {
    private final Attributes attributes;
    StubSpanData(Attributes attributes) { this.attributes = attributes; }

    @Override public Attributes getAttributes() { return attributes; }
    @Override public String getName() { return "test-span"; }
    @Override public SpanKind getKind() { return SpanKind.INTERNAL; }
    @Override public SpanContext getSpanContext() {
      return SpanContext.create("00000000000000000000000000000001",
          "0000000000000001", TraceFlags.getSampled(), TraceState.getDefault());
    }
    @Override public SpanContext getParentSpanContext() { return SpanContext.getInvalid(); }
    @Override public StatusData getStatus() { return StatusData.ok(); }
    @Override public long getStartEpochNanos() { return 0L; }
    @Override public long getEndEpochNanos() { return 1000L; }
    @Override public List<EventData> getEvents() { return List.of(); }
    @Override public List<LinkData> getLinks() { return List.of(); }
    @Override public int getTotalAttributeCount() { return attributes.size(); }
    @Override public int getTotalRecordedEvents() { return 0; }
    @Override public int getTotalRecordedLinks() { return 0; }
    @Override public boolean hasEnded() { return true; }
    @Override public InstrumentationScopeInfo getInstrumentationScopeInfo() {
      return InstrumentationScopeInfo.create("test");
    }
    @Override public io.opentelemetry.sdk.common.InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
      return io.opentelemetry.sdk.common.InstrumentationLibraryInfo.create("test", null);
    }
    @Override public Resource getResource() { return Resource.empty(); }
  }
}
