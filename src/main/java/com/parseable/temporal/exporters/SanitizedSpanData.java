package com.parseable.temporal.exporters;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;

import java.util.List;

/**
 * A thin {@link SpanData} wrapper that delegates everything to an original span but
 * substitutes a sanitized {@link Attributes} instance.
 */
final class SanitizedSpanData implements SpanData {

  private final SpanData delegate;
  private final Attributes sanitizedAttributes;

  SanitizedSpanData(SpanData delegate, Attributes sanitizedAttributes) {
    this.delegate = delegate;
    this.sanitizedAttributes = sanitizedAttributes;
  }

  @Override public Attributes getAttributes() { return sanitizedAttributes; }

  // ── All other methods delegate straight through ───────────────────────────

  @Override public String getName() { return delegate.getName(); }
  @Override public SpanKind getKind() { return delegate.getKind(); }
  @Override public SpanContext getSpanContext() { return delegate.getSpanContext(); }
  @Override public SpanContext getParentSpanContext() { return delegate.getParentSpanContext(); }
  @Override public StatusData getStatus() { return delegate.getStatus(); }
  @Override public long getStartEpochNanos() { return delegate.getStartEpochNanos(); }
  @Override public long getEndEpochNanos() { return delegate.getEndEpochNanos(); }
  @Override public List<EventData> getEvents() { return delegate.getEvents(); }
  @Override public List<LinkData> getLinks() { return delegate.getLinks(); }
  @Override public int getTotalAttributeCount() { return sanitizedAttributes.size(); }
  @Override public int getTotalRecordedEvents() { return delegate.getTotalRecordedEvents(); }
  @Override public int getTotalRecordedLinks() { return delegate.getTotalRecordedLinks(); }
  @Override public boolean hasEnded() { return delegate.hasEnded(); }
  @Override public InstrumentationScopeInfo getInstrumentationScopeInfo() {
    return delegate.getInstrumentationScopeInfo();
  }

  @Override
  public io.opentelemetry.sdk.common.InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
    return delegate.getInstrumentationLibraryInfo();
  }
  @Override public Resource getResource() { return delegate.getResource(); }
}
