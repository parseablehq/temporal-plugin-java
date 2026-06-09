package com.parseable.temporal.exporters;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link SpanExporter} wrapper that strips or flattens non-primitive span attributes before
 * forwarding to a delegate exporter.
 *
 * <p>Parseable's OTLP ingest endpoint rejects spans that contain array or map-type attribute
 * values. This exporter converts array attributes to a comma-joined string representation and
 * removes map-type attributes entirely, ensuring clean ingestion.
 *
 * <p>Primitive types passed through unchanged: {@code STRING}, {@code BOOLEAN}, {@code LONG},
 * {@code DOUBLE}.
 */
public final class SanitizingSpanExporter implements SpanExporter {

  private final SpanExporter delegate;

  public SanitizingSpanExporter(SpanExporter delegate) {
    this.delegate = delegate;
  }

  @Override
  public CompletableResultCode export(Collection<SpanData> spans) {
    List<SpanData> sanitized = new ArrayList<>(spans.size());
    for (SpanData span : spans) {
      sanitized.add(sanitizeSpan(span));
    }
    return delegate.export(sanitized);
  }

  @Override
  public CompletableResultCode flush() {
    return delegate.flush();
  }

  @Override
  public CompletableResultCode shutdown() {
    return delegate.shutdown();
  }

  // ── Sanitization logic ────────────────────────────────────────────────────

  private SpanData sanitizeSpan(SpanData span) {
    Attributes original = span.getAttributes();
    AttributesBuilder builder = Attributes.builder();
    boolean[] changed = { false };

    original.forEach((key, value) -> {
      String type = key.getType().name(); // STRING, BOOLEAN, LONG, DOUBLE, STRING_ARRAY, ...
      switch (type) {
        case "STRING":
        case "BOOLEAN":
        case "LONG":
        case "DOUBLE":
          putUnchecked(builder, key, value);
          break;
        case "STRING_ARRAY":
          @SuppressWarnings("unchecked")
          List<String> strings = (List<String>) value;
          builder.put(key.getKey(), String.join(", ", strings));
          changed[0] = true;
          break;
        case "BOOLEAN_ARRAY":
          @SuppressWarnings("unchecked")
          List<Boolean> booleans = (List<Boolean>) value;
          builder.put(key.getKey(), joinToString(booleans));
          changed[0] = true;
          break;
        case "LONG_ARRAY":
          @SuppressWarnings("unchecked")
          List<Long> longs = (List<Long>) value;
          builder.put(key.getKey(), joinToString(longs));
          changed[0] = true;
          break;
        case "DOUBLE_ARRAY":
          @SuppressWarnings("unchecked")
          List<Double> doubles = (List<Double>) value;
          builder.put(key.getKey(), joinToString(doubles));
          changed[0] = true;
          break;
        default:
          // Unknown / map type — drop silently
          changed[0] = true;
          break;
      }
    });

    if (!changed[0]) {
      return span;
    }
    return new SanitizedSpanData(span, builder.build());
  }

  @SuppressWarnings("unchecked")
  private static void putUnchecked(AttributesBuilder builder, AttributeKey<?> key, Object value) {
    builder.put((AttributeKey<Object>) key, value);
  }

  private static <T> String joinToString(List<T> list) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < list.size(); i++) {
      if (i > 0) { sb.append(", "); }
      sb.append(list.get(i));
    }
    return sb.toString();
  }
}
