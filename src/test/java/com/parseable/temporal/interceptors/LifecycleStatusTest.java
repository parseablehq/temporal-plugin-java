package com.parseable.temporal.interceptors;

import io.temporal.client.ActivityCanceledException;
import io.temporal.failure.CanceledFailure;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LifecycleStatusTest {

  @Test
  void cancellationExceptionsAreCanceled() {
    assertEquals(LifecycleStatus.CANCELED,
        LifecycleStatus.fromThrowable(new CanceledFailure("canceled")));
    assertEquals(LifecycleStatus.CANCELED,
        LifecycleStatus.fromThrowable(new ActivityCanceledException()));
    assertEquals(LifecycleStatus.CANCELED,
        LifecycleStatus.fromThrowable(new CancellationException("canceled")));
  }

  @Test
  void cancellationCauseIsCanceled() {
    RuntimeException wrapped = new RuntimeException(new CanceledFailure("canceled"));

    assertEquals(LifecycleStatus.CANCELED, LifecycleStatus.fromThrowable(wrapped));
  }

  @Test
  void ordinaryExceptionsAreFailed() {
    assertEquals(LifecycleStatus.FAILED,
        LifecycleStatus.fromThrowable(new RuntimeException("boom")));
  }
}
