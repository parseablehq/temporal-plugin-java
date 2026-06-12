package com.parseable.temporal.interceptors;

import io.temporal.client.ActivityCanceledException;
import io.temporal.failure.CanceledFailure;

import java.util.concurrent.CancellationException;

final class LifecycleStatus {

  static final String STARTED = "started";
  static final String COMPLETED = "completed";
  static final String FAILED = "failed";
  static final String CANCELED = "canceled";

  private LifecycleStatus() { }

  static String fromThrowable(Throwable throwable) {
    return isCancellation(throwable) ? CANCELED : FAILED;
  }

  private static boolean isCancellation(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof CanceledFailure
          || current instanceof ActivityCanceledException
          || current instanceof CancellationException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
