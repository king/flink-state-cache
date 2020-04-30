package com.king.flink.metrics;

/**
 * Basically an interface for wrapping a Stopwatch with some additional
 * behavior that come in handy for metrics tracking.
 */
public interface Watch {
	void restart();
	void stopAndRecord();
	long getElapsedNanos();
}
