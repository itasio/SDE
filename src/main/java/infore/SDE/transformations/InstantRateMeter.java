package infore.SDE.transformations;

import org.apache.flink.metrics.Meter;

public class InstantRateMeter implements Meter {

    private long lastEventTimeNanos = -1;
    private double currentRate = 0.0;

    @Override
    public void markEvent() {
        markEvent(1);
    }

    @Override
    public void markEvent(long n) {
        long now = System.nanoTime();
        if (lastEventTimeNanos > 0) {
            long nanosBetweenEvents = now - lastEventTimeNanos;
            if (nanosBetweenEvents > 0) {
                // Rate per second, not smoothed
                currentRate = (n / (nanosBetweenEvents / 1_000_000_000.0));
            }
        }
        lastEventTimeNanos = now;
    }

    @Override
    public double getRate() {
        return currentRate;
    }

    @Override
    public long getCount() {
        // Not tracked here; optional to implement
        return -1;
    }
}
