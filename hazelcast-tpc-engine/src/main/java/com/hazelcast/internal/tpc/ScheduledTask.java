package com.hazelcast.internal.tpc;

final class ScheduledTask implements Runnable, Comparable<ScheduledTask> {

    private final Eventloop eventloop;
    Fut fut;
    long deadlineNanos;
    Runnable task;
    long periodNanos = -1;
    long delayNanos = -1;

    public ScheduledTask(Eventloop eventloop) {
        this.eventloop = eventloop;
    }

    @Override
    public void run() {
        if (task != null) {
            task.run();
        }

        if (periodNanos != -1 || delayNanos != -1) {
            if (periodNanos != -1) {
                deadlineNanos += periodNanos;
            } else {
                deadlineNanos = eventloop.unsafe.nanoClock.nanoTime() + delayNanos;
            }

            if (deadlineNanos < 0) {
                deadlineNanos = Long.MAX_VALUE;
            }

            if (!eventloop.scheduledTaskQueue.offer(this)) {
                //todo: some log message
            }
        } else {
            if (fut != null) {
                fut.complete(null);
            }
        }
    }

    @Override
    public int compareTo(ScheduledTask that) {
        if (that.deadlineNanos == this.deadlineNanos) {
            return 0;
        }

        return this.deadlineNanos > that.deadlineNanos ? 1 : -1;
    }
}
