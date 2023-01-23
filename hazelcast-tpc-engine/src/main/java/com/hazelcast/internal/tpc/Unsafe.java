package com.hazelcast.internal.tpc;

import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.util.CachedNanoClock;
import com.hazelcast.internal.tpc.util.NanoClock;
import com.hazelcast.internal.tpc.util.StandardNanoClock;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

/**
 * Exposes methods that should only be called from within the {@link Eventloop}.
 */
public abstract class Unsafe {

    private final Eventloop eventloop;
    protected final NanoClock nanoClock;
    public ArrayDeque<Runnable> localTaskQueue;

    protected Unsafe(Eventloop eventloop) {
        this.localTaskQueue = eventloop.localTaskQueue;
        this.eventloop = eventloop;
        this.nanoClock = eventloop.clockRefreshInterval == 0
                ? new StandardNanoClock()
                : new CachedNanoClock(eventloop.clockRefreshInterval);
    }

    public NanoClock nanoClock() {
        return nanoClock;
    }

    /**
     * Returns the {@link Eventloop} that belongs to this {@link Unsafe} instance.
     *
     * @return the Eventloop.
     */
    public Eventloop eventloop() {
        return eventloop;
    }

    /**
     * Creates a new {@link AsyncFile} with the given path.
     *
     * @param path the path to the AsyncFile.
     * @return the created AsyncFile.
     */
    public abstract AsyncFile newAsyncFile(String path);

    /**
     * Returns the IOBufferAllocator needed to interact with the {@link AsyncFile}.
     * <p/>
     * These buffers could have special requirements. E.g. when Direct I/O is used, the
     * buffers need to be 4KB aligned and direct.
     *
     * @return t
     */
    public abstract IOBufferAllocator fileIOBufferAllocator();

    public <E> Fut<E> newCompletedFuture(E value) {
        Fut<E> fut = eventloop.futAllocator.allocate();
        fut.complete(value);
        return fut;
    }

    public <E> Fut<E> newFut() {
        return eventloop.futAllocator.allocate();
    }

    /**
     * Offers a task to be scheduled on the eventloop.
     *
     * @param task the task to schedule.
     * @return true if the task was successfully offered, false otherwise.
     */
    public boolean offer(Runnable task) {
        return localTaskQueue.offer(task);
    }

    /**
     * Schedules a one shot action with the given delay.
     *
     * @param task  the task to execute.
     * @param delay the delay
     * @param unit  the unit of the delay
     * @return true if the task was successfully scheduled.
     * @throws NullPointerException     if task or unit is null
     * @throws IllegalArgumentException when delay smaller than 0.
     */
    public boolean schedule(Runnable task, long delay, TimeUnit unit) {
        checkNotNull(task);
        checkNotNegative(delay, "delay");
        checkNotNull(unit);

        ScheduledTask scheduledTask = new ScheduledTask(eventloop);
        scheduledTask.task = task;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        return eventloop.scheduledTaskQueue.offer(scheduledTask);
    }

    /**
     * Creates a periodically executing task with a fixed delay between the completion and start of
     * the task.
     *
     * @param task         the task to periodically execute.
     * @param initialDelay the initial delay
     * @param delay        the delay between executions.
     * @param unit         the unit of the initial delay and delay
     * @return true if the task was successfully executed.
     */
    public boolean scheduleWithFixedDelay(Runnable task, long initialDelay, long delay, TimeUnit unit) {
        checkNotNull(task);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(delay, "delay");
        checkNotNull(unit);

        ScheduledTask scheduledTask = new ScheduledTask(eventloop);
        scheduledTask.task = task;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(initialDelay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        scheduledTask.delayNanos = unit.toNanos(delay);
        return eventloop.scheduledTaskQueue.offer(scheduledTask);
    }

    /**
     * Creates a periodically executing task with a fixed delay between the start of the task.
     *
     * @param task         the task to periodically execute.
     * @param initialDelay the initial delay
     * @param period       the period between executions.
     * @param unit         the unit of the initial delay and delay
     * @return true if the task was successfully executed.
     */
    public boolean scheduleAtFixedRate(Runnable task, long initialDelay, long period, TimeUnit unit) {
        checkNotNull(task);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(period, "period");
        checkNotNull(unit);

        ScheduledTask scheduledTask = new ScheduledTask(eventloop);
        scheduledTask.task = task;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(initialDelay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        scheduledTask.periodNanos = unit.toNanos(period);
        return eventloop.scheduledTaskQueue.offer(scheduledTask);
    }

    public Fut sleep(long delay, TimeUnit unit) {
        checkNotNegative(delay, "delay");
        checkNotNull(unit, "unit");

        Fut fut = newFut();
        ScheduledTask scheduledTask = new ScheduledTask(eventloop);
        scheduledTask.fut = fut;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        eventloop.scheduledTaskQueue.add(scheduledTask);
        return fut;
    }

    public <I, O> Fut<List<O>> map(List<I> input, List<O> output, Function<I, O> function) {
        Fut fut = newFut();

        //todo: task can be pooled
        Runnable task = new Runnable() {
            Iterator<I> it = input.iterator();

            @Override
            public void run() {
                if (it.hasNext()) {
                    I item = it.next();
                    O result = function.apply(item);
                    output.add(result);
                }

                if (it.hasNext()) {
                    eventloop.unsafe.offer(this);
                } else {
                    fut.complete(output);
                }
            }
        };

        eventloop.execute(task);
        return fut;
    }

    /**
     * Keeps calling the loop function until it returns false.
     *
     * @param loopFunction the function that is called in a loop.
     * @return the future that is completed as soon as the loop finishes.
     */
    public Fut loop(Function<Eventloop, Boolean> loopFunction) {
        Fut fut = newFut();

        //todo: task can be pooled
        Runnable task = new Runnable() {
            @Override
            public void run() {
                if (loopFunction.apply(eventloop)) {
                    eventloop.unsafe.offer(this);
                } else {
                    fut.complete(null);
                }
            }
        };
        eventloop.execute(task);
        return fut;
    }
}
