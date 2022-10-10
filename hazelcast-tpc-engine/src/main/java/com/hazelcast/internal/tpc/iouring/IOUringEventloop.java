/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.util.LongObjectHashMap;
import com.hazelcast.internal.tpc.util.NanoClock;
import com.hazelcast.internal.tpc.util.UnsafeLocator;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.hazelcast.internal.tpc.Eventloop.State.RUNNING;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_TIMEOUT;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_SETUP_IOPOLL;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpc.util.CloseUtil.closeAllQuietly;
import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpc.util.OS.pageSize;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

/**
 * io_uring implementation of the {@link Eventloop}.
 *
 * <p>
 * Good read:
 * https://unixism.net/2020/04/io-uring-by-example-part-3-a-web-server-with-io-uring/
 * <p>
 * Another example (blocking socket)
 * https://github.com/ddeka0/AsyncIO/blob/master/src/asyncServer.cpp
 * <p>
 * no syscalls:
 * https://wjwh.eu/posts/2021-10-01-no-syscall-server-iouring.html
 */
public class IOUringEventloop extends Eventloop {

    private final static sun.misc.Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private final IOUring uring;
    private final IOUringEventloopBuilder config;
    private final CompletionQueue cq;
    //todo: Litter; we need to come up with better solution.
    protected final Set<AutoCloseable> closeables = new CopyOnWriteArraySet<>();

    private final long timeoutSpecAddr = UNSAFE.allocateMemory(Linux.SIZEOF_KERNEL_TIMESPEC);
    private long userdata_timeout;

    private final EventFd eventfd = new EventFd();
    private final long eventFdReadBuf = UNSAFE.allocateMemory(SIZEOF_LONG);
    private long userdata_eventRead;

    SubmissionQueue sq;
    protected final StorageDeviceRegistry storageScheduler;
    private EventloopHandler eventLoopHandler;

    public IOUringEventloop() {
        this(new IOUringEventloopBuilder());
    }

    public IOUringEventloop(IOUringEventloopBuilder eventloopBuilder) {
        super(eventloopBuilder);
        this.config = eventloopBuilder;
        this.uring = new IOUring(eventloopBuilder.uringSize, eventloopBuilder.flags);
        this.sq = uring.getSubmissionQueue();
        this.cq = uring.getCompletionQueue();
        this.storageScheduler = eventloopBuilder.ioRequestScheduler;
    }

    /**
     * Registers an AutoCloseable on this Eventloop.
     * <p>
     * Registered closeable are automatically closed when the eventloop closes.
     * Some examples: AsyncSocket and AsyncServerSocket.
     * <p>
     * If the Eventloop isn't in the running state, false is returned.
     * <p>
     * This method is thread-safe.
     *
     * @param closeable the AutoCloseable to register
     * @return true if the closeable was successfully register, false otherwise.
     * @throws NullPointerException if closeable is null.
     */
    public boolean registerCloseable(AutoCloseable closeable) {
        checkNotNull(closeable, "closeable");

        if (state != RUNNING) {
            return false;
        }

        closeables.add(closeable);

        if (state != RUNNING) {
            closeables.remove(closeable);
            return false;
        }

        return true;
    }

    /**
     * Deregisters an AutoCloseable from this Eventloop.
     * <p>
     * This method is thread-safe.
     * <p>
     * This method can be called no matter the state of the Eventloop.
     *
     * @param closeable the AutoCloseable to deregister.
     */
    public void deregisterCloseable(AutoCloseable closeable) {
         closeables.remove(checkNotNull(closeable, "closeable"));
    }

    @Override
    public AsyncServerSocket openTcpAsyncServerSocket() {
        return IOUringAsyncServerSocket.openTcpServerSocket(this);
    }

    @Override
    public AsyncSocket openTcpAsyncSocket() {
        return IOUringAsyncSocket.openTcpSocket();
    }

    @Override
    protected Unsafe createUnsafe() {
        return new IOUringUnsafe();
    }

    @Override
    public final IOUringUnsafe unsafe() {
        return (IOUringUnsafe) unsafe;
    }

    @Override
    protected void beforeEventloop() {
        eventLoopHandler = new EventloopHandler();
        storageScheduler.init(this);
        IOUringUnsafe unsafe = unsafe();
        this.userdata_eventRead = unsafe.nextUserdata();
        this.userdata_timeout = unsafe.nextUserdata();
        unsafe.handlers.put(userdata_eventRead, new EventFdCompletionHandler());
        unsafe.handlers.put(userdata_timeout, new TimeoutCompletionHandler());
    }

    @Override
    protected void afterEventloop() {
        closeQuietly(uring);
        closeQuietly(eventfd);
        closeAllQuietly(closeables);
        closeables.clear();

        if (timeoutSpecAddr != 0) {
            UNSAFE.freeMemory(timeoutSpecAddr);
        }

        if (eventFdReadBuf != 0) {
            UNSAFE.freeMemory(eventFdReadBuf);
        }
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == eventloopThread) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            eventfd.write(1L);
        }
    }

    @Override
    protected void eventLoop() {
        if ((config.flags & IORING_SETUP_IOPOLL) != 0) {
            throw new UnsupportedOperationException();
        } else {
            NanoClock nanoClock = unsafe.nanoClock();
            sq_offerEventFdRead();

            boolean moreWork = false;
            do {
                if (cq.hasCompletions()) {
                    // todo: do we want to control number of events being processed.
                    cq.process(eventLoopHandler);
                } else if (spin || moreWork) {
                    sq.submit();
                } else {
                    wakeupNeeded.set(true);
                    if (concurrentTaskQueue.isEmpty()) {
                        if (earliestDeadlineNanos != -1) {
                            long timeoutNanos = earliestDeadlineNanos - nanoClock.nanoTime();
                            if (timeoutNanos > 0) {
                                sq_offerTimeout(timeoutNanos);
                                sq.submitAndWait();
                            } else {
                                sq.submit();
                            }
                        } else {
                            sq.submitAndWait();
                        }
                    } else {
                        sq.submit();
                    }
                    wakeupNeeded.set(false);
                }

                // what are the queues that are available for processing
                // 1: completion events
                // 2: concurrent task queue
                // 3: timed task queue
                // 4: local task queue
                // 5: scheduler task queue

                moreWork = runConcurrentTasks();
                moreWork |= scheduler.tick();
                moreWork |= runScheduledTasks();
                moreWork |= runLocalTasks();
            } while (state == State.RUNNING);
        }
    }

    // todo: I'm questioning of this is not going to lead to problems. Can it happen that
    // multiple timeout requests are offered? So one timeout request is scheduled while another command is
    // already in the pipeline. Then the thread waits, and this earlier command completes while the later
    // timeout command is still scheduled. If another timeout is scheduled, then you have 2 timeouts in the
    // uring and both share the same timeoutSpecAddr.
    private void sq_offerTimeout(long timeoutNanos) {
        if (timeoutNanos <= 0) {
            UNSAFE.putLong(timeoutSpecAddr, 0);
            UNSAFE.putLong(timeoutSpecAddr + SIZEOF_LONG, 0);
        } else {
            long seconds = timeoutNanos / 1_000_000_000;
            UNSAFE.putLong(timeoutSpecAddr, seconds);
            UNSAFE.putLong(timeoutSpecAddr + SIZEOF_LONG, timeoutNanos - seconds * 1_000_000_000);
        }

        // todo: return value isn't checked
        sq.offer(IORING_OP_TIMEOUT,
                0,
                0,
                -1,
                timeoutSpecAddr,
                1,
                0,
                userdata_timeout);
    }

    private void sq_offerEventFdRead() {
        // todo: we are not checking return value.
        sq.offer(IORING_OP_READ,
                0,
                0,
                eventfd.fd(),
                eventFdReadBuf,
                SIZEOF_LONG,
                0,
                userdata_eventRead);
    }

    private class EventloopHandler implements IOCompletionHandler {
        final LongObjectHashMap<IOCompletionHandler> handlers = unsafe().handlers;

        @Override
        public void handle(int res, int flags, long userdata) {
            IOCompletionHandler h = handlers.get(userdata);

            if (h == null) {
                logger.warning("no handler found for: " + userdata);
            } else {
                h.handle(res, flags, userdata);
            }
        }
    }


    private class EventFdCompletionHandler implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            sq_offerEventFdRead();
        }
    }

    private class TimeoutCompletionHandler implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
        }
    }

    public class IOUringUnsafe extends Unsafe {

        private long userdataGenerator = 0;
        final LongObjectHashMap<IOCompletionHandler> handlers = new LongObjectHashMap<>(4096);

        // this is not a very efficient allocator. It would be better to allocate a large chunk of
        // memory and then carve out smaller blocks. But for now it will do.
        private IOBufferAllocator storeIOBufferAllocator = new NonConcurrentIOBufferAllocator(4096, true, pageSize());

        public long nextUserdata() {
            return userdataGenerator++;
        }

        @Override
        public IOBufferAllocator fileIOBufferAllocator() {
            return storeIOBufferAllocator;
        }

        @Override
        public AsyncFile newAsyncFile(String path) {
            return new IOUringAsyncFile(path, IOUringEventloop.this);
        }
    }

}

