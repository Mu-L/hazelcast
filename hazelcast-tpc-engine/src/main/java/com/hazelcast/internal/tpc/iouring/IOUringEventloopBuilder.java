package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.EventloopBuilder;
import com.hazelcast.internal.tpc.EventloopType;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;

/**
 * Contains the configuration for the {@link IOUringEventloop}.
 */
public class IOUringEventloopBuilder extends EventloopBuilder {
    private static final int DEFAULT_IOURING_SIZE = 8192;

    int flags;
    int uringSize = DEFAULT_IOURING_SIZE;
    StorageDeviceRegistry ioRequestScheduler = new StorageDeviceRegistry();

    public IOUringEventloopBuilder() {
        super(EventloopType.IOURING);
    }

    @Override
    public Eventloop create() {
        return new IOUringEventloop(this);
    }

    public void setFlags(int flags) {
        this.flags = checkNotNegative(flags, "flags");
    }

    public void setUringSize(int uringSize) {
        this.uringSize = checkPositive(uringSize, "uringSize");
    }

    public void setStorageDeviceRegistry(StorageDeviceRegistry ioRequestScheduler) {
        this.ioRequestScheduler = checkNotNull(ioRequestScheduler);
    }
}
