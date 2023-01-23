package com.hazelcast;

import com.hazelcast.internal.tpc.EventloopBuilder;
import com.hazelcast.internal.tpc.iouring.IOUringEventloopBuilder;

public class IOUringAsyncSocketBounceBenchmark extends AsyncSocketBounceBenchmark {

    @Override
    public EventloopBuilder createEventloopBuilder() {
        return new IOUringEventloopBuilder();
    }

}
