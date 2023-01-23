package com.hazelcast;

import com.hazelcast.internal.tpc.EventloopBuilder;
import com.hazelcast.internal.tpc.nio.NioEventloopBuilder;

public class NioAsyncSocketBounceBenchmark extends AsyncSocketBounceBenchmark {

    @Override
    public EventloopBuilder createEventloopBuilder() {
        return new NioEventloopBuilder();
    }
}
