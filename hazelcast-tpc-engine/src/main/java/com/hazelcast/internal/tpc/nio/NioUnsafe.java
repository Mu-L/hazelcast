package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.Unsafe;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;

class NioUnsafe extends Unsafe {

    NioUnsafe(NioEventloop eventloop){
        super(eventloop);
    }

    @Override
    public IOBufferAllocator fileIOBufferAllocator() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public AsyncFile newAsyncFile(String path) {
        throw new RuntimeException("Not implemented");
    }
}
