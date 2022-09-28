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

package com.hazelcast.tpc.requestservice;

import com.hazelcast.tpc.engine.iobuffer.IOBuffer;
import com.hazelcast.tpc.engine.iobuffer.IOBufferAllocator;
import com.hazelcast.tpc.engine.nio.NioAsyncReadHandler;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.tpc.requestservice.FrameCodec.FLAG_OP_RESPONSE;

public class RequestNioReadHandler extends NioAsyncReadHandler {

    private IOBuffer inboundFrame;
    public IOBufferAllocator requestIOBufferAllocator;
    public IOBufferAllocator remoteResponseIOBufferAllocator;
    public OpScheduler opScheduler;
    public Consumer<IOBuffer> responseHandler;

    @Override
    public void onRead(ByteBuffer buffer) {
        IOBuffer responseChain = null;
        for (; ; ) {
            if (inboundFrame == null) {
                if (buffer.remaining() < INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES) {
                    break;
                }

                int size = buffer.getInt();
                int flags = buffer.getInt();
                if ((flags & FLAG_OP_RESPONSE) == 0) {
                    inboundFrame = requestIOBufferAllocator.allocate(size);
                } else {
                    inboundFrame = remoteResponseIOBufferAllocator.allocate(size);
                }
                inboundFrame.byteBuffer().limit(size);
                inboundFrame.writeInt(size);
                inboundFrame.writeInt(flags);
                inboundFrame.socket = socket;
            }

            int size = inboundFrame.size();
            int remaining = size - inboundFrame.position();
            inboundFrame.write(buffer, remaining);

            if (!inboundFrame.isComplete()) {
                break;
            }

            inboundFrame.reconstructComplete();
            //framesRead.inc();

            if (inboundFrame.isFlagRaised(FLAG_OP_RESPONSE)) {
                inboundFrame.next = responseChain;
                responseChain = inboundFrame;
            } else {
                opScheduler.schedule(inboundFrame);
            }
            inboundFrame = null;
        }

        if (responseChain != null) {
            responseHandler.accept(responseChain);
        }
    }
}