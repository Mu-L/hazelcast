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

package com.hazelcast;

import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.EventloopBuilder;
import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.util.ThreadAffinity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.BenchmarkSupport.terminate;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpc.util.BufferUtil.put;

/**
 * A benchmarks that test the throughput of 2 sockets that are bouncing packets
 * with some payload between them.
 */
public abstract class AsyncSocketBounceBenchmark {
    // use small buffers to cause a lot of network scheduling overhead (and shake down problems)
    public static int socketBufferSize = 128 * 1024;
    public static boolean useDirectByteBuffers = true;
    public static int iterations = 2_000_000;
    public static int payloadSize = 1;
    public static int concurrency = 1;
    public static boolean tcpNoDelay = true;
    public static boolean spin = false;
    public String cpuAffinityClient = "1";
    public String cpuAffinityServer = "4";
    private Eventloop clientEventloop;
    private Eventloop serverEventloop;

    public abstract EventloopBuilder createEventloopBuilder();

    @Before
    public void before() {
        EventloopBuilder clientEventloopBuilder = createEventloopBuilder();
        clientEventloopBuilder.setSpin(spin);
        clientEventloopBuilder.setThreadNameSupplier(() -> "Client-Thread");
        clientEventloopBuilder.setThreadAffinity(cpuAffinityClient == null ? null : new ThreadAffinity(cpuAffinityClient));
        clientEventloop = clientEventloopBuilder.create();
        clientEventloop.start();

        EventloopBuilder serverEventloopBuilder = createEventloopBuilder();
        serverEventloopBuilder.setSpin(spin);
        serverEventloopBuilder.setThreadNameSupplier(() -> "Server-Thread");
        serverEventloopBuilder.setThreadAffinity(cpuAffinityServer == null ? null : new ThreadAffinity(cpuAffinityServer));
        serverEventloop = serverEventloopBuilder.create();
        serverEventloop.start();
    }

    @After
    public void after() {
        terminate(clientEventloop);
        terminate(serverEventloop);
    }

    @Test
    public void test() throws InterruptedException {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);

        AsyncServerSocket serverSocket = newServer(serverAddress);

        CountDownLatch latch = new CountDownLatch(concurrency);

        AsyncSocket clientSocket = newClient(serverAddress, latch);

        long start = System.currentTimeMillis();

        for (int k = 0; k < concurrency; k++) {
            byte[] payload = new byte[payloadSize];
            IOBuffer buf = new IOBuffer(SIZEOF_INT + SIZEOF_LONG + payload.length, true);
            buf.writeInt(payload.length);
            buf.writeLong(iterations / concurrency);
            buf.writeBytes(payload);
            buf.flip();
            if (!clientSocket.write(buf)) {
                throw new RuntimeException();
            }
        }
        clientSocket.flush();

        latch.await();

        long duration = System.currentTimeMillis() - start;
        System.out.println("Throughput:" + (iterations * 1000 / duration) + " ops");
    }

    private AsyncSocket newClient(SocketAddress serverAddress, CountDownLatch latch) {
        AsyncSocket clientSocket = clientEventloop.openTcpAsyncSocket();
        clientSocket.setTcpNoDelay(tcpNoDelay);
        clientSocket.setSendBufferSize(socketBufferSize);
        clientSocket.setReceiveBufferSize(socketBufferSize);
        clientSocket.setReadHandler(new ReadHandler() {
            private ByteBuffer payloadBuffer;
            private long round;
            private int payloadSize = -1;
            private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, useDirectByteBuffers);

            @Override
            public void onRead(ByteBuffer receiveBuffer) {
                for (; ; ) {
                    if (payloadSize == -1) {
                        if (receiveBuffer.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                            break;
                        }

                        payloadSize = receiveBuffer.getInt();
                        round = receiveBuffer.getLong();
                        if (round < 0) {
                            throw new RuntimeException("round can't be smaller than 0, found:" + round);
                        }
                        if (payloadBuffer == null) {
                            payloadBuffer = ByteBuffer.allocate(payloadSize);
                        } else {
                            payloadBuffer.clear();
                        }
                    }

                    put(payloadBuffer, receiveBuffer);

                    if (payloadBuffer.remaining() > 0) {
                        // not all bytes have been received.
                        break;
                    }
                    payloadBuffer.flip();
//
//                    if (round % 100 == 0) {
//                        System.out.println("client round:" + round);
//                    }

                    if (round == 0) {
                        latch.countDown();
                    } else {
                        IOBuffer responseBuf = responseAllocator.allocate(SIZEOF_INT + SIZEOF_LONG + payloadSize);
                        responseBuf.writeInt(payloadSize);
                        responseBuf.writeLong(round);
                        responseBuf.write(payloadBuffer);
                        responseBuf.flip();
                        if (!socket.unsafeWriteAndFlush(responseBuf)) {
                            throw new RuntimeException("Socket has no space");
                        }
                    }
                    payloadSize = -1;
                }
            }
        });
        clientSocket.activate(clientEventloop);
        clientSocket.connect(serverAddress).join();

        return clientSocket;
    }

    private AsyncServerSocket newServer(SocketAddress serverAddress) {
        AsyncServerSocket serverSocket = serverEventloop.openTcpAsyncServerSocket();
        serverSocket.setReceiveBufferSize(socketBufferSize);
        serverSocket.bind(serverAddress);

        serverSocket.accept(socket -> {
            socket.setTcpNoDelay(tcpNoDelay);
            socket.setSendBufferSize(socketBufferSize);
            socket.setReceiveBufferSize(serverSocket.getReceiveBufferSize());
            socket.setReadHandler(new ReadHandler() {
                private ByteBuffer payloadBuffer;
                private long round;
                private int payloadSize = -1;
                private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, useDirectByteBuffers);

                @Override
                public void onRead(ByteBuffer receiveBuffer) {

                    for (; ; ) {
                        if (payloadSize == -1) {
                            if (receiveBuffer.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                                break;
                            }
                            payloadSize = receiveBuffer.getInt();
                            round = receiveBuffer.getLong();
                            if (round < 0) {
                                throw new RuntimeException("round can't be smaller than 0, found:" + round);
                            }
                            if (payloadBuffer == null) {
                                payloadBuffer = ByteBuffer.allocate(payloadSize);
                            } else {
                                payloadBuffer.clear();
                            }
                        }

                        put(payloadBuffer, receiveBuffer);
                        if (payloadBuffer.remaining() > 0) {
                            // not all bytes have been received.
                            break;
                        }

//                        if (round % 100 == 0) {
//                            System.out.println("server round:" + round);
//                        }

                        payloadBuffer.flip();
                        IOBuffer responseBuf = responseAllocator.allocate(SIZEOF_INT + SIZEOF_LONG + payloadSize);
                        responseBuf.writeInt(payloadSize);
                        responseBuf.writeLong(round - 1);
                        responseBuf.write(payloadBuffer);
                        responseBuf.flip();
                        if (!socket.unsafeWriteAndFlush(responseBuf)) {
                            throw new RuntimeException("Socket has no space");
                        }
                        payloadSize = -1;
                    }
                }
            });
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }
}
