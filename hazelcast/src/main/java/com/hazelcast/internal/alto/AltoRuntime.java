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

package com.hazelcast.internal.alto;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.TcpServer;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Configuration;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.EventloopBuilder;
import com.hazelcast.internal.tpc.EventloopType;
import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.TpcEngine;
import com.hazelcast.internal.tpc.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.UnpooledIOBufferAllocator;
import com.hazelcast.internal.tpc.iouring.IOUringEventloopBuilder;
import com.hazelcast.internal.tpc.nio.NioAsyncSocket;
import com.hazelcast.internal.tpc.nio.NioEventloopBuilder;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.table.impl.TableManager;
import com.hazelcast.table.impl.TopicManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.hazelcast.internal.alto.FrameCodec.OFFSET_REQ_CALL_ID;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * The AltoRuntime is runtime that provides the infrastructure to build next generation data-structures.
 * For more information see:
 * https://www.micahlerner.com/2022/06/04/data-parallel-actors-a-programming-model-for-scalable-query-serving-systems.html
 * = * <p>
 * Mapping from partition to CPU is easy; just a simple mod.
 * <p>
 * RSS: How can we align:
 * - the CPU receiving data from some TCP/IP-connection.
 * - and pinning the same CPU to the RX-queue that processes that TCP/IP-connection
 * So how can we make sure that all TCP/IP-connections for that CPU are processed by the same CPU processing the IRQ.
 * <p>
 * And how can we make sure that for example we want to isolate a few CPUs for the RSS part, but then
 * forward to the CPU that owns the TCP/IP-connection
 * <p>
 * So it appears that Seastar is using the toeplitz hash
 * https://github.com/scylladb/seastar/issues/654
 * <p>
 * So we have a list of channels to some machine.
 * <p>
 * And we determine for each of the channel the toeplitz hash based on src/dst port/ip;
 * <p>
 * So this would determine which channels are mapped to some CPU.
 * <p>
 * So how do we go from partition to a channel?
 */
public class AltoRuntime {

    public final Node node;
    public final InternalSerializationService ss;
    public final ILogger logger;
    private final Address thisAddress;
     private final SocketConfig socketConfig;
    private final boolean poolRequests;
    private final boolean poolRemoteResponses;
    private final boolean writeThrough;
    private final int requestTimeoutMs;
    private final boolean regularSchedule;
    private ResponseHandler responseHandler;
    private volatile ServerConnectionManager connectionManager;
    public volatile boolean shuttingdown = false;
    public Managers managers;
    private RequestRegistry requestRegistry;
    private TpcEngine tpcEngine;
    private final int concurrentRequestLimit;
    private final Map<Eventloop, Supplier<? extends ReadHandler>> readHandlerSuppliers = new HashMap<>();
    private PartitionActorRef[] partitionActorRefs;
    private ArrayList<OpScheduler> schedulers = new ArrayList<>();

    public AltoRuntime(Node node) {
        this.node = node;
        this.logger = node.getLogger(AltoRuntime.class);
        this.ss = (InternalSerializationService) node.nodeEngine.getSerializationService();
        this.writeThrough = parseBoolean(getProperty("hazelcast.tpc.write-through", "false"));
        this.regularSchedule = parseBoolean(getProperty("hazelcast.tpc.regular-schedule", "true"));
        this.poolRequests = parseBoolean(getProperty("hazelcast.alto.pool-requests", "true"));
        boolean poolLocalResponses = parseBoolean(getProperty("hazelcast.alto.pool-local-responses", "true"));
        this.poolRemoteResponses = parseBoolean(getProperty("hazelcast.alto.pool-remote-responses", "false"));
        this.concurrentRequestLimit = parseInt(getProperty("hazelcast.alto.concurrent-request-limit", "-1"));
        this.requestTimeoutMs = parseInt(getProperty("hazelcast.alto.request.timeoutMs", "23000"));
        this.thisAddress = node.getThisAddress();
        this.socketConfig = new SocketConfig();
    }

    public TpcEngine getTpcEngine() {
        return tpcEngine;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public PartitionActorRef[] partitionActorRefs() {
        return partitionActorRefs;
    }

    public void start() {
        logger.info("AltoRuntime starting");

        this.managers = new Managers();
        InternalPartitionService partitionService = node.nodeEngine.getPartitionService();
        managers.tableManager = new TableManager(partitionService.getPartitionCount());
        managers.topicManager = new TopicManager(partitionService.getPartitionCount());

        this.partitionActorRefs = new PartitionActorRef[partitionService.getPartitionCount()];
        this.requestRegistry = new RequestRegistry(concurrentRequestLimit, partitionActorRefs.length);
        int responseThreadCount = parseInt(getProperty("hazelcast.alto.responsethread.count", "1"));
        boolean responseThreadSpin = parseBoolean(getProperty("hazelcast.alto.responsethread.spin", "false"));
        this.responseHandler = new ResponseHandler(responseThreadCount,
                responseThreadSpin,
                requestRegistry);

        responseHandler.start();

        EventloopType eventloopType = EventloopType.fromString(getProperty("hazelcast.alto.eventloop.type", "nio"));
        EventloopBuilder eventloopBuilder;
        switch (eventloopType) {
            case NIO:
                eventloopBuilder = new NioEventloopBuilder();
                break;
            case IOURING:
                eventloopBuilder = new IOUringEventloopBuilder();
                break;
            default:
                throw new IllegalStateException("Unhandeled eventlooptype: " + eventloopType);
        }
        eventloopBuilder.setThreadFactory(AltoOperationThread::new);
        AtomicInteger threadId = new AtomicInteger();
        eventloopBuilder.setThreadNameSupplier(() -> "alto-thread-" + threadId.getAndIncrement());

        eventloopBuilder.setSchedulerSupplier(() -> {
            // remote responses will be created and released by the TPC thread.
            // So a non-concurrent allocator is good enough.
            IOBufferAllocator remoteResponseAllocator = new NonConcurrentIOBufferAllocator(128, true);
            // local responses will be created by the TPC thread, but will be released by a user thread.
            // So a concurrent allocator is needed.
            IOBufferAllocator localResponseAllocator = new ConcurrentIOBufferAllocator(128, true);

            OpScheduler scheduler = new OpScheduler(
                    32768,
                    Integer.MAX_VALUE,
                    managers,
                    localResponseAllocator,
                    remoteResponseAllocator,
                    responseHandler);
            schedulers.add(scheduler);
            return scheduler;
        });
        Configuration engineConfig = new Configuration();
        engineConfig.setEventloopBuilder(eventloopBuilder);
        tpcEngine = new TpcEngine(engineConfig);
        tpcEngine.start();

        startNetworking();

        for (int partitionId = 0; partitionId < partitionActorRefs.length; partitionId++) {
            partitionActorRefs[partitionId] = new PartitionActorRef(
                    partitionId,
                    partitionService,
                    tpcEngine,
                    this,
                    thisAddress,
                    requestRegistry.getByPartitionId(partitionId));
        }

        logger.info("AltoRuntime started");
    }

    private void startNetworking() {
        List<Integer> tpcPorts = new ArrayList<>();
        for (int k = 0; k < tpcEngine.eventloopCount(); k++) {
            Eventloop eventloop = tpcEngine.eventloop(k);
            try {
                Supplier<ReadHandler> readHandlerSupplier = () -> {
                    RequestReadHandler readHandler = new RequestReadHandler();
                    readHandler.opScheduler = (OpScheduler) eventloop.scheduler();
                    readHandler.responseHandler = responseHandler;
                    readHandler.requestIOBufferAllocator = poolRequests
                            ? new NonConcurrentIOBufferAllocator(128, true)
                            : new UnpooledIOBufferAllocator();
                    readHandler.remoteResponseIOBufferAllocator = poolRemoteResponses
                            ? new ConcurrentIOBufferAllocator(128, true)
                            : new UnpooledIOBufferAllocator();
                    return readHandler;
                };
                readHandlerSuppliers.put(eventloop, readHandlerSupplier);

                int port = toPort(thisAddress, k);
                tpcPorts.add(port);
                AsyncServerSocket serverSocket = eventloop.openTcpAsyncServerSocket();
                serverSocket.setReceiveBufferSize(socketConfig.receiveBufferSize);
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress(thisAddress.getInetAddress(), port));
                serverSocket.accept(socket -> {
                    if (socket instanceof NioAsyncSocket) {
                        NioAsyncSocket nioSocket = (NioAsyncSocket) socket;
                        nioSocket.setWriteThrough(writeThrough);
                        nioSocket.setRegularSchedule(regularSchedule);
                    }
                    socket.setReadHandler(readHandlerSuppliers.get(eventloop).get());
                    socket.setSendBufferSize(socketConfig.sendBufferSize);
                    socket.setReceiveBufferSize(socketConfig.receiveBufferSize);
                    socket.setTcpNoDelay(socketConfig.tcpNoDelay);
                    socket.setKeepAlive(true);
                    socket.activate(eventloop);
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        TcpServer server = (TcpServer) node.getServer();
        server.setTpcPorts(tpcPorts);
    }

    public int toPort(Address address, int socketId) {
        return (address.getPort() - 5701) * 100 + 11000 + socketId;
    }

    private void ensureActive() {
        if (shuttingdown) {
            throw new RuntimeException("Can't make invocation, frontend shutting down");
        }
    }

    public void shutdown() {
        logger.info("AltoRuntime shutdown");

        shuttingdown = true;

        if (tpcEngine != null) {
            tpcEngine.shutdown();
        }

        if (requestRegistry != null) {
            requestRegistry.shutdown();
        }

        if (responseHandler != null) {
            responseHandler.shutdown();
        }

        try {
            if (tpcEngine != null) {
                tpcEngine.awaitTermination(5, SECONDS);
            }
        } catch (InterruptedException e) {
            logger.warning("TpcEngine failed to terminate.");
            Thread.currentThread().interrupt();
        }

        logger.info("AltoRuntime terminated");

        long totalScheduled = 0;
        for (OpScheduler scheduler : schedulers) {
            totalScheduled += scheduler.getScheduled();
        }

        System.out.println("----------- distribution of processed operations -----------------------------");
        for (int k = 0; k < schedulers.size(); k++) {
            OpScheduler scheduler = schedulers.get(k);
            double percentage = (100d * scheduler.getScheduled()) / totalScheduled;
            System.out.println("OpScheduler[" + k + "] percentage:" + percentage + "%, total:" + scheduler.getScheduled());
        }
        System.out.println("----------- distribution of processed operations -----------------------------");
    }

    public RequestFuture invoke(IOBuffer request, AsyncSocket socket) {
        ensureActive();

        RequestFuture future = new RequestFuture(request);
        // we need to acquire the frame because storage will release it once written
        // and we need to keep the frame around for the response.
        request.acquire();
        Requests requests = requestRegistry.getRequestsOrCreate(socket.getRemoteAddress());
        long callId = requests.nextCallId();
        request.putLong(OFFSET_REQ_CALL_ID, callId);
        socket.writeAndFlush(request);
        return future;
    }

    TcpServerConnection getConnection(Address address) {
        if (connectionManager == null) {
            connectionManager = node.getServer().getConnectionManager(EndpointQualifier.MEMBER);
        }

        TcpServerConnection connection = (TcpServerConnection) connectionManager.get(address);
        if (connection == null) {
            connectionManager.getOrConnect(address);
            for (int k = 0; k < 60; k++) {
                try {
                    System.out.println("Waiting for connection: " + address);
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                connection = (TcpServerConnection) connectionManager.get(address);
                if (connection != null) {
                    break;
                }
            }

            if (connection == null) {
                throw new RuntimeException("Could not connect to : " + address);
            }
        }


        if (connection.sockets == null) {
            synchronized (connection) {
                if (connection.sockets == null) {
                    long start = System.currentTimeMillis();
                    List<Integer> remoteTpcPorts = connection.getRemoteTpcPorts();
                    System.out.println("alto runtime getting remote tpc ports:"+ remoteTpcPorts);

                    CompletableFuture[] futures = new CompletableFuture[remoteTpcPorts.size()];
                    for (int socketIndex = 0; socketIndex < remoteTpcPorts.size(); socketIndex++) {
                        SocketAddress eventloopAddress = new InetSocketAddress(address.getHost(), remoteTpcPorts.get(socketIndex));
//                        System.out.println("Connecting to: " + eventloopAddress + " " + socketIndex + "/" + socketCount);
                        futures[socketIndex] = connect(eventloopAddress, socketIndex);
                    }

                    AsyncSocket[] sockets = new AsyncSocket[remoteTpcPorts.size()];
                    for (int socketIndex = 0; socketIndex < futures.length; socketIndex++) {
                        CompletableFuture future = futures[socketIndex];
                        AsyncSocket socket = (AsyncSocket) future.join();
                        System.out.println("AsyncSocket " + address + " connected");
                        sockets[socketIndex] = socket;
                    }

                    connection.sockets = sockets;
                    System.out.println("Duration to make all connections:" + (System.currentTimeMillis() - start) + " ms");
                }
            }
        }

        return connection;
    }

    public CompletableFuture<Void> connect(SocketAddress address, int channelIndex) {
        int eventloopIndex = HashUtil.hashToIndex(channelIndex, tpcEngine.eventloopCount());
        Eventloop eventloop = tpcEngine.eventloop(eventloopIndex);

        AsyncSocket socket = eventloop.openTcpAsyncSocket();
        if (socket instanceof NioAsyncSocket) {
            NioAsyncSocket nioSocket = (NioAsyncSocket) socket;
            nioSocket.setWriteThrough(writeThrough);
            nioSocket.setRegularSchedule(regularSchedule);
        }
        socket.setReadHandler(readHandlerSuppliers.get(eventloop).get());
        socket.setSendBufferSize(socketConfig.sendBufferSize);
        socket.setReceiveBufferSize(socketConfig.receiveBufferSize);
        socket.setTcpNoDelay(socketConfig.tcpNoDelay);
        socket.activate(eventloop);

        return socket.connect(address);
    }
}
