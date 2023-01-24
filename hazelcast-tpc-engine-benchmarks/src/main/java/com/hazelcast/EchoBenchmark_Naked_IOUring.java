package com.hazelcast;

import com.hazelcast.internal.tpc.iouring.AcceptMemory;
import com.hazelcast.internal.tpc.iouring.CompletionQueue;
import com.hazelcast.internal.tpc.iouring.IOCompletionHandler;
import com.hazelcast.internal.tpc.iouring.IOUring;
import com.hazelcast.internal.tpc.iouring.Socket;
import com.hazelcast.internal.tpc.iouring.SubmissionQueue;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.ThreadAffinityHelper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_ACCEPT;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_WRITE;
import static com.hazelcast.internal.tpc.iouring.Linux.SOCK_CLOEXEC;
import static com.hazelcast.internal.tpc.iouring.Linux.SOCK_NONBLOCK;
import static com.hazelcast.internal.tpc.iouring.Linux.strerror;
import static com.hazelcast.internal.tpc.iouring.Socket.AF_INET;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;

/**
 * Tests the lower level IOUring API. So without all the TPC functionality on top.
 * This helps to give us a base line of performance of the lower level API and how
 * much performance is lost in the TPC layer on top.
 */
public class EchoBenchmark_Naked_IOUring {

    public static final long iterations = 40_000_000;
    public static final String cpuAffinityClient = "1";
    public static final String cpuAffinityServer = "4";

    private final static InetSocketAddress address = new InetSocketAddress("127.0.0.1", 5000);

    public static void main(String[] args) throws IOException, InterruptedException {
        Thread serverThread = new ServerThread();
        serverThread.start();

        Thread.sleep(1000);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread clientThread = new ClientThread(countDownLatch);

        long start = System.currentTimeMillis();
        clientThread.start();

        //  serverThread.join();
        //  clientThread.join();

        countDownLatch.await();
        long duration = System.currentTimeMillis() - start;
        System.out.println("Duration " + duration + " ms");
        System.out.println("Throughput:" + (iterations * 1000 / duration) + " ops");

        System.exit(0);
    }

    private static class ClientThread extends Thread {
        private final static int USERDATA_OP_READ = 1;
        private final static int USERDATA_OP_WRITE = 2;

        final IOUring uring = new IOUring(4096, 0);
        final SubmissionQueue sq = uring.getSubmissionQueue();
        final CompletionQueue cq = uring.getCompletionQueue();
        private final CountDownLatch countdownLatch;
        private ByteBuffer receiveBuffer;
        private long receiveBufferAddress;
        private ByteBuffer sendBuffer;
        private long sendBufferAddress;
        final Socket socket = Socket.openTcpIpv4Socket();
        final CompletionHandler handler = new CompletionHandler();

        public ClientThread(CountDownLatch countDownLatch) {
            this.countdownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                ThreadAffinity threadAffinity = cpuAffinityClient == null ? null : new ThreadAffinity(cpuAffinityClient);
                if (threadAffinity != null) {
                    System.out.println("Setting ClientThread affinity " + cpuAffinityClient);
                    ThreadAffinityHelper.setAffinity(threadAffinity.nextAllowedCpus());
                }

                socket.setTcpNoDelay(true);
                socket.setBlocking();
                socket.connect(address);
                System.out.println("Connected");

                this.receiveBuffer = ByteBuffer.allocateDirect(64 * 1024);
                this.receiveBufferAddress = addressOf(receiveBuffer);
                this.sendBuffer = ByteBuffer.allocateDirect(64 * 1024);
                this.sendBufferAddress = addressOf(sendBuffer);

                sendBuffer.putLong(iterations);
                sendBuffer.flip();

                //System.out.println("sendBuffer.remaining");

                sq.offer(
                        IORING_OP_WRITE,                // op
                        0,                              // flags
                        0,                              // rw-flags
                        socket.fd(),                    // fd
                        sendBufferAddress,              // buffer address
                        sendBuffer.remaining(),         // number of bytes to write.
                        0,                              // offset
                        USERDATA_OP_WRITE               // userdata
                );

                sq.offer(
                        IORING_OP_READ,                     // op
                        0,                                  // flags
                        0,                                  // rw-flags
                        socket.fd(),                        // fd
                        receiveBufferAddress,               // buffer address
                        receiveBuffer.remaining(),          // length
                        0,                                  // offset
                        USERDATA_OP_READ                    // userdata
                );

                for (; ; ) {
                    sq.submitAndWait();
                    cq.process(handler);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        private class CompletionHandler implements IOCompletionHandler {

            @Override
            public void handle(int res, int flags, long userdata_id) {
                if (res < 0) {
                    throw new UncheckedIOException(new IOException(strerror(-res)));
                }

                if (userdata_id == USERDATA_OP_READ) {
                    //System.out.println("Client read " + res + " bytes");

                    receiveBuffer.position(receiveBuffer.position() + res);
                    receiveBuffer.flip();
                    long round = receiveBuffer.getLong();
                    //System.out.println("Client round:" + round);
                    receiveBuffer.clear();

                    if (round == 0) {
                        countdownLatch.countDown();
                        System.out.println("Done");
                        return;
                    }
                    sendBuffer.putLong(round - 1);
                    sendBuffer.flip();

                    sq.offer(
                            IORING_OP_WRITE,                // op
                            0,                              // flags
                            0,                              // rw-flags
                            socket.fd(),                    // fd
                            sendBufferAddress,              // buffer address
                            sendBuffer.remaining(),         // number of bytes to write.
                            0,                              // offset
                            USERDATA_OP_WRITE               // userdata
                    );

                    sq.offer(
                            IORING_OP_READ,                     // op
                            0,                                  // flags
                            0,                                  // rw-flags
                            socket.fd(),                        // fd
                            receiveBufferAddress,               // buffer address
                            receiveBuffer.remaining(),          // length
                            0,                                  // offset
                            USERDATA_OP_READ                    // userdata
                    );
                } else if (userdata_id == USERDATA_OP_WRITE) {
                    //System.out.println("Client wrote " + res + " bytes");
                    //sendBuffer.position(sendBuffer.position() + res);
                    sendBuffer.clear();
                } else {
                    System.out.println("Client unknown userdata_id");
                }
            }
        }
    }

    private static class ServerThread extends Thread {
        private int userdata_generator = 0;
        private Userdata[] userdataArray = new Userdata[1024];
        final IOUring uring = new IOUring(4096, 0);
        final SubmissionQueue sq = uring.getSubmissionQueue();
        final CompletionQueue cq = uring.getCompletionQueue();
        final Socket serverSocket = Socket.openTcpIpv4Socket();
        final AcceptMemory acceptMemory = new AcceptMemory();


        @Override
        public void run() {
            try {
                ThreadAffinity threadAffinity = cpuAffinityServer == null ? null : new ThreadAffinity(cpuAffinityServer);
                if (threadAffinity != null) {
                    System.out.println("Setting ServerThread affinity " + cpuAffinityServer);
                    ThreadAffinityHelper.setAffinity(threadAffinity.nextAllowedCpus());
                }

                serverSocket.setReusePort(true);
                serverSocket.bind(address);
                serverSocket.listen(10);
                System.out.println("server started on:" + address);

                Userdata userdata_OP_ACCEPT = new Userdata();
                userdata_OP_ACCEPT.type = IORING_OP_ACCEPT;
                userdata_OP_ACCEPT.id = userdata_generator;
                userdataArray[userdata_OP_ACCEPT.id] = userdata_OP_ACCEPT;
                userdata_generator++;

                sq.offer(
                        IORING_OP_ACCEPT,
                        0,
                        SOCK_NONBLOCK | SOCK_CLOEXEC,
                        serverSocket.fd(),
                        acceptMemory.memoryAddress,
                        0,
                        acceptMemory.lengthMemoryAddress,
                        userdata_OP_ACCEPT.id
                );

                CompletionHandler handler = new CompletionHandler();
                for (; ; ) {
                    sq.submitAndWait();
                    cq.process(handler);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        private class CompletionHandler implements IOCompletionHandler {

            @Override
            public void handle(int res, int flags, long userdata_id) {
                try {
                    if (res < 0) {
                        throw new UncheckedIOException(new IOException(strerror(-res)));
                    }
                    //System.out.println("server handle " + res + " userdata " + userdata_id);
                    Userdata userdata = userdataArray[(int) userdata_id];

                    if (userdata.type == IORING_OP_ACCEPT) {
                        Socket clientSocket = new Socket(res, AF_INET);
                        clientSocket.setTcpNoDelay(true);

                        sq.offer(
                                IORING_OP_ACCEPT,
                                0,
                                SOCK_NONBLOCK | SOCK_CLOEXEC,
                                serverSocket.fd(),
                                acceptMemory.memoryAddress,
                                0,
                                acceptMemory.lengthMemoryAddress,
                                userdata.id
                        );
                        //System.out.println("Connection established " + clientSocket.getLocalAddress() + "->" + clientSocket.getRemoteAddress());

                        Userdata userdata_OP_READ = new Userdata();
                        userdata_OP_READ.type = IORING_OP_READ;
                        userdata_OP_READ.id = userdata_generator;
                        userdata_generator++;
                        userdata_OP_READ.buffer = ByteBuffer.allocateDirect(64 * 1024);
                        userdata_OP_READ.bufferAddress = addressOf(userdata_OP_READ.buffer);
                        userdata_OP_READ.socket = clientSocket;
                        userdataArray[userdata_OP_READ.id] = userdata_OP_READ;

                        Userdata userdata_OP_WRITE = new Userdata();
                        userdata_OP_WRITE.type = IORING_OP_WRITE;
                        userdata_OP_WRITE.id = userdata_generator;
                        userdata_generator++;
                        userdata_OP_WRITE.buffer = ByteBuffer.allocateDirect(64 * 1024);
                        userdata_OP_WRITE.bufferAddress = addressOf(userdata_OP_WRITE.buffer);
                        userdata_OP_WRITE.socket = clientSocket;
                        userdataArray[userdata_OP_WRITE.id] = userdata_OP_WRITE;


                        sq.offer(
                                IORING_OP_READ,                         // op
                                0,                                      // flags
                                0,                                      // rw-flags
                                clientSocket.fd(),                      // fd
                                userdata_OP_READ.bufferAddress,         // buffer address
                                userdata_OP_READ.buffer.remaining(),    // length
                                0,                                      // offset
                                userdata_OP_READ.id                     // userdata
                        );

                    } else if (userdata.type == IORING_OP_READ) {
                        //System.out.println("Server Read " + res + " bytes");
                        Userdata userdata_OP_READ = userdata;

                        userdata_OP_READ.buffer.position(userdata.buffer.position() + res);
                        userdata_OP_READ.buffer.flip();
                        long round = userdata_OP_READ.buffer.getLong();
                        //System.out.println("Server round:" + round);
                        userdata_OP_READ.buffer.clear();

                        Userdata userdata_OP_WRITE = userdataArray[userdata.id + 1];
                        userdata_OP_WRITE.buffer.putLong(round);
                        userdata_OP_WRITE.buffer.flip();

                        sq.offer(
                                IORING_OP_READ,                         // op
                                0,                                      // flags
                                0,                                      // rw-flags
                                userdata.socket.fd(),                      // fd
                                userdata.bufferAddress,         // buffer address
                                userdata.buffer.remaining(),    // length
                                0,                                      // offset
                                userdata.id                     // userdata
                        );

                        sq.offer(
                                IORING_OP_WRITE,                            // op
                                0,                                          // flags
                                0,                                          // rw-flags
                                userdata_OP_WRITE.socket.fd(),              // fd
                                userdata_OP_WRITE.bufferAddress,            // buffer address
                                userdata_OP_WRITE.buffer.remaining(),       // number of bytes to write.
                                0,                                          // offset
                                userdata_OP_WRITE.id                        // userdata
                        );
                        //SocketData asyncSocket = userdataArray[(int) userdata];

                    } else if (userdata.type == IORING_OP_WRITE) {
                        //System.out.println("Server wrote " + res + " bytes");
                        userdata.buffer.clear();
                    } else {
                        System.out.println("Server unknown userdata_id");

                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private static class Userdata {
        byte type;
        int id;
        Socket socket;
        ByteBuffer buffer;
        long bufferAddress;

    }

}
