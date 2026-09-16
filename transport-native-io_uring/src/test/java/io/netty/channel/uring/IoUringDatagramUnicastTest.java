/*
 * Copyright 2024 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.uring;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketProtocolFamily;
import io.netty.channel.unix.SegmentedDatagramPacket;
import io.netty.testsuite.transport.TestsuitePermutation;
import io.netty.testsuite.transport.socket.DatagramUnicastInetTest;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class IoUringDatagramUnicastTest extends DatagramUnicastInetTest {

    @BeforeAll
    public static void loadJNI() {
        assumeTrue(IoUring.isAvailable());
    }

    @Override
    protected List<TestsuitePermutation.BootstrapComboFactory<Bootstrap, Bootstrap>> newFactories() {
        return IoUringSocketTestPermutation.INSTANCE.datagram(SocketProtocolFamily.INET);
    }

    @Test
    @Timeout(8)
    public void testRecvMsgDontBlock(TestInfo testInfo) throws Throwable {
        run(testInfo, this::testRecvMsgDontBlock);
    }

    public void testRecvMsgDontBlock(Bootstrap sb, Bootstrap cb) throws Throwable {
        Channel sc = null;
        Channel cc = null;

        try {
            cb.handler(new SimpleChannelInboundHandler<Object>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                    // NOOP.
                }
            });
            cc = cb.bind(newSocketAddress()).sync().channel();

            CountDownLatch readLatch = new CountDownLatch(1);
            CountDownLatch readCompleteLatch = new CountDownLatch(1);
            sc = sb.handler(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                    readLatch.countDown();
                    ReferenceCountUtil.release(msg);
                }

                @Override
                public void channelReadComplete(ChannelHandlerContext ctx) {
                    readCompleteLatch.countDown();
                }
            }).option(ChannelOption.RECVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(2048))
                    .bind(newSocketAddress()).sync().channel();
            InetSocketAddress addr = convertAnyAddress((InetSocketAddress) sc.localAddress());
            cc.writeAndFlush(new DatagramPacket(cc.alloc().buffer().writeZero(512),  addr)).sync();

            readLatch.await();
            readCompleteLatch.await();
        } finally {
            if (cc != null) {
                cc.close().sync();
            }
            if (sc != null) {
                sc.close().sync();
            }
        }
    }

    private static final int NUM_SEGMENTS = 16;
    private static final int SEGMENT_SIZE = 512;
    private static final int BATCH_SIZE = NUM_SEGMENTS * SEGMENT_SIZE;

    @Test
    public void testSendSegmentedDatagramPacket(TestInfo testInfo) throws Throwable {
        run(testInfo, (sb, cb) -> testSegmentedDatagramPacket(sb, cb, false, 0, 0, NUM_SEGMENTS));
    }

    @Test
    public void testSendAndReceiveSegmentedDatagramPacket(TestInfo testInfo) throws Throwable {
        // Room for the whole batch, as the kernel truncates a batch that doesn't fit.
        run(testInfo, (sb, cb) -> testSegmentedDatagramPacket(sb, cb, true, 0, BATCH_SIZE, NUM_SEGMENTS));
    }

    @Test
    public void testSendAndReceiveSegmentedDatagramPacketBatched(TestInfo testInfo) throws Throwable {
        // Room for two batches, so the read is done with two linked recvmsg.
        run(testInfo, (sb, cb) -> testSegmentedDatagramPacket(
                sb, cb, true, BATCH_SIZE, 2 * BATCH_SIZE, NUM_SEGMENTS));
    }

    private void testSegmentedDatagramPacket(Bootstrap sb, Bootstrap cb, boolean gro, int maxDatagramPayloadSize,
                                             int recvBufferSize, int expectedSegments) throws Throwable {
        if (!isIoUring(cb.config().group()) || gro && !isIoUring(sb.config().group())) {
            return;
        }
        Channel sc = null;
        Channel cc = null;

        try {
            cb.handler(new SimpleChannelInboundHandler<Object>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                }
            });
            cc = cb.bind(newSocketAddress()).sync().channel();

            CountDownLatch latch = new CountDownLatch(expectedSegments);
            CountDownLatch readCompleteLatch = new CountDownLatch(1);
            AtomicReference<Throwable> errorRef = new AtomicReference<>();
            if (gro) {
                sb.option(IoUringChannelOption.UDP_GRO, true);
                if (maxDatagramPayloadSize > 0) {
                    sb.option(IoUringChannelOption.MAX_DATAGRAM_PAYLOAD_SIZE, maxDatagramPayloadSize);
                }
                sb.option(ChannelOption.RECVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(recvBufferSize));
            }
            sc = sb.handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                    int size = msg.content().readableBytes();
                    if (size == SEGMENT_SIZE) {
                        latch.countDown();
                    } else {
                        errorRef.compareAndSet(null, new AssertionError("Unexpected datagram size: " + size));
                    }
                }

                @Override
                public void channelReadComplete(ChannelHandlerContext ctx) {
                    if (latch.getCount() == 0) {
                        readCompleteLatch.countDown();
                    }
                }
            }).bind(newSocketAddress()).sync().channel();

            if (sc instanceof IoUringDatagramChannel) {
                assertEquals(gro, sc.config().getOption(IoUringChannelOption.UDP_GRO));
            }
            InetSocketAddress addr = convertAnyAddress((InetSocketAddress) sc.localAddress());
            cc.writeAndFlush(new SegmentedDatagramPacket(
                    cc.alloc().directBuffer(BATCH_SIZE).writeZero(BATCH_SIZE), SEGMENT_SIZE, addr)).sync();

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertTrue(readCompleteLatch.await(10, TimeUnit.SECONDS));
            Throwable error = errorRef.get();
            if (error != null) {
                throw error;
            }
        } finally {
            if (cc != null) {
                cc.close().sync();
            }
            if (sc != null) {
                sc.close().sync();
            }
        }
    }

    private static boolean isIoUring(EventLoopGroup group) {
        return group instanceof IoEventLoopGroup && ((IoEventLoopGroup) group).isIoType(IoUringIoHandler.class);
    }

    @Override
    protected boolean supportDisconnect() {
        return false;
    }
}
