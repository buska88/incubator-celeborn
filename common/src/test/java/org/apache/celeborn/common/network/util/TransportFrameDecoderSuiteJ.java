/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.common.network.util;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.protocol.Message;
import org.apache.celeborn.common.network.protocol.OneWayMessage;

public class TransportFrameDecoderSuiteJ {

  private TransportFrameDecoder decoder;
  private ChannelHandlerContext ctx;
  private Channel channel;
  private ChannelPipeline pipeline;
  private final List<Object> decodedMessages = new ArrayList<>();

  @Before
  public void setUp() {
    decoder = new TransportFrameDecoder();
    ctx = mock(ChannelHandlerContext.class);
    channel = mock(Channel.class);
    pipeline = mock(ChannelPipeline.class);
    when(ctx.channel()).thenReturn(channel);
    when(channel.pipeline()).thenReturn(pipeline);
    decodedMessages.clear();
    when(ctx.fireChannelRead(any()))
        .thenAnswer(
            invocation -> {
              decodedMessages.add(invocation.getArgument(0));
              return ctx;
            });
  }

  private ByteBuf encodeMessage(Message message) throws IOException {
    ByteBuf buf = Unpooled.buffer();
    buf.writeInt(message.encodedLength());
    message.type().encode(buf);
    if (message.body() != null) {
      buf.writeInt((int) message.body().size());
    } else {
      buf.writeInt(0);
    }
    message.encode(buf);
    if (message.body() != null) {
      buf.writeBytes(message.body().nioByteBuffer());
    }
    return buf;
  }

  private OneWayMessage oneWayMessage(byte[] payload) {
    return new OneWayMessage(new NioManagedBuffer(java.nio.ByteBuffer.wrap(payload)));
  }

  @Test
  public void hasLikelyLargeIncompleteFrameIsFalseInitially() {
    assertFalse(decoder.hasLikelyLargeIncompleteFrame());
  }

  @Test
  public void hasLikelyLargeIncompleteFrameIsFalseForASmallStuckHalfFrame() throws IOException {
    // A small amount of leftover bytes is well within what a single channelRead can deliver, so
    // it isn't evidence of a large frame stuck across multiple reads.
    ByteBuf full = encodeMessage(oneWayMessage(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
    ByteBuf partial = full.retainedSlice(0, FrameDecoder.HEADER_SIZE + 2);
    decoder.channelRead(ctx, partial);

    assertFalse(decoder.hasLikelyLargeIncompleteFrame());
    full.release();
  }

  @Test
  public void hasLikelyLargeIncompleteFrameIsTrueOnceLeftoverExceedsSingleReadCap()
      throws IOException {
    // A single channelRead cannot deliver more than 64KB (Netty's AdaptiveRecvByteBufAllocator
    // default maximum), so leftover bytes beyond that can only have piled up over multiple reads
    // that each failed to complete the frame — i.e. a genuinely large frame stuck mid-transfer.
    int oversizedBodyLength = 65536 + 1024;
    byte[] payload = new byte[oversizedBodyLength];
    ByteBuf full = encodeMessage(oneWayMessage(payload));
    // Withhold the last byte so the frame never actually completes.
    ByteBuf partial = full.retainedSlice(0, full.readableBytes() - 1);
    decoder.channelRead(ctx, partial);

    assertTrue(decoder.hasLikelyLargeIncompleteFrame());
    assertTrue(decodedMessages.isEmpty());
    full.release();
  }

  @Test
  public void hasLikelyLargeIncompleteFrameIsFalseWhenLeftoverExactlyEqualsSingleReadCap()
      throws IOException {
    // The check is a strict ">", not ">=": leftover bytes exactly at the single-channelRead cap
    // (64KB) are still fully explainable by one read, so this boundary must not be flagged yet.
    int singleReadCapBytes = 65536;
    byte[] payload = new byte[singleReadCapBytes + 1024];
    ByteBuf full = encodeMessage(oneWayMessage(payload));
    // Header (9 bytes) + exactly singleReadCapBytes of body, leaving totalSize == 65536.
    ByteBuf partial = full.retainedSlice(0, FrameDecoder.HEADER_SIZE + singleReadCapBytes);
    decoder.channelRead(ctx, partial);

    assertFalse(decoder.hasLikelyLargeIncompleteFrame());
    assertTrue(decodedMessages.isEmpty());
    full.release();
  }

  @Test
  public void frameDrainFiresEventOnceButStillDispatchesFramesAlreadyBuffered() throws IOException {
    // Two complete frames arrive in a single channelRead call, simulating data that piles up
    // while frame-drain resumes a channel that has more than just the stuck half-frame available.
    // Since both frames are already fully in memory (no extra I/O needed to obtain them), both
    // should still be decoded and dispatched to the downstream handler right away; only the
    // drain-completed notification (which governs whether autoRead stays on) fires once.
    ByteBuf frame1 = encodeMessage(oneWayMessage(new byte[] {1, 2, 3}));
    ByteBuf frame2 = encodeMessage(oneWayMessage(new byte[] {4, 5, 6}));
    ByteBuf combined = Unpooled.wrappedBuffer(frame1, frame2);

    decoder.enableFrameDrain();
    decoder.channelRead(ctx, combined);

    // Both frames already sitting in memory should have been decoded and dispatched...
    assertEquals(2, decodedMessages.size());
    // ...while the drain-completed event fires exactly once, right after the first frame.
    verify(pipeline, times(1))
        .fireUserEventTriggered(TransportFrameDecoder.FrameDrainCompleted.INSTANCE);
  }

  @Test
  public void nonFrameDrainModeConsumesAllAvailableFramesWithoutFiringEvent() throws IOException {
    ByteBuf frame1 = encodeMessage(oneWayMessage(new byte[] {1, 2, 3}));
    ByteBuf frame2 = encodeMessage(oneWayMessage(new byte[] {4, 5, 6}));
    ByteBuf combined = Unpooled.wrappedBuffer(frame1, frame2);

    decoder.channelRead(ctx, combined);

    assertEquals(2, decodedMessages.size());
    verify(pipeline, times(0)).fireUserEventTriggered(any());
  }

  @Test
  public void frameDrainFlagIsResetAfterFirstFrameCompletes() throws IOException {
    ByteBuf frame1 = encodeMessage(oneWayMessage(new byte[] {1, 2, 3}));
    decoder.enableFrameDrain();
    decoder.channelRead(ctx, frame1);
    verify(pipeline, times(1))
        .fireUserEventTriggered(TransportFrameDecoder.FrameDrainCompleted.INSTANCE);

    // A subsequent frame, arriving after frame-drain mode has already been consumed, should not
    // fire the event again since frameDrain was reset to false.
    ByteBuf frame2 = encodeMessage(oneWayMessage(new byte[] {4, 5, 6}));
    decoder.channelRead(ctx, frame2);
    verify(pipeline, times(1))
        .fireUserEventTriggered(TransportFrameDecoder.FrameDrainCompleted.INSTANCE);
    assertEquals(2, decodedMessages.size());
  }
}
