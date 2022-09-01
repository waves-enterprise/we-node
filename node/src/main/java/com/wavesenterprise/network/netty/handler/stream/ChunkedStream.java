/*
 * Copyright 2012 The Netty Project
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
package com.wavesenterprise.network.netty.handler.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.internal.ObjectUtil;

import java.io.InputStream;
import java.io.PushbackInputStream;

/**
 * A {@link ChunkedInput} that fetches data from an {@link InputStream} chunk by chunk.
 * <p>
 * Based on <a href="https://github.com/netty/netty/blob/4.1/handler/src/main/java/io/netty/handler/stream/ChunkedStream.java">netty implementation</a>.
 * Modifications: each chunk includes a header with magic number.
 */
public class ChunkedStream implements ChunkedInput<ByteBuf> {

    private final PushbackInputStream in;
    private final int chunkSize;
    private long offset;
    private boolean closed;

    public ChunkedStream(InputStream in) {
        this(in, StreamHandlerBase.DefaultChunkSize());
    }

    public ChunkedStream(InputStream in, int chunkSize) {
        ObjectUtil.checkNotNull(in, "in");
        ObjectUtil.checkPositive(chunkSize, "chunkSize");

        if (in instanceof PushbackInputStream) {
            this.in = (PushbackInputStream) in;
        } else {
            this.in = new PushbackInputStream(in);
        }
        this.chunkSize = chunkSize;
    }

    public long transferredBytes() {
        return offset;
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        if (closed) {
            return true;
        }
        if (in.available() > 0) {
            return false;
        }

        int b = in.read();
        if (b < 0) {
            return true;
        } else {
            in.unread(b);
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        in.close();
    }

    @Deprecated
    @Override
    public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
        return readChunk(ctx.alloc());
    }

    @Override
    public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        if (isEndOfInput()) {
            return null;
        }

        final int availableBytes = in.available();
        final int chunkSize;
        if (availableBytes <= 0) {
            chunkSize = this.chunkSize;
        } else {
            chunkSize = Math.min(this.chunkSize, in.available());
        }

        boolean release = true;
        final int magicNumberSize = Integer.BYTES;
        ByteBuf buffer = allocator.buffer(chunkSize + magicNumberSize);

        try {
            buffer.writeInt(ChunkedWriteHandler.CHUNK_MAGIC);
            offset += buffer.writeBytes(in, chunkSize);
            release = false;
            return buffer;
        } finally {
            if (release) {
                buffer.release();
            }
        }
    }

    @Override
    public long length() {
        return -1;
    }

    @Override
    public long progress() {
        return offset;
    }
}
