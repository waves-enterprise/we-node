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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

/**
 * A {@link ChunkedInput} that fetches data from a file chunk by chunk using NIO {@link FileChannel}.
 * <p>
 * Based on <a href="https://github.com/netty/netty/blob/4.1/handler/src/main/java/io/netty/handler/stream/ChunkedNioFile.java">netty implementation</a>.
 * Modifications: each chunk includes a header with magic number.
 */
public class ChunkedNioFile implements ChunkedInput<ByteBuf> {

    private final FileChannel in;
    private final long startOffset;
    private final long endOffset;
    private final int chunkSize;
    private long offset;

    public ChunkedNioFile(File in) throws IOException {
        this(new RandomAccessFile(in, "r").getChannel());
    }

    public ChunkedNioFile(File in, int chunkSize) throws IOException {
        this(new RandomAccessFile(in, "r").getChannel(), chunkSize);
    }

    public ChunkedNioFile(FileChannel in) throws IOException {
        this(in, StreamHandlerBase.DefaultChunkSize());
    }

    public ChunkedNioFile(FileChannel in, int chunkSize) throws IOException {
        this(in, 0, in.size(), chunkSize);
    }

    public ChunkedNioFile(FileChannel in, long offset, long length, int chunkSize)
            throws IOException {
        ObjectUtil.checkNotNull(in, "in");
        ObjectUtil.checkPositiveOrZero(offset, "offset");
        ObjectUtil.checkPositiveOrZero(length, "length");
        ObjectUtil.checkPositive(chunkSize, "chunkSize");
        if (!in.isOpen()) {
            throw new ClosedChannelException();
        }
        this.in = in;
        this.chunkSize = chunkSize;
        this.offset = startOffset = offset;
        endOffset = offset + length;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public long currentOffset() {
        return offset;
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        return !(offset < endOffset && in.isOpen());
    }

    @Override
    public void close() throws Exception {
        in.close();
    }

    @Deprecated
    @Override
    public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
        return readChunk(ctx.alloc());
    }

    @Override
    public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        long offset = this.offset;
        if (offset >= endOffset) {
            return null;
        }

        int chunkSize = (int) Math.min(this.chunkSize, endOffset - offset);
        final int magicNumberSize = Integer.BYTES;
        ByteBuf buffer = allocator.buffer(chunkSize + magicNumberSize);
        boolean release = true;
        try {
            int readBytes = 0;
            for (; ; ) {
                buffer.writeInt(ChunkedWriteHandler.CHUNK_MAGIC);
                int localReadBytes = buffer.writeBytes(in, offset + readBytes, chunkSize - readBytes);
                if (localReadBytes < 0) {
                    break;
                }
                readBytes += localReadBytes;
                if (readBytes == chunkSize) {
                    break;
                }
            }
            this.offset += readBytes;
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
        return endOffset - startOffset;
    }

    @Override
    public long progress() {
        return offset - startOffset;
    }
}
