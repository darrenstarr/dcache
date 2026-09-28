package org.dcache.pool.repository;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;

public class FileRepositoryChannel implements RepositoryChannel {

    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute<?>[0];
    private static final int BUFFER_SIZE = 16777216;

    private final FileChannel _fileChannel;
    private final long _fileSize;
    private ByteBuffer _writeBuffer;

    public FileRepositoryChannel(Path path, Set<? extends OpenOption> openOptions)
          throws FileNotFoundException, IOException {
        _fileChannel = FileChannel.open(path, openOptions, NO_ATTRIBUTES);
        _fileSize = !openOptions.contains(StandardOpenOption.WRITE) ? _fileChannel.size() : -1;
        if (openOptions.contains(StandardOpenOption.WRITE)) {
            _writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
            _writeBuffer.clear();
        }
    }

    @Override
    public long position() throws IOException {
        return _fileChannel.position() + (_writeBuffer != null ? _writeBuffer.position() : 0);
    }

    @Override
    public RepositoryChannel position(long p) throws IOException {
        flushWriteBuffer();
        _fileChannel.position(p);
        return this;
    }

    @Override
    public long size() throws IOException {
        return _fileSize == -1 ? _fileChannel.size() : _fileSize;
    }

    @Override
    public void sync() throws SyncFailedException, IOException {
        flushWriteBuffer();
        _fileChannel.force(false);
    }

    @Override
    public RepositoryChannel truncate(long size) throws IOException {
        flushWriteBuffer();
        _fileChannel.truncate(size);
        return this;
    }

    @Override
    public void close() throws IOException {
        flushWriteBuffer();
        _fileChannel.close();
    }

    @Override
    public boolean isOpen() {
        return _fileChannel.isOpen();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return _fileChannel.read(dst);
    }

    @Override
    public int read(ByteBuffer b, long pos) throws IOException {
        return _fileChannel.read(b, pos);
    }

    @Override
    public long read(ByteBuffer[] dsts, int off, int len) throws IOException {
        return _fileChannel.read(dsts, off, len);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return _fileChannel.read(dsts);
    }

    private void flushWriteBuffer() throws IOException {
        if (_writeBuffer != null && _writeBuffer.position() > 0) {
            _writeBuffer.flip();
            while (_writeBuffer.hasRemaining()) {
                _fileChannel.write(_writeBuffer);
            }
            _writeBuffer.clear();
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (_writeBuffer == null) {
            return _fileChannel.write(src);
        }
        int totalWritten = src.remaining();
        while (src.hasRemaining()) {
            int space = _writeBuffer.remaining();
            if (space == 0) {
                flushWriteBuffer();
                space = _writeBuffer.remaining();
            }
            int chunk = Math.min(space, src.remaining());
            int oldLimit = src.limit();
            src.limit(src.position() + chunk);
            _writeBuffer.put(src);
            src.limit(oldLimit);
        }
        return totalWritten;
    }

    @Override
    public int write(ByteBuffer b, long pos) throws IOException {
        flushWriteBuffer();
        return _fileChannel.write(b, pos);
    }

    @Override
    public long write(ByteBuffer[] srcs, int off, int len) throws IOException {
        flushWriteBuffer();
        return _fileChannel.write(srcs, off, len);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        flushWriteBuffer();
        return _fileChannel.write(srcs);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        long pos = position;
        long remaining = count;
        while (remaining > 0) {
            int chunk = (int) Math.min(remaining, BUFFER_SIZE);
            buffer.clear();
            buffer.limit(chunk);
            if (_fileChannel.read(buffer, pos) < 0) {
                break;
            }
            buffer.flip();
            int nw = target.write(buffer);
            if (nw <= 0) {
                break;
            }
            pos += nw;
            remaining -= nw;
        }
        return count - remaining;
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long pos, long count) throws IOException {
        return _fileChannel.transferFrom(src, pos, count);
    }
}
