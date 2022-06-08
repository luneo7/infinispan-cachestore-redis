package org.infinispan.persistence.redis.compression;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.PersistenceException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ZstdCompressor implements Compressor {
    private final PersistenceMarshaller marshaller;
    private final int level;

    public ZstdCompressor(PersistenceMarshaller marshaller, int level) {
        this.marshaller = marshaller;
        this.level = level;
    }

    @Override
    public byte[] compress(Object entry) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ZstdOutputStreamNoFinalizer zstdOutput = new ZstdOutputStreamNoFinalizer(out, RecyclingBufferPool.INSTANCE, level)) {
            marshaller.writeObject(entry, zstdOutput);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
        return out.toByteArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> E uncompress(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
             ZstdInputStreamNoFinalizer zstdInputStream = new ZstdInputStreamNoFinalizer(byteStream)) {
            return (E) marshaller.readObject(zstdInputStream);
        } catch (IOException | ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }
}
