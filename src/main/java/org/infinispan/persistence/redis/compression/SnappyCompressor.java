package org.infinispan.persistence.redis.compression;

import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.PersistenceException;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SnappyCompressor implements Compressor {
    private final PersistenceMarshaller marshaller;
    private final int blockSize;

    public SnappyCompressor(PersistenceMarshaller marshaller, int blockSize) {
        this.marshaller = marshaller;
        this.blockSize = blockSize;
    }

    @Override
    public byte[] compress(Object entry) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (SnappyOutputStream snappyStream = new SnappyOutputStream(out, blockSize)) {
            marshaller.writeObject(entry, snappyStream);
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
             SnappyInputStream brotliStream = new SnappyInputStream(byteStream)) {
            return (E) marshaller.readObject(brotliStream);
        } catch (IOException | ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }
}
