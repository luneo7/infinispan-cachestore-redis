package org.infinispan.persistence.redis.compression;

import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.PersistenceException;

import java.io.IOException;

public class NoCompressor implements Compressor {
    private final PersistenceMarshaller marshaller;

    public NoCompressor(PersistenceMarshaller marshaller) {
        this.marshaller = marshaller;
    }

    @Override
    public byte[] compress(Object entry) {
        try {
            return marshaller.objectToByteBuffer(entry);
        } catch (IOException | InterruptedException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> E uncompress(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try {
            return (E) marshaller.objectFromByteBuffer(bytes);
        } catch (IOException | ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }
}
