package org.infinispan.persistence.redis.compression;

import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.PersistenceException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class LzfCompressor implements Compressor {
    private final PersistenceMarshaller marshaller;

    public LzfCompressor(PersistenceMarshaller marshaller) {
        this.marshaller = marshaller;
    }

    @Override
    public byte[] compress(Object entry) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (LZFOutputStream lzfStream = new LZFOutputStream(out).setFinishBlockOnFlush(true)) {
            marshaller.writeObject(entry, lzfStream);
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
             LZFInputStream lzfStream = new LZFInputStream(byteStream)) {
            return (E) marshaller.readObject(lzfStream);
        } catch (IOException | ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }
}
