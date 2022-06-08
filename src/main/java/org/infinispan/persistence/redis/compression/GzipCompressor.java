package org.infinispan.persistence.redis.compression;

import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.PersistenceException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCompressor implements Compressor {
    private final PersistenceMarshaller marshaller;
    private final int level;

    public GzipCompressor(PersistenceMarshaller marshaller, int level) {
        this.marshaller = marshaller;
        this.level = level;
    }

    @Override
    public byte[] compress(Object entry) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStreamEx gzipStream = new GZIPOutputStreamEx(out, level)) {
            marshaller.writeObject(entry, gzipStream);
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
             GZIPInputStream gzipStream = new GZIPInputStream(byteStream)) {
            return (E) marshaller.readObject(gzipStream);
        } catch (IOException | ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }

    public static class GZIPOutputStreamEx extends GZIPOutputStream {
        public GZIPOutputStreamEx(OutputStream out, int level) throws IOException {
            super(out);
            def.setLevel(level);
        }
    }
}
