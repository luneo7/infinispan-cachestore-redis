package org.infinispan.persistence.redis.compression;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.BrotliInputStream;
import com.aayushatharva.brotli4j.encoder.BrotliOutputStream;
import com.aayushatharva.brotli4j.encoder.Encoder;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.PersistenceException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BrotliCompressor implements Compressor {
    private final PersistenceMarshaller marshaller;
    private final Encoder.Parameters encoderParameters;

    public BrotliCompressor(PersistenceMarshaller marshaller, int level) {
        Brotli4jLoader.ensureAvailability();
        this.marshaller = marshaller;
        encoderParameters = new Encoder.Parameters().setQuality(level).setMode(Encoder.Mode.GENERIC);
    }

    @Override
    public byte[] compress(Object entry) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BrotliOutputStream brotliStream = new BrotliOutputStream(out, encoderParameters)) {
            marshaller.writeObject(entry, brotliStream);
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
             BrotliInputStream brotliStream = new BrotliInputStream(byteStream)) {
            return (E) marshaller.readObject(brotliStream);
        } catch (IOException | ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }
}
