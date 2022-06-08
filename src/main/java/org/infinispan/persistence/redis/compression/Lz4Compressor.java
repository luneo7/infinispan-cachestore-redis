package org.infinispan.persistence.redis.compression;

import net.jpountz.lz4.LZ4CompressorWithLength;
import net.jpountz.lz4.LZ4DecompressorWithLength;
import net.jpountz.lz4.LZ4Factory;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.PersistenceException;

import java.io.IOException;

public class Lz4Compressor implements Compressor {
    private final LZ4Factory lz4Factory;
    private final PersistenceMarshaller marshaller;

    public Lz4Compressor(PersistenceMarshaller marshaller) {
        lz4Factory = LZ4Factory.fastestInstance();
        this.marshaller = marshaller;
    }

    @Override
    public byte[] compress(Object entry) {
        try {
            LZ4CompressorWithLength compressor = new LZ4CompressorWithLength(lz4Factory.fastCompressor());
            return compressor.compress(marshaller.objectToByteBuffer(entry));
        } catch (IOException | InterruptedException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> E uncompress(byte[] bytes) {
        try {
            LZ4DecompressorWithLength decompressor = new LZ4DecompressorWithLength(lz4Factory.safeDecompressor());
            return (E) marshaller.objectFromByteBuffer(decompressor.decompress(bytes));
        } catch (IOException | ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }
}
