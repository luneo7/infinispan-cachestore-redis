package org.infinispan.persistence.redis.compression;

import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration;

final public class Util {
    private Util() {
    }

    public static Compressor getCompressor(RedisStoreConfiguration configuration, PersistenceMarshaller persistenceMarshaller) {
        return getCompressor(configuration.compressor(), configuration, persistenceMarshaller);
    }

    public static Compressor getCompressor(RedisStoreConfiguration.Compressor compressor, RedisStoreConfiguration configuration, PersistenceMarshaller persistenceMarshaller) {
        switch (compressor) {
            case BROTLI:
                return new BrotliCompressor(persistenceMarshaller, configuration.compressionLevel());
            case GZIP:
                return new GzipCompressor(persistenceMarshaller, configuration.compressionLevel());
            case LZ4:
                return new Lz4Compressor(persistenceMarshaller);
            case LZF:
                return new LzfCompressor(persistenceMarshaller);
            case NONE:
                return new NoCompressor(persistenceMarshaller);
            case SNAPPY:
                return new SnappyCompressor(persistenceMarshaller, configuration.compressionBlockSize());
            case ZSTD:
                return new ZstdCompressor(persistenceMarshaller, configuration.compressionLevel());
            default:
                throw new IllegalStateException("Invalid compressor");
        }
    }
}
