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
        return switch (compressor) {
            case BROTLI -> new BrotliCompressor(persistenceMarshaller, configuration.compressionLevel());
            case GZIP -> new GzipCompressor(persistenceMarshaller, configuration.compressionLevel());
            case LZ4 -> new Lz4Compressor(persistenceMarshaller);
            case LZF -> new LzfCompressor(persistenceMarshaller);
            case NONE -> new NoCompressor(persistenceMarshaller);
            case SNAPPY -> new SnappyCompressor(persistenceMarshaller, configuration.compressionBlockSize());
            case ZSTD -> new ZstdCompressor(persistenceMarshaller, configuration.compressionLevel());
        };
    }
}
