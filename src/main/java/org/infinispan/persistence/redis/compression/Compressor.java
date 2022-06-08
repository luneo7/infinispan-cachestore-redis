package org.infinispan.persistence.redis.compression;

public interface Compressor {
    byte[] compress(Object entry);

    <E> E uncompress(byte[] bytes);
}
