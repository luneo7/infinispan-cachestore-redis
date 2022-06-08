package org.infinispan.persistence.redis.client;

import java.io.IOException;

public interface RedisConnection
{
    /**
     * Redis scan command
     */
    Iterable<byte[]> scan(String prefix);

    /**
     * Redis get command
     */
    byte[] get(byte[] key);

    /**
     * Redis set command
     */

    void set(byte[] key, byte[] value, long ttl);

    /**
     * Redis del command
     */
    boolean delete(byte[] key);

    /**
     * Redis exists command
     */
    boolean exists(byte[] key);

    /**
     * Redis dbsize command
     */
    long dbSize();

    /**
     * Redis flushdb command
     */
    void flushDb();

    /**
     * Release the connection, returning the connection to the pool
     */
    void release();
}
