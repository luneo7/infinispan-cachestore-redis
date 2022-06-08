package org.infinispan.persistence.redis.client;

import redis.clients.jedis.Jedis;

import java.util.Iterator;

final public class RedisServerScanIterable implements Iterable<byte[]>
{
    private final Jedis client;
    private final String prefix;

    public RedisServerScanIterable(Jedis client, String prefix)
    {
        this.client = client;
        this.prefix = prefix;
    }

    @Override
    public Iterator<byte[]> iterator()
    {
        return new RedisServerKeyIterator(client, prefix);
    }
}
