package org.infinispan.persistence.redis.client;

import redis.clients.jedis.JedisCluster;

import java.util.Iterator;

final public class RedisClusterNodeIterable implements Iterable<byte[]> {
    private final JedisCluster client;
    private final String prefix;

    public RedisClusterNodeIterable(JedisCluster client, String prefix) {
        this.client = client;
        this.prefix = prefix;
    }

    @Override
    public Iterator<byte[]> iterator() {
        return new RedisClusterNodeIterator(client, prefix);
    }
}
