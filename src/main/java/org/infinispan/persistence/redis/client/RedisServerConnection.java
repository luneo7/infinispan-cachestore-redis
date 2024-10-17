package org.infinispan.persistence.redis.client;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

final public class RedisServerConnection implements RedisConnection {
    private final Jedis client;

    RedisServerConnection(Jedis client) {
        this.client = client;
    }

    @Override
    public void release() {
        client.close();
    }

    @Override
    public Iterable<byte[]> scan(String prefix) {
        return new RedisServerScanIterable(client, prefix);
    }

    @Override
    public byte[] get(byte[] key) {
        return client.get(key);
    }

    @Override
    public void set(byte[] key, byte[] value, long ttl) {
        if (ttl > 0) {
            client.set(key, value, SetParams.setParams().ex(ttl));
        } else {
            client.set(key, value);
        }
    }

    @Override
    public boolean delete(byte[] key) {
        return client.del(key) > 0;
    }

    @Override
    public boolean exists(byte[] key) {
        return client.exists(key);
    }

    @Override
    public long dbSize() {
        return client.dbSize();
    }

    @Override
    public void flushDb() {
        client.flushDB();
    }
}
