package org.infinispan.persistence.redis.client;

import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.params.SetParams;

import java.util.Map;

final public class RedisClusterConnection implements RedisConnection {
    private final JedisCluster cluster;

    RedisClusterConnection(JedisCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void release() {
        // Nothing to do. Connection pools are managed internally by the cluster client.
    }

    @Override
    public Iterable<byte[]> scan(String prefix) {
        return new RedisClusterNodeIterable(cluster, prefix);
    }

    @Override
    public byte[] get(byte[] key) {
        return cluster.get(key);
    }

    @Override
    public void set(byte[] key, byte[] value, long ttl) {
        if (ttl > 0) {
            cluster.set(key, value, SetParams.setParams().ex(ttl));
        } else {
            cluster.set(key, value);
        }
    }

    @Override
    public boolean delete(byte[] key) {
        return cluster.del(key) > 0;
    }

    @Override
    public boolean exists(byte[] key) {
        return cluster.exists(key);
    }

    @Override
    public long dbSize() {
        long totalSize = 0;
        Map<String, ConnectionPool> clusterNodes = cluster.getClusterNodes();
        for (ConnectionPool  connectionPool : clusterNodes.values()) {
            try (Jedis client = new Jedis(connectionPool.getResource())) {
                totalSize += client.dbSize();
            }
        }

        return totalSize;
    }

    @Override
    public void flushDb() {
        Map<String, ConnectionPool> clusterNodes = cluster.getClusterNodes();
        for (ConnectionPool connectionPool : clusterNodes.values()) {
            try (Jedis client = new Jedis(connectionPool.getResource())) {
                client.flushDB();
            }
        }
    }
}
