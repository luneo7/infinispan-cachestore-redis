package org.infinispan.persistence.redis.client;

import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.Iterator;
import java.util.Map;

final public class RedisClusterNodeIterator implements Iterator<byte[]> {
    private final Map<String, ConnectionPool> clusterNodes;
    private final Iterator<String> clusterNodeIt;
    private final String prefix;
    private RedisServerKeyIterator keyIterator = null;

    public RedisClusterNodeIterator(JedisCluster cluster, String prefix) {
        clusterNodes = cluster.getClusterNodes();
        clusterNodeIt = clusterNodes.keySet().iterator();
        this.prefix = prefix;
    }

    @Override
    public boolean hasNext() {
        if (null != keyIterator && keyIterator.hasNext()) {
            // Further keys on the current cluster node to process
            return true;
        } else {
            // Discover next cluster node
            Jedis client = null;

            while (clusterNodeIt.hasNext()) {
                try {
                    if (null != keyIterator) {
                        keyIterator.release();
                    }

                    ConnectionPool pool = clusterNodes.get(clusterNodeIt.next());
                    client = new Jedis(pool.getResource());
                    keyIterator = new RedisServerKeyIterator(client, prefix);

                    if (keyIterator.hasNext()) {
                        return true;
                    }
                } catch (Exception ex) {
                    if (null != client) {
                        client.close();
                    }

                    throw ex;
                }
            }

            if (null != keyIterator) {
                keyIterator.release();
            }

            return false;
        }
    }

    @Override
    public byte[] next() {
        return keyIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
