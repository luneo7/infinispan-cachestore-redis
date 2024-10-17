package org.infinispan.persistence.redis.client;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.infinispan.persistence.redis.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.redis.configuration.RedisServerConfiguration;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration;
import org.infinispan.persistence.redis.util.Maps;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

final public class RedisClusterConnectionPool implements RedisConnectionPool {
    private final JedisCluster cluster;

    public RedisClusterConnectionPool(RedisStoreConfiguration configuration) {
        Set<HostAndPort> clusterNodes = new HashSet<>(Maps.capacity(configuration.servers().size()));
        for (RedisServerConfiguration server : configuration.servers()) {
            clusterNodes.add(new HostAndPort(server.host(), server.port()));
        }

        ConnectionPoolConfiguration connectionPoolConfiguration = configuration.connectionPool();

        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(connectionPoolConfiguration.maxTotal());
        poolConfig.setMinIdle(connectionPoolConfiguration.minIdle());
        poolConfig.setMaxIdle(connectionPoolConfiguration.maxIdle());
        poolConfig.setMinEvictableIdleTime(Duration.ofMillis(connectionPoolConfiguration.minEvictableIdleTime()));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(connectionPoolConfiguration.timeBetweenEvictionRuns()));
        poolConfig.setTestOnCreate(connectionPoolConfiguration.testOnCreate());
        poolConfig.setTestOnBorrow(connectionPoolConfiguration.testOnBorrow());
        poolConfig.setTestOnReturn(connectionPoolConfiguration.testOnReturn());
        poolConfig.setTestWhileIdle(connectionPoolConfiguration.testOnIdle());

        DefaultJedisClientConfig jedisClientConfig = DefaultJedisClientConfig.builder()
                                                                             .connectionTimeoutMillis(configuration.connectionTimeout())
                                                                             .socketTimeoutMillis(configuration.socketTimeout())
                                                                             .ssl(configuration.ssl())
                                                                             .build();

        cluster = new JedisCluster(
                clusterNodes,
                jedisClientConfig,
                configuration.maxRedirections(),
                poolConfig
        );
    }

    @Override
    public RedisConnection getConnection() {
        return new RedisClusterConnection(cluster);
    }

    @Override
    public void shutdown() {
        cluster.close();
    }
}
