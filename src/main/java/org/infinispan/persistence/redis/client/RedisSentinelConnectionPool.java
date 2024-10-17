package org.infinispan.persistence.redis.client;

import org.infinispan.persistence.redis.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.redis.configuration.RedisServerConfiguration;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration;
import org.infinispan.persistence.redis.util.Maps;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

final public class RedisSentinelConnectionPool implements RedisConnectionPool {
    private final JedisSentinelPool sentinelPool;

    public RedisSentinelConnectionPool(RedisStoreConfiguration configuration) {
        boolean ssl = configuration.ssl();
        Set<HostAndPort> sentinels = new HashSet<>(Maps.capacity(configuration.sentinels().size()));

        for (RedisServerConfiguration server : configuration.sentinels()) {
            sentinels.add(new HostAndPort(server.host(), server.port()));
        }

        ConnectionPoolConfiguration connectionPoolConfiguration = configuration.connectionPool();

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(connectionPoolConfiguration.maxTotal());
        poolConfig.setMinIdle(connectionPoolConfiguration.minIdle());
        poolConfig.setMaxIdle(connectionPoolConfiguration.maxIdle());
        poolConfig.setMinEvictableIdleTime(Duration.ofMillis(connectionPoolConfiguration.minEvictableIdleTime()));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(connectionPoolConfiguration.timeBetweenEvictionRuns()));
        poolConfig.setTestOnCreate(connectionPoolConfiguration.testOnCreate());
        poolConfig.setTestOnBorrow(connectionPoolConfiguration.testOnBorrow());
        poolConfig.setTestOnReturn(connectionPoolConfiguration.testOnReturn());
        poolConfig.setTestWhileIdle(connectionPoolConfiguration.testOnIdle());

        DefaultJedisClientConfig masterConfig = DefaultJedisClientConfig.builder()
                                                                        .connectionTimeoutMillis(configuration.connectionTimeout())
                                                                        .socketTimeoutMillis(configuration.socketTimeout())
                                                                        .password(configuration.password())
                                                                        .database(configuration.database())
                                                                        .ssl(ssl)
                                                                        .build();

        DefaultJedisClientConfig sentinelConfig = DefaultJedisClientConfig.builder()
                                                                          .ssl(ssl)
                                                                          .build();

        sentinelPool = new JedisSentinelPool(
                configuration.masterName(),
                sentinels,
                poolConfig,
                masterConfig,
                sentinelConfig
        );
    }

    @Override
    public RedisConnection getConnection() {
        return new RedisServerConnection(sentinelPool.getResource());
    }

    @Override
    public void shutdown() {
        sentinelPool.destroy();
    }
}
