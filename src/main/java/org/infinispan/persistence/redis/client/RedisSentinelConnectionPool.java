package org.infinispan.persistence.redis.client;

import org.infinispan.persistence.redis.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.redis.configuration.RedisServerConfiguration;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

final public class RedisSentinelConnectionPool implements RedisConnectionPool {
    private final JedisSentinelPool sentinelPool;

    public RedisSentinelConnectionPool(RedisStoreConfiguration configuration) {
        Set<String> sentinels = new HashSet<String>();
        for (RedisServerConfiguration server : configuration.sentinels()) {
            sentinels.add(String.format("%s:%s", server.host(), server.port()));
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

        sentinelPool = new JedisSentinelPool(
                configuration.masterName(),
                sentinels,
                poolConfig,
                configuration.connectionTimeout(),
                configuration.socketTimeout(),
                configuration.password(),
                configuration.database(),
                null
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
