package org.infinispan.persistence.redis.client;

import org.infinispan.persistence.redis.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.redis.configuration.RedisServerConfiguration;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.List;

final public class RedisServerConnectionPool implements RedisConnectionPool {
    private final JedisPool connectionPool;
    private static final Log log = LogFactory.getLog(RedisServerConnectionPool.class, Log.class);

    public RedisServerConnectionPool(RedisStoreConfiguration configuration)
            throws RedisClientException {
        List<RedisServerConfiguration> servers = configuration.servers();
        RedisServerConfiguration server;

        if (servers.size() == 0) {
            RedisServerConnectionPool.log.error("No redis servers defined");
            throw new RedisClientException();
        }

        server = servers.get(0);

        if (servers.size() > 1) {
            RedisServerConnectionPool.log.warn(String.format("Multiple redis servers defined. Using the first only (%s:%d)",
                                                             server.host(), server.port()));
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

        connectionPool = new JedisPool(
                poolConfig,
                server.host(),
                server.port(),
                configuration.connectionTimeout(),
                configuration.socketTimeout(),
                configuration.password(),
                configuration.database(),
                null,
                configuration.ssl()
        );
    }

    @Override
    public RedisConnection getConnection() {
        return new RedisServerConnection(connectionPool.getResource());
    }

    @Override
    public void shutdown() {
        connectionPool.destroy();
    }
}
