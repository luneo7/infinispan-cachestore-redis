package org.infinispan.persistence.redis.client;

import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration;

import java.security.InvalidParameterException;

final public class RedisConnectionPoolFactory {
    public static RedisConnectionPool factory(RedisStoreConfiguration configuration)
            throws RedisClientException {
        return switch (configuration.topology()) {
            case CLUSTER -> new RedisClusterConnectionPool(configuration);
            case SENTINEL -> new RedisSentinelConnectionPool(configuration);
            case SERVER -> new RedisServerConnectionPool(configuration);
        };

    }
}
