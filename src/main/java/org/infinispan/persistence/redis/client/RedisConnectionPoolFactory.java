package org.infinispan.persistence.redis.client;

import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration;

import java.security.InvalidParameterException;

final public class RedisConnectionPoolFactory {
    public static RedisConnectionPool factory(RedisStoreConfiguration configuration)
            throws RedisClientException {
        switch (configuration.topology()) {
            case CLUSTER: {
                return new RedisClusterConnectionPool(configuration);
            }

            case SENTINEL: {
                return new RedisSentinelConnectionPool(configuration);
            }

            case SERVER: {
                return new RedisServerConnectionPool(configuration);
            }
        }

        throw new InvalidParameterException();
    }
}
