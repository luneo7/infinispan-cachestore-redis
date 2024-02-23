package org.infinispan.persistence.redis.configuration;

import org.infinispan.configuration.cache.StoreConfigurationChildBuilder;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.Key2StringMapper;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Compressor;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Topology;

public interface RedisStoreConfigurationChildBuilder<S> extends StoreConfigurationChildBuilder<S> {
    /**
     * Adds a new Redis server
     */
    RedisServerConfigurationBuilder addServer();

    /**
     * Adds a new sentinel server
     */
    RedisSentinelConfigurationBuilder addSentinel();

    /**
     * Configures the connection pool
     */
    ConnectionPoolConfigurationBuilder connectionPool();

    /**
     * The property defines the database ID number
     */
    RedisStoreConfigurationBuilder database(int database);

    /**
     * The property defines the password for accessing Redis
     */
    RedisStoreConfigurationBuilder password(String password);

    /**
     * The property defines the master name for when using Redis Sentinel
     */
    RedisStoreConfigurationBuilder masterName(String masterName);

    /**
     * The property defines the max number of redirections for when using Redis Cluster
     */
    RedisStoreConfigurationBuilder maxRedirections(int maxRedirections);

    /**
     * The property defines the topology of the Redis store
     */
    RedisStoreConfigurationBuilder topology(Topology topology);

    /**
     * This property defines the maximum socket connect timeout before giving up connecting to the
     * server.
     */
    RedisStoreConfigurationBuilder connectionTimeout(int connectionTimeout);

    /**
     * This property defines the maximum socket read timeout in milliseconds before giving up waiting
     * for bytes from the server. Defaults to 60000 (1 minute)
     */
    RedisStoreConfigurationBuilder socketTimeout(int socketTimeout);

    /**
     * Sets compressor
     */
    RedisStoreConfigurationBuilder compressor(Compressor compressor);

    /**
     * Compression block size
     */
    RedisStoreConfigurationBuilder compressionBlockSize(int compressionBlockSize);

    /**
     * Compression level
     */
    RedisStoreConfigurationBuilder compressionLevel(int compressionLevel);

    /**
     * The class name of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
     * storage in a database table. Defaults to {@link DefaultTwoWayKey2StringMapper}
     */
    RedisStoreConfigurationBuilder key2StringMapper(String key2StringMapper);

    /**
     * If Redis is using SSL
     */
    RedisStoreConfigurationBuilder ssl(boolean ssl);
}
