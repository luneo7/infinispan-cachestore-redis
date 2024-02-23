package org.infinispan.persistence.redis.configuration;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Compressor;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Topology;

import java.util.ArrayList;
import java.util.List;

final public class RedisStoreConfigurationBuilder
        extends AbstractStoreConfigurationBuilder<RedisStoreConfiguration, RedisStoreConfigurationBuilder>
        implements RedisStoreConfigurationChildBuilder<RedisStoreConfigurationBuilder> {
    private final List<RedisServerConfigurationBuilder> servers = new ArrayList<>();
    private final List<RedisSentinelConfigurationBuilder> sentinels = new ArrayList<>();
    private final ConnectionPoolConfigurationBuilder connectionPool;

    public RedisStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
        super(builder, RedisStoreConfiguration.attributeDefinitionSet());
        segmented(false);
        connectionPool = new ConnectionPoolConfigurationBuilder(this);
    }

    @Override
    public RedisStoreConfigurationBuilder self() {
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder database(int database) {
        this.attributes.attribute(RedisStoreConfiguration.DATABASE).set(database);
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder password(String password) {
        this.attributes.attribute(RedisStoreConfiguration.PASSWORD).set(password);
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder topology(Topology topology) {
        this.attributes.attribute(RedisStoreConfiguration.TOPOLOGY).set(topology);
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder connectionTimeout(int connectionTimeout) {
        attributes.attribute(RedisStoreConfiguration.CONNECTION_TIMEOUT).set(connectionTimeout);
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder socketTimeout(int socketTimeout) {
        attributes.attribute(RedisStoreConfiguration.SOCKET_TIMEOUT).set(socketTimeout);
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder masterName(String masterName) {
        attributes.attribute(RedisStoreConfiguration.MASTER_NAME).set(masterName);
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder maxRedirections(int maxRedirections) {
        attributes.attribute(RedisStoreConfiguration.MAX_REDIRECTIONS).set(maxRedirections);
        return this;
    }

    @Override
    public RedisServerConfigurationBuilder addServer() {
        RedisServerConfigurationBuilder builder = new RedisServerConfigurationBuilder(this);
        this.servers.add(builder);
        return builder;
    }

    @Override
    public RedisSentinelConfigurationBuilder addSentinel() {
        RedisSentinelConfigurationBuilder builder = new RedisSentinelConfigurationBuilder(this);
        this.sentinels.add(builder);
        return builder;
    }

    @Override
    public ConnectionPoolConfigurationBuilder connectionPool() {
        return this.connectionPool;
    }

    @Override
    public void validate() {
        super.validate();

        Topology topology = this.attributes.attribute(RedisStoreConfiguration.TOPOLOGY).get();
        String masterName = this.attributes.attribute(RedisStoreConfiguration.MASTER_NAME).get();

        if (topology.equals(Topology.SENTINEL) && (masterName == null || masterName.equals(""))) {
            // Master name is required
            throw new CacheConfigurationException("master-name must be defined when using a sentinel topology.");
        }

        if (topology.equals(Topology.SENTINEL) && this.sentinels.size() == 0) {
            // One or more Sentinel servers are required
            throw new CacheConfigurationException("At least one sentinel-server must be defined " +
                                                          "when using a sentinel topology.");
        }

        if (topology.equals(Topology.CLUSTER) && this.servers.size() == 0) {
            // One or more Redis servers are required
            throw new CacheConfigurationException("One or more redis-server must be defined " +
                                                          "when using a cluster topology.");
        }

        if (topology.equals(Topology.SERVER) && this.servers.size() == 0) {
            // A single Redis servers are required
            throw new CacheConfigurationException("A redis-server must be defined " +
                                                          "when using a server topology.");
        }
    }

    @Override
    public RedisStoreConfigurationBuilder compressor(Compressor compressor) {
        attributes.attribute(RedisStoreConfiguration.COMPRESSOR).set(compressor);
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder compressionBlockSize(int compressionBlockSize) {
        attributes.attribute(RedisStoreConfiguration.COMPRESSION_BLOCK_SIZE).set(compressionBlockSize);
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder compressionLevel(int compressionLevel) {
        attributes.attribute(RedisStoreConfiguration.COMPRESSION_LEVEL).set(compressionLevel);
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder key2StringMapper(String key2StringMapper) {
        attributes.attribute(RedisStoreConfiguration.KEY2STRING_MAPPER).set(key2StringMapper);
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder ssl(boolean ssl) {
        attributes.attribute(RedisStoreConfiguration.SSL).set(ssl);
        return this;
    }

    @Override
    public RedisStoreConfigurationBuilder read(RedisStoreConfiguration template, Combine combine) {
        super.read(template, combine);
        for (RedisServerConfiguration server : template.servers()) {
            this.addServer().host(server.host()).port(server.port());
        }

        for (RedisServerConfiguration server : template.sentinels()) {
            this.addSentinel().host(server.host()).port(server.port());
        }

        return this;
    }

    @Override
    public RedisStoreConfiguration create() {
        List<RedisServerConfiguration> redisServers = new ArrayList<RedisServerConfiguration>();
        for (RedisServerConfigurationBuilder server : servers) {
            redisServers.add(server.create());
        }

        List<RedisServerConfiguration> redisSentinels = new ArrayList<RedisServerConfiguration>();
        for (RedisSentinelConfigurationBuilder server : sentinels) {
            redisSentinels.add(server.create());
        }

        attributes.attribute(RedisStoreConfiguration.SERVERS).set(redisServers);
        attributes.attribute(RedisStoreConfiguration.SENTINELS).set(redisSentinels);

        return new RedisStoreConfiguration(this.attributes.protect(), this.async.create(), this.connectionPool.create());
    }
}
